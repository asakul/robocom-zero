{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module ATrade.Driver.Junction.QuoteThread
 (
   QuoteThreadHandle,
   startQuoteThread,
   stopQuoteThread,
   addSubscription,
   DownloaderM,
   DownloaderEnv(..),
   runDownloaderM,
   withQThread
 ) where

import           ATrade.Driver.Junction.ProgramConfiguration (ProgramConfiguration (..))
import           ATrade.Driver.Junction.QuoteStream          (QuoteSubscription (..))
import           ATrade.Quotes.HistoryProvider               (HistoryProvider (..))
import           ATrade.Quotes.QHP                           (QHPHandle, requestHistoryFromQHP)
import           ATrade.Quotes.QTIS                          (TickerInfo (tiLotSize, tiTickSize, tiTicker),
                                                              qtisGetTickersInfo)
import           ATrade.Quotes.TickerInfoProvider            (TickerInfoProvider (..))
import           ATrade.QuoteSource.Client                   (QuoteData (QDBar, QDTick),
                                                              QuoteSourceClientHandle,
                                                              quoteSourceClientSubscribe,
                                                              startQuoteSourceClient,
                                                              stopQuoteSourceClient)
import           ATrade.RoboCom.Types                        (Bar (barSecurity),
                                                              BarSeries (..),
                                                              BarSeriesId (BarSeriesId),
                                                              Bars,
                                                              InstrumentParameters (InstrumentParameters))
import           ATrade.Types                                (BarTimeframe (BarTimeframe),
                                                              ClientSecurityParams (ClientSecurityParams),
                                                              Tick (security),
                                                              TickerId)
import           Control.Concurrent                          (ThreadId, forkIO,
                                                              killThread)
import           Control.Concurrent.BoundedChan              (BoundedChan,
                                                              newBoundedChan,
                                                              readChan,
                                                              writeChan)
import           Control.Exception.Safe                      (MonadThrow,
                                                              bracket)
import           Control.Monad                               (forM, forever)
import           Control.Monad.Reader                        (MonadIO (liftIO), ReaderT (runReaderT),
                                                              lift)
import           Control.Monad.Reader.Class                  (MonadReader, asks)
import qualified Data.HashMap.Strict                         as HM
import           Data.IORef                                  (IORef,
                                                              atomicModifyIORef',
                                                              newIORef,
                                                              readIORef)
import qualified Data.Map.Strict                             as M
import qualified Data.Text                                   as T
import           Data.Time                                   (addUTCTime,
                                                              getCurrentTime)
import           System.ZMQ4                                 (Context)
import           System.ZMQ4.ZAP                             (loadCertificateFromFile)


data QuoteThreadHandle = QuoteThreadHandle ThreadId ThreadId QuoteThreadEnv

data QuoteThreadEnv =
  QuoteThreadEnv
  {
    bars        :: IORef Bars,
    endpoints   :: IORef (HM.HashMap QuoteSubscription [BoundedChan QuoteData]),
    qsclient    :: QuoteSourceClientHandle,
    paramsCache :: IORef (M.Map TickerId InstrumentParameters),
    downloaderChan :: BoundedChan QuoteSubscription
  }

startQuoteThread :: (MonadIO m,
                     MonadIO m1,
                     HistoryProvider m1,
                     TickerInfoProvider m1) =>
  IORef Bars ->
  Context ->
  T.Text ->
  ClientSecurityParams ->
  (m1 () -> IO ()) ->
  m QuoteThreadHandle
startQuoteThread barsRef ctx ep secparams downloadThreadRunner = do
  chan <- liftIO $ newBoundedChan 2000
  dChan <- liftIO $ newBoundedChan 2000
  qsc <- liftIO $ startQuoteSourceClient chan [] ctx ep secparams
  env <- liftIO $ QuoteThreadEnv barsRef <$> newIORef HM.empty <*> pure qsc <*> newIORef M.empty <*> pure dChan
  tid <- liftIO . forkIO $ quoteThread env chan
  downloaderTid <- liftIO . forkIO $ downloadThreadRunner (downloaderThread env dChan)
  return $ QuoteThreadHandle tid downloaderTid env
  where
    downloaderThread env chan = forever $ do
      QuoteSubscription tickerid tf <- liftIO $ readChan chan
      paramsMap <- liftIO $ readIORef $ paramsCache env
      mbParams <- case M.lookup tickerid paramsMap of
        Nothing -> do
          paramsList <- getInstrumentParameters [tickerid]
          case paramsList of
            (params:_) -> liftIO $ atomicModifyIORef' (paramsCache env) (\m -> (M.insert tickerid params m, Just params))
            _ -> return Nothing
        Just params -> return $ Just params
      barsMap <- liftIO $ readIORef (bars env)
      case M.lookup (BarSeriesId tickerid tf) barsMap of
        Just _ -> return () -- already downloaded
        Nothing -> case mbParams of
          Just params -> do
            now <- liftIO getCurrentTime
            barsData <- getHistory tickerid tf ((-86400 * 60) `addUTCTime` now) now
            let barSeries = BarSeries tickerid tf barsData params
            liftIO $ atomicModifyIORef' (bars env) (\m -> (M.insert (BarSeriesId tickerid tf) barSeries m, ()))
          _ -> return () -- TODO log


    quoteThread env chan = flip runReaderT env $ forever $ do
      qssData <- lift $ readChan chan
      case qssData of
        QDBar (tf, bar) -> do
          barsRef' <- asks bars
          lift $ atomicModifyIORef' barsRef' (\x -> (updateBarsMap x bar tf, ()))
        _ -> return () -- TODO pass to bar aggregator
      let key = case qssData of
                  QDTick tick -> QuoteSubscription (security tick) (BarTimeframe 0)
                  QDBar (tf, bar) -> QuoteSubscription (barSecurity bar) tf
      subs <- asks endpoints >>= (lift . readIORef)
      case HM.lookup key subs of
        Just clientChannels -> do
          lift $ mapM_ (`writeChan` qssData) clientChannels
        Nothing            -> return ()

stopQuoteThread :: (MonadIO m) => QuoteThreadHandle -> m ()
stopQuoteThread (QuoteThreadHandle tid dtid env) = liftIO $ do
  killThread tid
  killThread dtid
  stopQuoteSourceClient (qsclient env)

addSubscription :: (MonadIO m) => QuoteThreadHandle -> TickerId -> BarTimeframe -> BoundedChan QuoteData -> m ()
addSubscription (QuoteThreadHandle _ _ env) tid tf chan = liftIO $ do
  writeChan (downloaderChan env) (QuoteSubscription tid tf)
  atomicModifyIORef' (endpoints env) (\m -> (doAddSubscription m tid, ()))
  quoteSourceClientSubscribe (qsclient env) [(tid, BarTimeframe 0)]
  where
    doAddSubscription m tickerid =
      let m1 = HM.alter (\case
                   Just chans -> Just (chan : chans)
                   _ -> Just [chan]) (QuoteSubscription tickerid tf) m in
      HM.alter (\case
                   Just chans -> Just (chan : chans)
                   _ -> Just [chan]) (QuoteSubscription tickerid (BarTimeframe 0)) m1

updateBarsMap :: Bars -> Bar -> BarTimeframe -> Bars
updateBarsMap barsMap bar tf = M.adjust (addToSeries bar) (BarSeriesId (barSecurity bar) tf) barsMap

addToSeries :: Bar -> BarSeries -> BarSeries
addToSeries bar series = series { bsBars = bar : bsBars series }

data DownloaderEnv =
  DownloaderEnv
  {
    qhp                    :: QHPHandle,
    downloaderContext      :: Context,
    downloaderQtisEndpoint :: T.Text
  }

newtype DownloaderM a = DownloaderM { unDownloaderM :: ReaderT DownloaderEnv IO a }
  deriving (Functor, Applicative, Monad, MonadReader DownloaderEnv, MonadIO, MonadThrow)

instance HistoryProvider DownloaderM where
  getHistory tid tf from to = do
    q <- asks qhp
    requestHistoryFromQHP q tid tf from to

instance TickerInfoProvider DownloaderM where
  getInstrumentParameters tickers = do
    ctx <- asks downloaderContext
    ep <- asks downloaderQtisEndpoint
    tis <- liftIO $ forM tickers (qtisGetTickersInfo ctx ep)
    pure $ convert `fmap` tis
    where
      convert ti = InstrumentParameters
                     (tiTicker ti)
                     (fromInteger $ tiLotSize ti)
                     (tiTickSize ti)

withQThread :: DownloaderEnv -> IORef Bars -> ProgramConfiguration -> Context -> (QuoteThreadHandle -> IO ()) -> IO ()
withQThread env barsMap cfg ctx f = do
    securityParameters <- loadSecurityParameters
    bracket
      (startQuoteThread
          barsMap
          ctx
          (quotesourceEndpoint cfg)
          securityParameters
          (runDownloaderM env))
      stopQuoteThread f
  where
    loadSecurityParameters :: IO ClientSecurityParams
    loadSecurityParameters =
      case (quotesourceClientCert cfg, quotesourceServerCert cfg) of
        (Just clientCertPath, Just serverCertPath) -> do
          eClientCert <- loadCertificateFromFile clientCertPath
          eServerCert <- loadCertificateFromFile serverCertPath
          case (eClientCert, eServerCert) of
            (Right clientCert, Right serverCert) -> return $ ClientSecurityParams (Just clientCert) (Just serverCert)
            (_, _) -> return $ ClientSecurityParams Nothing Nothing

        _ -> return $ ClientSecurityParams Nothing Nothing

runDownloaderM :: DownloaderEnv -> DownloaderM () -> IO ()
runDownloaderM env = (`runReaderT` env) . unDownloaderM
