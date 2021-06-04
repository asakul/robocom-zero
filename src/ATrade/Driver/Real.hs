{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}

module ATrade.Driver.Real (
  Strategy(..),
  StrategyInstanceParams(..),
  robotMain,
  BigConfig(..),
  mkBarStrategy,
  barStrategyDriver
) where

import Options.Applicative
import System.IO
import System.Signal
import System.Exit
import System.Random
import System.Log.Logger
import System.Log.Handler.Simple
import System.Log.Handler (setFormatter)
import System.Log.Formatter
import Control.Monad
import Control.Monad.Reader
import Control.Concurrent hiding (writeChan, readChan, writeList2Chan, yield)
import Control.Concurrent.BoundedChan as BC
import Control.Exception.Safe
import Control.Lens hiding (Context, (.=))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Aeson
import Data.IORef
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Maybe
import Database.Redis hiding (info, decode)
import ATrade.Types
import ATrade.Quotes
import ATrade.RoboCom.Monad (EventCallback, Event(..), StrategyEnvironment(..), seBars, seLastTimestamp, Event(..), MonadRobot(..))
import ATrade.BarAggregator
import ATrade.Driver.Real.BrokerClientThread
import ATrade.Driver.Real.QuoteSourceThread
import ATrade.Driver.Types (Strategy(..), StrategyInstanceParams(..), InitializationCallback)
import ATrade.RoboCom.Types (BarSeries(..), Ticker(..), Timeframe(..))
import ATrade.Exceptions
import ATrade.Quotes.QHP as QQ
import System.ZMQ4 hiding (Event(..))
import GHC.Generics

data Params = Params {
  instanceId :: String,
  strategyConfigFile :: FilePath,
  strategyStateFile :: FilePath,
  brokerEp :: String,
  quotesourceEp :: String,
  historyProviderType :: Maybe String,
  historyProvider :: Maybe String,
  redisSocket :: Maybe String,
  qtisSocket :: Maybe String,
  accountId :: String,
  volumeFactor :: Int,
  sourceBarTimeframe :: Maybe Int
} deriving (Show, Eq)

paramsParser :: Parser Params
paramsParser = Params
  <$> strOption
    (     long "instance-id"
      <>  metavar "ID" )
  <*> strOption
    (     long "config"
      <>  metavar "FILEPATH" )
  <*> strOption
    (     long "state"
      <>  metavar "FILEPATH" )
  <*> strOption
    (     long "broker"
      <>  metavar "BROKER_ENDPOINT" )
  <*> strOption
    (     long "quotesource"
      <>  metavar "QUOTESOURCE_ENDPOINT" )
  <*> optional ( strOption
    (     long "history-provider-type"
      <>  metavar "TYPE/ID" ))
  <*> optional ( strOption
    (     long "history-provider"
      <>  metavar "ENDPOINT/ID" ))
  <*> optional ( strOption
    (     long "redis-socket"
      <>  metavar "ADDRESS" ))
  <*> optional ( strOption
    (     long "qtis"
      <>  metavar "ENDPOINT/ID" ))
  <*> strOption
    (     long "account"
      <>  metavar "ACCOUNT" )
  <*> option auto
    (     long "volume"
      <>  metavar "VOLUME" )
  <*> optional ( option auto
    (     long "source-timeframe"
      <> metavar "SECONDS" ))

data Env historySource c s = Env {
  envHistorySource :: historySource,
  envStrategyInstanceParams :: StrategyInstanceParams,
  envStrategyEnvironment :: IORef StrategyEnvironment,
  envConfigRef :: IORef c,
  envStateRef :: IORef s,
  envBrokerChan :: BC.BoundedChan BrokerCommand,
  envTimers :: IORef [UTCTime],
  envEventChan :: BC.BoundedChan Event,
  envAggregator :: IORef BarAggregator,
  envLastTimestamp :: IORef UTCTime
} deriving (Generic)

type App historySource c s = ReaderT (Env historySource c s) IO

instance MonadRobot (App historySource c s) c s where
  submitOrder order = do
    bc <- asks envBrokerChan
    lift $ BC.writeChan bc $ BrokerSubmitOrder order

  cancelOrder oId = do
    bc <- asks envBrokerChan
    lift $ BC.writeChan bc $ BrokerCancelOrder oId
    
  appendToLog = lift . debugM "Strategy" . T.unpack
  setupTimer t = do
    timers <- asks envTimers
    lift $ atomicModifyIORef' timers (\s -> (t : s, ()))

  enqueueIOAction actionId action' = do
    eventChan <- asks envEventChan
    lift $ void $ forkIO $ do
      v <- action'
      BC.writeChan eventChan $ ActionCompleted actionId v
      
  getConfig = asks envConfigRef >>= lift . readIORef
  getState = asks envStateRef >>= lift . readIORef
  setState s = do
    ref <- asks envStateRef
    lift $ writeIORef ref s

  getEnvironment = do
    aggRef <- asks envAggregator
    envRef <- asks envStrategyEnvironment
    agg <- lift $ readIORef aggRef
    env <- lift $ readIORef envRef
    nowRef <- asks envLastTimestamp
    now <- lift $ readIORef nowRef
    return $ env & seBars .~ bars agg & seLastTimestamp .~ now

instance MonadHistory (App QQ.QHPHandle c s) where
  getHistory tickerId timeframe fromTime toTime = do
    qhp <- asks envHistorySource
    QQ.requestHistoryFromQHP qhp tickerId timeframe fromTime toTime

data BigConfig c = BigConfig {
  confTickers :: [Ticker],
  strategyConfig :: c
}

instance (FromJSON c) => FromJSON (BigConfig c) where
  parseJSON = withObject "object" (\obj -> BigConfig <$>
    obj .: "tickers" <*>
    obj .: "params")

instance (ToJSON c) => ToJSON (BigConfig c) where
  toJSON conf = object ["tickers" .= confTickers conf,
    "params" .= strategyConfig conf ]

storeState :: (ToJSON s) => Params -> IORef s -> IORef [UTCTime] -> IO ()
storeState params stateRef timersRef = do
  currentStrategyState <- readIORef stateRef
  currentTimersState <- readIORef timersRef
  case redisSocket params of
    Nothing -> withFile (strategyStateFile params) WriteMode (\f -> BS.hPut f $ BL.toStrict $ encode currentStrategyState)
        `catch` (\e -> warningM "main" ("Unable to save state: " ++ show (e :: IOException)))
    Just sock -> do
#ifdef linux_HOST_OS
      conn <- checkedConnect $ defaultConnectInfo { connectPort = UnixSocket sock }
      now <- getPOSIXTime
      res <- runRedis conn $ mset [(encodeUtf8 $ T.pack $ instanceId params, BL.toStrict $ encode currentStrategyState),
          (encodeUtf8 $ T.pack $ instanceId params ++ ":last_store", encodeUtf8 $ T.pack $ show now),
          (encodeUtf8 $ T.pack $ instanceId params ++ ":timers", BL.toStrict $ encode currentTimersState) ]

      case res of
        Left _ -> warningM "main" "Unable to save state"
        Right _ -> return ()
#else
      return ()
#endif

gracefulShutdown :: (ToJSON s) => Params -> IORef s -> IORef [UTCTime] -> MVar () -> Signal -> IO ()
gracefulShutdown params stateRef timersRef shutdownMv _ = do
  infoM "main" "Shutdown, saving state"
  storeState params stateRef timersRef
  putMVar shutdownMv ()
  exitSuccess

robotMain :: (ToJSON s, FromJSON s, FromJSON c) => DiffTime -> s -> Maybe (InitializationCallback c) -> EventCallback c s -> IO ()
robotMain dataDownloadDelta defaultState initCallback callback = do
  params <- execParser opts
  initLogging params
  infoM "main" "Starting"

  (tickerList, config) <- loadStrategyConfig params
  stratState <- loadStrategyState params
  timersState <- loadStrategyTimers params

  let instanceParams = StrategyInstanceParams {
    strategyInstanceId = T.pack . instanceId $ params,
    strategyAccount = T.pack . accountId $ params,
    strategyVolume = volumeFactor params,
    tickers = tickerList,
    strategyQTISEp = T.pack <$> qtisSocket params}

  updatedConfig <- case initCallback of
    Just cb -> cb config instanceParams
    Nothing -> return config

  let strategy = mkBarStrategy instanceParams dataDownloadDelta updatedConfig stratState callback
  stateRef <- newIORef stratState
  configRef <- newIORef updatedConfig
  timersRef <- newIORef timersState
  shutdownMv <- newEmptyMVar
  installHandler sigINT (gracefulShutdown params stateRef timersRef shutdownMv)
  installHandler sigTERM (gracefulShutdown params stateRef timersRef shutdownMv)
  randsec <- getStdRandom(randomR(1, 10))
  threadDelay $ randsec * 1000000
  debugM "main" "Forking state saving thread"
  stateSavingThread <- forkIO $ forever $ do
    threadDelay 1000000
    storeState params stateRef timersRef

  straEnv <- newIORef StrategyEnvironment {
        _seInstanceId = strategyInstanceId . strategyInstanceParams $ strategy,
        _seAccount = strategyAccount . strategyInstanceParams $ strategy,
        _seVolume = strategyVolume . strategyInstanceParams $ strategy,
        _seBars = M.empty,
        _seLastTimestamp = UTCTime (fromGregorian 1970 1 1) 0
  }
  -- Event channel is for strategy events, like new tick arrival, or order execution notification
  eventChan <- BC.newBoundedChan 1000
  -- Orders channel passes strategy orders to broker thread
  brokerChan <- BC.newBoundedChan 1000

  debugM "main" "Starting strategy driver"
  withContext (\ctx -> do

    let qsEp = T.pack $ quotesourceEp params
    let brEp =  T.pack $ brokerEp params
    agg <- newIORef $ mkAggregatorFromBars M.empty [(hmsToDiffTime 3 50 0, hmsToDiffTime 21 10 0)]
    bracket (startQuoteSourceThread ctx qsEp strategy eventChan agg tickFilter (sourceBarTimeframe params)) killThread $ \_ -> do
      debugM "Strategy" "QuoteSource thread forked"
      bracket (startBrokerClientThread (strategyInstanceId . strategyInstanceParams $ strategy) ctx brEp brokerChan eventChan shutdownMv) killThread $ \_ -> do
        debugM "Strategy" "Broker thread forked"

        now <- getCurrentTime >>= newIORef

        let env = Env {
            envHistorySource = mkQHPHandle ctx (T.pack . fromMaybe "" . historyProvider $ params),
            envStrategyInstanceParams = instanceParams,
            envStrategyEnvironment = straEnv,
            envConfigRef = configRef,
            envStateRef = stateRef,
            envBrokerChan = brokerChan,
            envTimers = timersRef,
            envEventChan = eventChan,
            envAggregator = agg,
            envLastTimestamp = now
            }
        runReaderT (barStrategyDriver strategy shutdownMv) env `finally` killThread stateSavingThread)
  where
    tickFilter :: Tick -> Bool
    tickFilter tick =
      let classCode = T.takeWhile (/= '#') (security tick) in
          if classCode == "SPBFUT" || classCode == "SPBOPT"
            then any (inInterval . utctDayTime . timestamp $ tick) fortsIntervals
            else any (inInterval . utctDayTime . timestamp $ tick) secIntervals

    fortsIntervals = [(fromHMS 4 0 0, fromHMS 11 0 0), (fromHMS 11 5 0, fromHMS 15 45 0), (fromHMS 16 0 0, fromHMS 20 50 0)]
    secIntervals = [(fromHMS 6 50 0, fromHMS 15 51 0)]

    fromHMS h m s = h * 3600 + m * 60 + s
    inInterval ts (start, end) = ts >= start && ts <= end

    opts = info (helper <*> paramsParser)
      ( fullDesc <> header "ATrade strategy execution framework" )

    initLogging params = do
      handler <- streamHandler stderr DEBUG >>=
        (\x -> return $
          setFormatter x (simpleLogFormatter $
            "$utcTime\t[" ++ instanceId params ++ "]\t\t{$loggername}\t\t<$prio> -> $msg"))

      hSetBuffering stderr LineBuffering
      updateGlobalLogger rootLoggerName (setLevel DEBUG)
      updateGlobalLogger rootLoggerName (setHandlers [handler])

    loadStrategyConfig params = withFile (strategyConfigFile params) ReadMode (\f -> do
      bigconfig <- eitherDecode . BL.fromStrict <$> BS.hGetContents f
      case bigconfig of
        Right conf -> return (confTickers conf, strategyConfig conf)
        Left errmsg -> throw $ UnableToLoadConfig $ (T.pack . show) errmsg)

    loadStrategyTimers :: Params -> IO [UTCTime]
    loadStrategyTimers params = case redisSocket params of
      Nothing -> return []
      Just sock -> do
#ifdef linux_HOST_OS
        conn <- checkedConnect $ defaultConnectInfo { connectPort = UnixSocket sock }
        res <- runRedis conn $ get (encodeUtf8 $ T.pack $ instanceId params ++ ":timers")
        case res of
          Left _ -> do
            warningM "main" "Unable to load state"
            return []
          Right mv -> case mv of
            Just v -> case eitherDecode $ BL.fromStrict v of
              Left _ -> do
                warningM "main" "Unable to load state"
                return []
              Right s -> return s
            Nothing -> do
              warningM "main" "Unable to load state"
              return []
#else
          error "Not implemented"
#endif
        

    loadStrategyState params = case redisSocket params of
      Nothing -> loadStateFromFile (strategyStateFile params)
      Just sock -> do
#ifdef linux_HOST_OS
        conn <- checkedConnect $ defaultConnectInfo { connectPort = UnixSocket sock }
        res <- runRedis conn $ get (encodeUtf8 $ T.pack $ instanceId params)
        case res of
          Left _ -> do
            warningM "main" "Unable to load state"
            return defaultState
          Right mv -> case mv of
            Just v -> case eitherDecode $ BL.fromStrict v of
              Left _ -> do
                warningM "main" "Unable to load state"
                return defaultState
              Right s -> return s
            Nothing -> do
              warningM "main" "Unable to load state"
              return defaultState
#else
          error "Not implemented"
#endif
      
    loadStateFromFile filepath = withFile filepath ReadMode (\f -> do
        maybeState <- decode . BL.fromStrict <$> BS.hGetContents f
        case maybeState of
          Just st -> return st
          Nothing -> return defaultState ) `catch`
            (\e -> warningM "main" ("Unable to load state: " ++ show (e :: IOException)) >> return defaultState)
  
-- | Helper function to make 'Strategy' instances
mkBarStrategy :: StrategyInstanceParams -> DiffTime -> c -> s -> EventCallback c s -> Strategy c s
mkBarStrategy instanceParams dd params initialState cb = BarStrategy {
  downloadDelta = dd,
  eventCallback = cb,
  currentState = initialState,
  strategyParams = params,
  strategyTimers = [],

  strategyInstanceParams = instanceParams }

-- | Main function which handles incoming events (ticks/orders), passes them to strategy callback
-- and executes returned strategy actions
barStrategyDriver :: (MonadHistory (App hs c s)) => Strategy c s -> MVar () -> App hs c s ()
barStrategyDriver strategy shutdownVar = do
  now <- liftIO getCurrentTime
  history <- M.fromList <$> mapM (loadTickerHistory now) (tickers . strategyInstanceParams $ strategy)
  eventChan <- asks envEventChan
  brokerChan <- asks envBrokerChan
  agg <- asks envAggregator
  liftIO $ atomicModifyIORef' agg (\s -> (replaceHistory s history, ()))

  wakeupTid <- lift . forkIO $ forever $ do
    maybeShutdown <- tryTakeMVar shutdownVar
    if isJust maybeShutdown
      then writeChan eventChan Shutdown
      else do
        threadDelay 1000000
        writeChan brokerChan BrokerRequestNotifications
  lift $ debugM "Strategy" "Wakeup thread forked"

  readAndHandleEvents agg strategy
  lift $ debugM "Strategy" "Stopping strategy driver"
  lift $ killThread wakeupTid

  where

    loadTickerHistory now t = do
      history <- getHistory (code t) (BarTimeframe (fromInteger . timeframeSeconds $ t))
                 ((fromRational . toRational . negate $ downloadDelta strategy) `addUTCTime` now) now
      return (code t, BarSeries (code t) (Timeframe (timeframeSeconds t)) history)

    readAndHandleEvents agg strategy' = do
        eventChan <- asks envEventChan
        event <- lift $ readChan eventChan
        if event /= Shutdown
          then do
            env <- getEnvironment
            let newTimestamp = case event of
                  NewTick tick -> timestamp tick
                  _ -> env ^. seLastTimestamp
            nowRef <- asks envLastTimestamp
            lift $ writeIORef nowRef newTimestamp

            newTimers <- catMaybes <$> (mapM (checkTimer eventChan newTimestamp) $ strategyTimers strategy')
            (eventCallback strategy) event
            timersRef <- asks envTimers
            lift $ writeIORef timersRef newTimers

            readAndHandleEvents agg strategy' 
          else
            lift $ debugM "Strategy" "Shutdown requested"
      where

        checkTimer eventChan' newTimestamp timerTime =
          if newTimestamp >= timerTime
            then do
              lift $ writeChan eventChan' $ TimerFired timerTime
              return Nothing
            else
              return $ Just timerTime

