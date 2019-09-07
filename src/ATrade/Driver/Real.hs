{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE RankNTypes #-}

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
import Control.Concurrent hiding (writeChan, readChan, writeList2Chan, yield)
import Control.Concurrent.BoundedChan as BC
import Control.Exception
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Aeson
import Data.IORef
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Maybe
import Data.Monoid
import Database.Redis hiding (info, decode)
import ATrade.Types
import ATrade.RoboCom.Monad (StrategyMonad, StrategyAction(..), EventCallback, Event(..), runStrategyElement, StrategyEnvironment(..), Event(..))
import ATrade.BarAggregator
import ATrade.Driver.Real.BrokerClientThread
import ATrade.Driver.Real.QuoteSourceThread
import ATrade.Driver.Real.Types (Strategy(..), StrategyInstanceParams(..), InitializationCallback)
import ATrade.RoboCom.Types (BarSeries(..), Ticker(..), Timeframe(..))
import ATrade.Exceptions
import ATrade.Quotes.Finam as QF
import ATrade.Quotes.QHP as QQ
import ATrade.Quotes.HAP as QH
import System.ZMQ4 hiding (Event(..))

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
    strategyQuotesourceEp = T.pack . quotesourceEp $ params,
    strategyBrokerEp = T.pack . brokerEp $ params,
    strategyHistoryProviderType = T.pack $ fromMaybe "finam" $ historyProviderType params,
    strategyHistoryProvider = T.pack $ fromMaybe "" $ historyProvider params,
    strategyQTISEp = T.pack <$> qtisSocket params}

  updatedConfig <- case initCallback of
    Just cb -> cb config instanceParams
    Nothing -> return config

  let strategy = mkBarStrategy instanceParams dataDownloadDelta updatedConfig stratState callback
  stateRef <- newIORef stratState
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

  debugM "main" "Starting strategy driver"
  barStrategyDriver (sourceBarTimeframe params) tickFilter strategy stateRef timersRef shutdownMv `finally` killThread stateSavingThread
  where
    tickFilter :: Tick -> Bool
    tickFilter tick =
      let classCode = T.takeWhile (/= '#') (security tick) in
          if
             | classCode == "SPBFUT" || classCode == "SPBOPT" -> any (inInterval . utctDayTime . timestamp $ tick) fortsIntervals
             | otherwise -> any (inInterval . utctDayTime . timestamp $ tick) secIntervals

    fortsIntervals = [(fromHMS 7 0 0, fromHMS 11 0 0), (fromHMS 11 5 0, fromHMS 15 45 0), (fromHMS 16 0 0, fromHMS 20 50 0)]
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
barStrategyDriver :: Maybe Int -> (Tick -> Bool) -> Strategy c s -> IORef s -> IORef [UTCTime] -> MVar () -> IO ()
barStrategyDriver mbSourceTimeframe tickFilter strategy stateRef timersRef shutdownVar = do
  -- Make channels
  -- Event channel is for strategy events, like new tick arrival, or order execution notification
  eventChan <- BC.newBoundedChan 1000
  -- Orders channel passes strategy orders to broker thread
  ordersChan <- BC.newBoundedChan 1000

  withContext (\ctx -> do
    -- Load tickers data and create BarAggregator from them
    historyBars <-
      if 
        | (strategyHistoryProviderType . strategyInstanceParams) strategy == "finam" -> 
            M.fromList <$> mapM loadTickerFromFinam (tickers . strategyInstanceParams $ strategy)
        | (strategyHistoryProviderType . strategyInstanceParams) strategy == "hap" ->
            M.fromList <$> mapM (loadTickerFromHAP ctx ((strategyHistoryProvider . strategyInstanceParams) strategy)) (tickers . strategyInstanceParams $ strategy)
        | otherwise ->
            M.fromList <$> mapM (loadTickerFromQHP ctx ((strategyHistoryProvider . strategyInstanceParams) strategy)) (tickers . strategyInstanceParams $ strategy)
    agg <- newIORef $ mkAggregatorFromBars historyBars [(hmsToDiffTime 6 50 0, hmsToDiffTime 21 10 0)]
    bracket (startQuoteSourceThread ctx qsEp strategy eventChan agg tickFilter mbSourceTimeframe) killThread (\_ -> do
      debugM "Strategy" "QuoteSource thread forked"
      bracket (startBrokerClientThread (strategyInstanceId . strategyInstanceParams $ strategy) ctx brEp ordersChan eventChan shutdownVar) killThread (\_ -> do
        debugM "Strategy" "Broker thread forked"

        wakeupTid <- forkIO $ forever $ do
          maybeShutdown <- tryTakeMVar shutdownVar
          if isJust maybeShutdown
            then writeChan eventChan Shutdown
            else do
              threadDelay 1000000
              writeChan ordersChan BrokerRequestNotifications
        debugM "Strategy" "Wakeup thread forked"

        let env = StrategyEnvironment {
          seInstanceId = strategyInstanceId . strategyInstanceParams $ strategy,
          seAccount = strategyAccount . strategyInstanceParams $ strategy,
          seVolume = strategyVolume . strategyInstanceParams $ strategy,
          seBars = M.empty,
          seLastTimestamp = UTCTime (fromGregorian 1970 1 1) 0
        }
        readAndHandleEvents agg ordersChan eventChan strategy env
        debugM "Strategy" "Stopping strategy driver"
        killThread wakeupTid)))

  debugM "Strategy" "Strategy done"

  where
    qsEp = strategyQuotesourceEp . strategyInstanceParams $ strategy
    brEp = strategyBrokerEp . strategyInstanceParams $ strategy
    readAndHandleEvents agg ordersChan eventChan strategy' env = do
        event <- readChan eventChan
        if event /= Shutdown
          then do
            currentBars <- bars <$> readIORef agg
            let params = strategyParams strategy'
            let curState = currentState strategy'
            let instId = strategyInstanceId . strategyInstanceParams $ strategy'
            let acc = strategyAccount . strategyInstanceParams $ strategy'
            let vol = strategyVolume . strategyInstanceParams $ strategy'

            let oldTimestamp = seLastTimestamp env
            let newTimestamp = case event of
                  NewTick tick -> timestamp tick
                  _ -> seLastTimestamp env

            newTimers <- catMaybes <$> (mapM (checkTimer eventChan newTimestamp) $ strategyTimers strategy')

            let !newenv = env { seBars = currentBars, seLastTimestamp = newTimestamp }
            let (!newState, !actions, _) = runStrategyElement params curState newenv $ (eventCallback strategy) event
            writeIORef stateRef newState
            writeIORef timersRef newTimers

            newTimers' <- catMaybes <$> mapM handleTimerActions actions
            mapM_ (handleActions ordersChan) actions
            readAndHandleEvents agg ordersChan eventChan (strategy' { currentState = newState, strategyTimers = newTimers ++ newTimers' }) newenv
          else debugM "Strategy" "Shutdown requested"
      where
        handleTimerActions action =
          case action of
            ActionSetupTimer timerTime -> return $ Just timerTime
            _ -> return Nothing

        handleActions ordersChan' action =
          case action of
            (ActionLog logText) -> debugM "Strategy" $ T.unpack logText
            (ActionOrder order) -> writeChan ordersChan' $ BrokerSubmitOrder order
            (ActionCancelOrder oid) -> writeChan ordersChan' $ BrokerCancelOrder oid
            (ActionSetupTimer _) -> return ()
            (ActionIO tag io) -> void $ forkIO $ do
              v <- io
              writeChan eventChan (ActionCompleted tag v)

        checkTimer eventChan' newTimestamp timerTime =
          if newTimestamp >= timerTime
            then do
              writeChan eventChan' $ TimerFired timerTime
              return Nothing
            else
              return $ Just timerTime

    loadTickerFromHAP :: Context -> T.Text -> Ticker -> IO (TickerId, BarSeries)
    loadTickerFromHAP ctx ep t = do
      debugM "Strategy" $ "Loading ticker from HAP: " ++ show (code t)
      case parseHAPPeriod $ timeframeSeconds t of 
        Just tf -> do
          now <- getCurrentTime
          historyBars <- QH.getQuotes ctx QH.RequestParams {
            QH.endpoint = ep,
            QH.ticker = code t,
            QH.startDate = addUTCTime (negate . (1 +) . fromRational . toRational $ downloadDelta strategy) now,
            QH.endDate = now,
            QH.period = tf }
          debugM "Strategy" $ "Obtained " ++ show (length historyBars) ++ " bars"
          return (code t, BarSeries { bsTickerId = code t, bsTimeframe = Timeframe (timeframeSeconds t), bsBars = historyBars })
        _ -> return (code t, BarSeries { bsTickerId = code t, bsTimeframe = Timeframe (timeframeSeconds t), bsBars = [] })


    loadTickerFromQHP :: Context -> T.Text -> Ticker -> IO (TickerId, BarSeries)
    loadTickerFromQHP ctx ep t = do
      debugM "Strategy" $ "Loading ticker from QHP: " ++ show (code t)
      case parseQHPPeriod $ timeframeSeconds t of 
        Just tf -> do
          now <- getCurrentTime
          historyBars <- QQ.getQuotes ctx QQ.RequestParams {
            QQ.endpoint = ep,
            QQ.ticker = code t,
            QQ.startDate = addDays (negate . (1 +) . ceiling $ downloadDelta strategy / 86400) (utctDay now),
            QQ.endDate = utctDay now,
            QQ.period = tf }
          debugM "Strategy" $ "Obtained " ++ show (length historyBars) ++ " bars"
          debugM "Strategy" $ show (take 20 historyBars)
          return (code t, BarSeries { bsTickerId = code t, bsTimeframe = Timeframe (timeframeSeconds t), bsBars = historyBars })
        _ -> return (code t, BarSeries { bsTickerId = code t, bsTimeframe = Timeframe (timeframeSeconds t), bsBars = [] })


    loadTickerFromFinam :: Ticker -> IO (TickerId, BarSeries)
    loadTickerFromFinam t = do
      randDelay <- getStdRandom (randomR (1, 5))
      threadDelay $ randDelay * 1000000
      now <- getCurrentTime
      debugM "Strategy" $ show (L.lookup "finam" (aliases t), parseFinamPeriod $ timeframeSeconds t)
      case (L.lookup "finam" (aliases t), parseFinamPeriod $ timeframeSeconds t) of
        (Just finamCode, Just per) -> do
          debugM "Strategy" $ "Downloading ticker: " ++ finamCode
          history <- downloadAndParseQuotes $ defaultParams { QF.ticker = T.pack finamCode,
            QF.startDate = addDays (negate . (1 +) . ceiling $ downloadDelta strategy / 86400) (utctDay now),
            QF.endDate = utctDay now,
            QF.period = per }
          case history of
            Just h -> return (code t, BarSeries { bsTickerId = code t, bsTimeframe = Timeframe (timeframeSeconds t), bsBars = convertFromFinamHistory (code t) h })
            Nothing -> return (code t, BarSeries { bsTickerId = code t, bsTimeframe = Timeframe (timeframeSeconds t), bsBars = [] })
        _ -> return (code t, BarSeries { bsTickerId = code t, bsTimeframe = Timeframe (timeframeSeconds t), bsBars = [] })

    convertFromFinamHistory :: TickerId -> [Row] -> [Bar]
    convertFromFinamHistory tid = L.reverse . fmap (\row -> Bar { barSecurity = tid,
          barTimestamp = rowTime row,
          barOpen = rowOpen row,
          barHigh = rowHigh row,
          barLow = rowLow row,
          barClose = rowClose row,
          barVolume = rowVolume row })

    parseFinamPeriod x
      | x == 0 = Just QF.PeriodTick
      | x == 60 = Just QF.Period1Min
      | x == 5 * 60 = Just QF.Period5Min
      | x == 10 * 60 = Just QF.Period10Min
      | x == 15 * 60 = Just QF.Period15Min
      | x == 30 * 60 = Just QF.Period30Min
      | x == 60 * 60 = Just QF.PeriodHour
      | x == 24 * 60 * 60 = Just QF.PeriodDay
      | otherwise = Nothing

    parseQHPPeriod x
      | x == 60 = Just QQ.Period1Min
      | x == 5 * 60 = Just QQ.Period5Min
      | x == 15 * 60 = Just QQ.Period15Min
      | x == 30 * 60 = Just QQ.Period30Min
      | x == 60 * 60 = Just QQ.PeriodHour
      | x == 24 * 60 * 60 = Just QQ.PeriodDay
      | otherwise = Nothing

    parseHAPPeriod x
      | x == 60 = Just QH.Period1Min
      | x == 5 * 60 = Just QH.Period5Min
      | x == 15 * 60 = Just QH.Period15Min
      | x == 30 * 60 = Just QH.Period30Min
      | x == 60 * 60 = Just QH.PeriodHour
      | x == 24 * 60 * 60 = Just QH.PeriodDay
      | otherwise = Nothing

