{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}


module ATrade.Driver.Junction.JunctionMonad
  (
    JunctionEnv(..),
    JunctionM(..),
    startRobot,
    saveRobots
  ) where

import           ATrade.Broker.Client                        (BrokerClientHandle)
import           ATrade.Driver.Junction.BrokerService        (BrokerService)
import           ATrade.Driver.Junction.ProgramConfiguration (ProgramConfiguration (logBasePath))
import           ATrade.Driver.Junction.QuoteStream          (QuoteStream (addSubscription, removeSubscription),
                                                              QuoteSubscription (QuoteSubscription),
                                                              SubscriptionId (SubscriptionId))
import           ATrade.Driver.Junction.QuoteThread          (QuoteThreadHandle)
import qualified ATrade.Driver.Junction.QuoteThread          as QT
import           ATrade.Driver.Junction.RobotDriverThread    (RobotDriverHandle, RobotEnv (RobotEnv),
                                                              RobotM (unRobotM),
                                                              createRobotDriverThread,
                                                              onStrategyInstance)
import           ATrade.Driver.Junction.Types                (StrategyDescriptorE (StrategyDescriptorE),
                                                              StrategyInstanceDescriptor,
                                                              accountId,
                                                              confStrategy,
                                                              confTickers,
                                                              configKey,
                                                              stateKey,
                                                              strategyBaseName,
                                                              strategyId,
                                                              strategyInstanceId,
                                                              strategyState,
                                                              strategyTimers,
                                                              tickerId,
                                                              timeframe)
import           ATrade.Logging                              (Message, Severity (Error, Info),
                                                              fmtMessage,
                                                              logWarning,
                                                              logWith)
import           ATrade.RoboCom.ConfigStorage                (ConfigStorage (loadConfig))
import           ATrade.RoboCom.Monad                        (StrategyEnvironment (..))
import           ATrade.RoboCom.Persistence                  (MonadPersistence (loadState, saveState))
import           ATrade.RoboCom.Types                        (BarSeriesId (BarSeriesId),
                                                              Bars,
                                                              TickerInfoMap)
import           Colog                                       (HasLog (getLogAction, setLogAction),
                                                              LogAction,
                                                              hoistLogAction,
                                                              logTextHandle,
                                                              (>$<))
import           Control.Exception.Safe                      (MonadThrow)
import           Control.Monad.Reader                        (MonadIO (liftIO),
                                                              MonadReader,
                                                              ReaderT (runReaderT),
                                                              asks)
import           Data.Aeson                                  (eitherDecode,
                                                              encode)
import qualified Data.ByteString.Lazy                        as BL
import           Data.Default                                (Default (def))
import           Data.Foldable                               (traverse_)
import           Data.IORef                                  (IORef,
                                                              atomicModifyIORef',
                                                              newIORef,
                                                              readIORef)
import           Data.List.NonEmpty                          (NonEmpty ((:|)))
import qualified Data.Map.Strict                             as M
import qualified Data.Text                                   as T
import           Data.Text.Encoding                          (encodeUtf8)
import           Data.Text.IO                                (readFile)
import           Data.Time                                   (getCurrentTime)
import           Data.Time.Clock.POSIX                       (getPOSIXTime)
import           Database.Redis                              (Connection, get,
                                                              mset, runRedis)
import           Dhall                                       (auto, input)
import           Prelude                                     hiding (readFile)
import           System.IO                                   (BufferMode (LineBuffering),
                                                              IOMode (AppendMode),
                                                              hSetBuffering,
                                                              openFile)
import           System.ZMQ4                                 (Rep, Socket)

data JunctionEnv =
  JunctionEnv
  {
    peRedisSocket         :: Connection,
    peConfigPath          :: FilePath,
    peQuoteThread         :: QuoteThreadHandle,
    peBroker              :: BrokerClientHandle,
    peRobots              :: IORef (M.Map T.Text RobotDriverHandle),
    peRemoteControlSocket :: Socket Rep,
    peLogAction           :: LogAction JunctionM Message
  }

newtype JunctionM a = JunctionM { unJunctionM :: ReaderT JunctionEnv IO a }
  deriving (Functor, Applicative, Monad, MonadReader JunctionEnv, MonadIO, MonadThrow)

instance HasLog JunctionEnv Message JunctionM where
  getLogAction = peLogAction
  setLogAction a e = e { peLogAction = a }

instance ConfigStorage JunctionM where
  loadConfig key = do
    basePath <- asks peConfigPath
    let path = basePath <> "/" <> T.unpack key -- TODO fix path construction
    liftIO $ readFile path >>= input auto

instance MonadPersistence JunctionM where
  saveState newState key = do
    conn <- asks peRedisSocket
    now <- liftIO getPOSIXTime
    res <- liftIO $ runRedis conn $ mset [(encodeUtf8 key, BL.toStrict $ encode newState),
                                 (encodeUtf8 (key <> ":last_store") , encodeUtf8 . T.pack . show $ now)]
    case res of
      Left _  -> logWarning "Junction " "Unable to save state"
      Right _ -> return ()

  loadState key = do
    conn <- asks peRedisSocket
    res <- liftIO $ runRedis conn $ get (encodeUtf8 key)
    -- TODO: just chain eithers
    case res of
      Left _ -> do
        logWarning "Junction" "Unable to load state"
        return def
      Right maybeRawState ->
        case maybeRawState of
          Just rawState -> case eitherDecode $ BL.fromStrict rawState of
            Left _ -> do
              logWarning "Junction" "Unable to decode state"
              return def
            Right decodedState -> return decodedState
          Nothing -> do
            logWarning "Junction" "Unable to decode state"
            return def

instance QuoteStream JunctionM where
  addSubscription (QuoteSubscription ticker tf) chan = do
    qt <- asks peQuoteThread
    QT.addSubscription qt ticker tf chan
  removeSubscription _ = undefined

startRobot :: LogAction IO Message -> ProgramConfiguration -> IORef Bars -> IORef TickerInfoMap ->
  BrokerService -> M.Map T.Text StrategyDescriptorE -> StrategyInstanceDescriptor -> JunctionM ()
startRobot ioLogger cfg barsMap tickerInfoMap broService descriptors inst = do
  logger <- asks peLogAction
  let log = logWith logger
  now <- liftIO getCurrentTime
  let lLogger = hoistLogAction liftIO ioLogger
  logWith lLogger Info "Junction" $ "Starting strategy: " <> strategyBaseName inst
  case M.lookup (strategyBaseName inst) descriptors of
    Just (StrategyDescriptorE desc) -> do
      bigConf <- loadConfig (configKey inst)
      case confTickers bigConf of
        (firstTicker:restTickers) -> do
          rConf <- liftIO $ newIORef (confStrategy bigConf)
          rState <- loadState (stateKey inst) >>= liftIO . newIORef
          rTimers <- loadState (stateKey inst <> ":timers") >>= liftIO . newIORef
          localH <- liftIO $ openFile (logBasePath cfg <> "/" <> T.unpack (strategyId inst) <> ".log") AppendMode
          liftIO $ hSetBuffering localH LineBuffering
          let robotLogAction = hoistLogAction liftIO ioLogger <> (fmtMessage >$< logTextHandle localH)
          stratEnv <- liftIO $ newIORef StrategyEnvironment
                         {
                           _seInstanceId = strategyId inst,
                           _seAccount = accountId inst,
                           _seVolume = 1,
                           _seLastTimestamp = now
                         }
          let robotEnv =
                RobotEnv rState rConf rTimers barsMap tickerInfoMap stratEnv robotLogAction broService (toBarSeriesId <$> (firstTicker :| restTickers))
          robot <- createRobotDriverThread inst desc (flip runReaderT robotEnv . unRobotM) bigConf rConf rState rTimers
          robotsMap' <- asks peRobots
          liftIO $ atomicModifyIORef' robotsMap' (\s -> (M.insert (strategyId inst) robot s, ()))
        _ -> logWith lLogger Error (strategyId inst) "No tickers configured !!!"
    Nothing   -> logWith lLogger Error "Junction" $ "Unknown strategy: " <> strategyBaseName inst

  where
    toBarSeriesId t = BarSeriesId (tickerId t) (timeframe t)

saveRobots :: JunctionM ()
saveRobots = do
  robotsMap <- asks peRobots >>= (liftIO . readIORef)
  traverse_ saveRobotState robotsMap

saveRobotState :: RobotDriverHandle -> JunctionM ()
saveRobotState handle = onStrategyInstance handle $ \inst -> do
  currentState <- liftIO $ readIORef (strategyState inst)
  saveState currentState (strategyInstanceId inst)
  currentTimers <- liftIO $ readIORef (strategyTimers inst)
  saveState currentTimers (strategyInstanceId inst <> ":timers")
