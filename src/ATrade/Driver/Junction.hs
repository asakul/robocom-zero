{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DuplicateRecordFields      #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}

module ATrade.Driver.Junction
  (
    junctionMain
  ) where

import           ATrade.Broker.Client                        (BrokerClientHandle,
                                                              startBrokerClient,
                                                              stopBrokerClient)
import           ATrade.Driver.Junction.ProgramConfiguration (ProgramConfiguration (brokerEndpoint, brokerNotificationEndpoint, instances, qhpEndpoint, qtisEndpoint, redisSocket, robotsConfigsPath),
                                                              ProgramOptions (ProgramOptions, configPath))
import           ATrade.Driver.Junction.QuoteStream          (QuoteStream (addSubscription, removeSubscription),
                                                              QuoteSubscription (QuoteSubscription),
                                                              SubscriptionId (SubscriptionId))
import           ATrade.Driver.Junction.QuoteThread          (DownloaderEnv (DownloaderEnv),
                                                              QuoteThreadHandle,
                                                              withQThread)
import qualified ATrade.Driver.Junction.QuoteThread          as QT
import           ATrade.Driver.Junction.RobotDriverThread    (RobotDriverHandle,
                                                              RobotEnv (..),
                                                              RobotM (..),
                                                              createRobotDriverThread,
                                                              onStrategyInstance)
import           ATrade.Driver.Junction.Types                (StrategyDescriptorE (StrategyDescriptorE),
                                                              StrategyInstance (strategyInstanceId),
                                                              StrategyInstanceDescriptor (..),
                                                              confStrategy,
                                                              strategyState)
import           ATrade.Quotes.QHP                           (mkQHPHandle)
import           ATrade.RoboCom.ConfigStorage                (ConfigStorage (loadConfig))
import           ATrade.RoboCom.Persistence                  (MonadPersistence (loadState, saveState))
import           ATrade.Types                                (ClientSecurityParams (ClientSecurityParams))
import           Control.Concurrent
import           Control.Exception.Safe                      (MonadThrow,
                                                              bracket)
import           Control.Monad                               (forM_, forever)
import           Control.Monad.IO.Class                      (MonadIO (liftIO))
import           Control.Monad.Reader                        (MonadReader, ReaderT (runReaderT),
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
import qualified Data.Map.Strict                             as M
import qualified Data.Text                                   as T
import           Data.Text.Encoding                          (encodeUtf8)
import           Data.Text.IO                                (readFile)
import           Data.Time.Clock.POSIX                       (getPOSIXTime)
import           Database.Redis                              (ConnectInfo (..),
                                                              Connection,
                                                              PortID (UnixSocket),
                                                              checkedConnect,
                                                              defaultConnectInfo,
                                                              get, mset,
                                                              runRedis)
import           Dhall                                       (auto, input)
import           Options.Applicative                         (Parser,
                                                              execParser,
                                                              fullDesc, header,
                                                              help, helper,
                                                              info, long,
                                                              metavar, progDesc,
                                                              short, strOption,
                                                              (<**>))
import           Prelude                                     hiding (readFile)
import           System.Log.Logger                           (warningM)
import           System.ZMQ4                                 (withContext)

data JunctionEnv =
  JunctionEnv
  {
    peRedisSocket :: Connection,
    peConfigPath  :: FilePath,
    peQuoteThread :: QuoteThreadHandle,
    peBroker      :: BrokerClientHandle,
    peRobots      :: IORef (M.Map T.Text RobotDriverHandle)
  }

newtype JunctionM a = JunctionM { unJunctionM :: ReaderT JunctionEnv IO a }
  deriving (Functor, Applicative, Monad, MonadReader JunctionEnv, MonadIO, MonadThrow)

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
      Left _  -> liftIO $ warningM "main" "Unable to save state"
      Right _ -> return ()

  loadState key = do
    conn <- asks peRedisSocket
    res <- liftIO $ runRedis conn $ get (encodeUtf8 key)
    -- TODO: just chain eithers
    case res of
      Left _ -> do
        liftIO $ warningM "main" "Unable to load state"
        return def
      Right maybeRawState ->
        case maybeRawState of
          Just rawState -> case eitherDecode $ BL.fromStrict rawState of
            Left _ -> do
              liftIO $ warningM "main" "Unable to decode state"
              return def
            Right decodedState -> return decodedState
          Nothing -> do
            liftIO $ warningM "main" "Unable to decode state"
            return def

instance QuoteStream JunctionM where
  addSubscription (QuoteSubscription ticker timeframe) chan = do
    qt <- asks peQuoteThread
    QT.addSubscription qt ticker timeframe chan
    return (SubscriptionId 0) -- TODO subscription Ids
  removeSubscription _ = undefined

junctionMain :: M.Map T.Text StrategyDescriptorE -> IO ()
junctionMain descriptors = do
  opts <- parseOptions

  cfg <- readFile (configPath opts) >>= input auto

  barsMap <- newIORef M.empty

  redis <- checkedConnect (defaultConnectInfo { connectPort = UnixSocket (T.unpack $ redisSocket cfg) })
  withContext $ \ctx -> do
    let downloaderEnv = DownloaderEnv (mkQHPHandle ctx (qhpEndpoint cfg)) ctx (qtisEndpoint cfg)
    withBroker cfg ctx $ \bro ->
      withQThread downloaderEnv barsMap cfg ctx $ \qt -> do
        robotsMap <- newIORef M.empty
        let env =
              JunctionEnv
              {
                peRedisSocket = redis,
                peConfigPath = robotsConfigsPath cfg,
                peQuoteThread = qt,
                peBroker = bro,
                peRobots = robotsMap
              }
        withJunction env $ do
          startRobots cfg bro barsMap
          forever $ do
            saveRobots
            liftIO $ threadDelay 5000000
  where
    saveRobots :: JunctionM ()
    saveRobots = do
      robotsMap <- asks peRobots >>= (liftIO . readIORef)
      traverse_ saveRobotState robotsMap

    saveRobotState :: RobotDriverHandle -> JunctionM ()
    saveRobotState handle = onStrategyInstance handle $ \inst -> do
      currentState <- liftIO $ readIORef (strategyState inst)
      saveState currentState (strategyInstanceId inst)

    startRobots cfg bro barsMap = forM_ (instances cfg) $ \inst ->
      case M.lookup (strategyBaseName inst) descriptors of
        Just (StrategyDescriptorE desc) -> do
          bigConf <- loadConfig (configKey inst)
          rConf <- liftIO $ newIORef (confStrategy bigConf)
          rState <- loadState (stateKey inst) >>= liftIO . newIORef
          rTimers <- loadState (stateKey inst <> ":timers") >>= liftIO . newIORef
          let robotEnv = RobotEnv rState rConf rTimers bro barsMap
          robot <- createRobotDriverThread inst desc (flip runReaderT robotEnv . unRobotM) bigConf rConf rState
          robotsMap' <- asks peRobots
          liftIO $ atomicModifyIORef' robotsMap' (\s -> (M.insert (strategyId inst) robot s, ()))
        Nothing   -> error "Unknown strategy"

    withJunction :: JunctionEnv -> JunctionM () -> IO ()
    withJunction env = (`runReaderT` env) . unJunctionM

    withBroker cfg ctx f = bracket
      (startBrokerClient
        "broker"
        ctx
        (brokerEndpoint cfg)
        (brokerNotificationEndpoint cfg)
        []
        (ClientSecurityParams -- TODO load certificates from file
         Nothing
         Nothing))
      stopBrokerClient f
    parseOptions = execParser options
    options = info (optionsParser <**> helper)
      (fullDesc <>
       progDesc "Robocom-zero junction mode driver" <>
       header "robocom-zero-junction")

    optionsParser :: Parser ProgramOptions
    optionsParser = ProgramOptions
      <$> strOption
            (long "config" <>
             short 'c' <>
             metavar "FILENAME" <>
             help "Configuration file path")

