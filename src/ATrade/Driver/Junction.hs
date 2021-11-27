{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DuplicateRecordFields      #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}

module ATrade.Driver.Junction
  (
    junctionMain
  ) where

import           ATrade.Broker.Client                        (startBrokerClient,
                                                              stopBrokerClient)
import           ATrade.Driver.Junction.ProgramConfiguration (ProgramConfiguration (brokerEndpoint, brokerNotificationEndpoint, instances, qhpEndpoint, qtisEndpoint, redisSocket, robotsConfigsPath),
                                                              ProgramOptions (ProgramOptions, configPath))
import           ATrade.Driver.Junction.QuoteStream          (QuoteStream (addSubscription, removeSubscription))
import           ATrade.Driver.Junction.QuoteThread          (DownloaderEnv (DownloaderEnv),
                                                              withQThread)
import           ATrade.Driver.Junction.RobotDriverThread    (createRobotDriverThread)
import           ATrade.Driver.Junction.Types                (StrategyDescriptorE (StrategyDescriptorE),
                                                              StrategyInstanceDescriptor (..),
                                                              confStrategy)
import           ATrade.Quotes.QHP                           (mkQHPHandle)
import           ATrade.RoboCom.ConfigStorage                (ConfigStorage (loadConfig))
import           ATrade.RoboCom.Monad                        (MonadRobot (..))
import           ATrade.RoboCom.Persistence                  (MonadPersistence (loadState, saveState))
import           ATrade.Types                                (ClientSecurityParams (ClientSecurityParams))
import           Control.Exception.Safe                      (MonadThrow,
                                                              bracket)
import           Control.Monad                               (forM_)
import           Control.Monad.IO.Class                      (MonadIO (liftIO))
import           Control.Monad.Reader                        (MonadReader, ReaderT (runReaderT),
                                                              asks)
import           Data.Aeson                                  (eitherDecode,
                                                              encode)
import qualified Data.ByteString.Lazy                        as BL
import           Data.Default                                (Default (def))
import           Data.IORef                                  (IORef, newIORef)
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

data PersistenceEnv =
  PersistenceEnv
  {
    peRedisSocket :: Connection,
    peConfigPath  :: FilePath
  }

newtype PersistenceT a = PersistenceT { unPersistenceT :: ReaderT PersistenceEnv IO a }
  deriving (Functor, Applicative, Monad, MonadReader PersistenceEnv, MonadIO, MonadThrow)

instance ConfigStorage PersistenceT where
  loadConfig key = do
    basePath <- asks peConfigPath
    let path = basePath <> "/" <> T.unpack key -- TODO fix path construction
    liftIO $ readFile path >>= input auto

instance MonadPersistence PersistenceT where
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

instance QuoteStream PersistenceT where
  addSubscription sub chan = undefined
  removeSubscription sub = undefined

data RobotEnv c s =
  RobotEnv
  {
    stateRef  :: IORef s,
    configRef :: IORef c
  }

newtype RobotM c s a = RobotM { unRobotM :: ReaderT (RobotEnv c s) IO a }
  deriving (Functor, Applicative, Monad, MonadReader (RobotEnv c s), MonadIO, MonadThrow)

instance MonadRobot (RobotM c s) c s where
  submitOrder = undefined
  cancelOrder = undefined
  appendToLog = undefined
  setupTimer = undefined
  enqueueIOAction = undefined
  getConfig = undefined
  getState = undefined
  setState = undefined
  getEnvironment = undefined
  getTicker  = undefined

junctionMain :: M.Map T.Text StrategyDescriptorE -> IO ()
junctionMain descriptors = do
  opts <- parseOptions

  cfg <- readFile (configPath opts) >>= input auto

  barsMap <- newIORef M.empty

  redis <- checkedConnect (defaultConnectInfo { connectPort = UnixSocket (T.unpack $ redisSocket cfg) })
  withContext $ \ctx -> do
    let env = DownloaderEnv (mkQHPHandle ctx (qhpEndpoint cfg)) ctx (qtisEndpoint cfg)
    withBroker cfg ctx $ \bro ->
      withQThread env barsMap cfg ctx $ \qt ->
        withPersistence (PersistenceEnv redis $ robotsConfigsPath cfg) $
          forM_ (instances cfg) $ \inst ->
            case M.lookup (strategyBaseName inst) descriptors of
              Just (StrategyDescriptorE desc) -> do
                bigConf <- loadConfig (configKey inst)
                rConf <- liftIO $ newIORef (confStrategy bigConf)
                rState <- loadState (stateKey inst) >>= liftIO . newIORef
                let robotEnv = RobotEnv rState rConf
                createRobotDriverThread inst desc (flip runReaderT robotEnv . unRobotM) bigConf rConf rState
              Nothing   -> error "Unknown strategy"
  where
    withPersistence :: PersistenceEnv -> PersistenceT () -> IO ()
    withPersistence env = (`runReaderT` env) . unPersistenceT

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

