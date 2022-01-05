{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DuplicateRecordFields      #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# OPTIONS_GHC -Wno-unused-imports #-}

module ATrade.Driver.Junction
  (
    junctionMain
  ) where

import           ATrade.Broker.Client                        (startBrokerClient,
                                                              stopBrokerClient)
import           ATrade.Broker.Protocol                      (Notification (OrderNotification, TradeNotification),
                                                              NotificationSqnum (unNotificationSqnum),
                                                              getNotificationSqnum)
import           ATrade.Driver.Junction.BrokerService        (BrokerService,
                                                              getNotifications,
                                                              mkBrokerService)
import           ATrade.Driver.Junction.JunctionMonad        (JunctionEnv (..),
                                                              JunctionM (..),
                                                              saveRobots,
                                                              startRobot)
import           ATrade.Driver.Junction.ProgramConfiguration (ProgramConfiguration (..),
                                                              ProgramOptions (ProgramOptions, configPath))
import           ATrade.Driver.Junction.QuoteThread          (DownloaderEnv (DownloaderEnv),
                                                              withQThread)
import           ATrade.Driver.Junction.RemoteControl        (handleRemoteControl)
import           ATrade.Driver.Junction.RobotDriverThread    (RobotDriverHandle, postNotificationEvent)
import           ATrade.Driver.Junction.Types                (StrategyDescriptorE,
                                                              confStrategy,
                                                              confTickers,
                                                              strategyState,
                                                              strategyTimers,
                                                              tickerId,
                                                              timeframe)
import           ATrade.Logging                              (Message, Severity (Debug, Info, Trace, Warning),
                                                              fmtMessage,
                                                              logWith)
import           ATrade.Quotes.QHP                           (mkQHPHandle)
import           ATrade.RoboCom.Types                        (Bars,
                                                              TickerInfoMap)
import           ATrade.Types                                (ClientSecurityParams (ClientSecurityParams),
                                                              OrderId,
                                                              Trade (tradeOrderId))
import           Colog                                       (LogAction (LogAction),
                                                              hoistLogAction,
                                                              logTextStdout,
                                                              (<&), (>$<))
import           Colog.Actions                               (logTextHandle)
import           Control.Concurrent.QSem                     (newQSem)
import           Control.Monad                               (forM_, forever)
import           Control.Monad.Extra                         (whenM)
import           Control.Monad.IO.Class                      (MonadIO (liftIO))
import           Control.Monad.Reader                        (ReaderT (runReaderT))
import           Data.IORef                                  (IORef,
                                                              atomicModifyIORef',
                                                              newIORef,
                                                              readIORef)
import qualified Data.Map.Strict                             as M
import           Data.Set                                    (notMember)
import qualified Data.Set                                    as S
import qualified Data.Text                                   as T
import           Data.Text.Encoding                          (encodeUtf8)
import           Data.Text.IO                                (readFile)
import           Database.Redis                              (ConnectInfo (..), PortID (UnixSocket),
                                                              checkedConnect,
                                                              defaultConnectInfo)
import           Dhall                                       (auto, input)
import           Options.Applicative                         (Parser,
                                                              execParser,
                                                              fullDesc, header,
                                                              help, helper,
                                                              info, long,
                                                              metavar, progDesc,
                                                              short, strOption,
                                                              (<**>))
import           Prelude                                     hiding (log,
                                                              readFile)
import           System.IO                                   (BufferMode (LineBuffering),
                                                              Handle,
                                                              IOMode (AppendMode),
                                                              hSetBuffering,
                                                              withFile)
import           System.ZMQ4                                 (Rep (Rep), bind,
                                                              withContext,
                                                              withSocket)
import           System.ZMQ4.ZAP                             (loadCertificateFromFile)
import           UnliftIO                                    (MonadUnliftIO)
import           UnliftIO.Exception                          (bracket)
import           UnliftIO.QSem                               (QSem, withQSem)


locked :: (MonadIO m, MonadUnliftIO m) => QSem -> LogAction m a -> LogAction m a
locked sem action = LogAction (\m -> withQSem sem (action <& m))

logger :: (MonadIO m) => Handle -> LogAction m Message
logger h = fmtMessage >$< (logTextStdout <> logTextHandle h)

junctionMain :: M.Map T.Text StrategyDescriptorE -> IO ()
junctionMain descriptors = do
  opts <- parseOptions

  let initialLogger = fmtMessage >$< logTextStdout

  logWith initialLogger Info "Junction" $ "Reading config from: " <> (T.pack . show) (configPath opts)

  cfg <- readFile (configPath opts) >>= input auto

  withFile (logBasePath cfg <> "/all.log") AppendMode $ \h -> do

    hSetBuffering h LineBuffering

    locksem <- newQSem 1
    let globalLogger = locked locksem (logger h)
    let log = logWith globalLogger

    barsMap <- newIORef M.empty
    tickerInfoMap <- newIORef M.empty

    log Info "Junction" $ "Connecting to redis: " <> redisSocket cfg
    redis <- checkedConnect (defaultConnectInfo { connectPort = UnixSocket (T.unpack $ redisSocket cfg) })
    log Info "Junction" "redis: connected"
    withContext $ \ctx -> do
      log Debug "Junction" "0mq context created"
      let downloaderEnv = DownloaderEnv (mkQHPHandle ctx (qhpEndpoint cfg)) ctx (qtisEndpoint cfg) (hoistLogAction liftIO globalLogger)
      robotsMap <- newIORef M.empty
      ordersMap <- newIORef M.empty
      handledNotifications <- newIORef S.empty
      withBroker cfg ctx robotsMap ordersMap handledNotifications globalLogger $ \bro ->
        withQThread downloaderEnv barsMap tickerInfoMap cfg ctx globalLogger $ \qt ->
          withSocket ctx Rep $ \rcSocket -> do
            liftIO $ bind rcSocket (T.unpack . remoteControlEndpoint $ cfg)
            broService <- mkBrokerService bro ordersMap
            let junctionLogAction = hoistLogAction liftIO globalLogger
            let env =
                  JunctionEnv
                  {
                    peRedisSocket = redis,
                    peConfigPath = robotsConfigsPath cfg,
                    peQuoteThread = qt,
                    peBroker = bro,
                    peRobots = robotsMap,
                    peRemoteControlSocket = rcSocket,
                    peLogAction = junctionLogAction,
                    peIoLogAction = globalLogger,
                    peProgramConfiguration = cfg,
                    peBarsMap = barsMap,
                    peTickerInfoMap = tickerInfoMap,
                    peBrokerService = broService,
                    peDescriptors = descriptors
                  }
            withJunction env $ do
              startRobots cfg
              forever $ do
                notifications <- liftIO $ getNotifications broService
                forM_ notifications (liftIO . handleBrokerNotification robotsMap ordersMap handledNotifications globalLogger)
                saveRobots
                handleRemoteControl 1000
  where
    startRobots :: ProgramConfiguration -> JunctionM ()
    startRobots cfg = forM_ (instances cfg) $ \inst -> do
      startRobot inst

    withJunction :: JunctionEnv -> JunctionM () -> IO ()
    withJunction env = (`runReaderT` env) . unJunctionM

    handleBrokerNotification :: IORef (M.Map T.Text RobotDriverHandle) ->
                                IORef (M.Map OrderId T.Text) ->
                                IORef (S.Set NotificationSqnum) ->
                                LogAction IO Message ->
                                Notification ->
                                IO ()
    handleBrokerNotification robotsRef ordersMapRef handled logger' notification= do
      logWith logger' Trace "Junction" $ "Incoming notification: " <> (T.pack . show . unNotificationSqnum . getNotificationSqnum) notification
      whenM (notMember (getNotificationSqnum notification) <$> readIORef handled) $ do
        robotsMap <- readIORef robotsRef
        ordersMap <- readIORef ordersMapRef

        case getNotificationTarget robotsMap ordersMap notification of
          Just robot -> postNotificationEvent robot notification
          Nothing    -> do
            logWith logger' Warning "Junction" $ "Unknown order: " <> (T.pack . show) (notificationOrderId notification)
            logWith logger' Debug "Junction" $ "Ordermap: " <> (T.pack . show) (M.toList ordersMap)

        atomicModifyIORef' handled (\s -> (S.insert (getNotificationSqnum notification) s, ()))

    getNotificationTarget :: M.Map T.Text RobotDriverHandle -> M.Map OrderId T.Text -> Notification -> Maybe RobotDriverHandle
    getNotificationTarget robotsMap ordersMap notification = do
      robotId <- M.lookup (notificationOrderId notification) ordersMap
      M.lookup robotId robotsMap

    notificationOrderId (OrderNotification _ oid _) = oid
    notificationOrderId (TradeNotification _ trade) = tradeOrderId trade

    withBroker cfg ctx robotsMap ordersMap handled logger' f = do
      securityParameters <- loadBrokerSecurityParameters cfg
      bracket
        (startBrokerClient
          (encodeUtf8 $ brokerIdentity cfg)
          ctx
          (brokerEndpoint cfg)
          (brokerNotificationEndpoint cfg)
          [handleBrokerNotification robotsMap ordersMap handled logger']
          securityParameters
          logger')
        stopBrokerClient f

    loadBrokerSecurityParameters cfg =
      case (brokerClientCert cfg, brokerServerCert cfg) of
        (Just clientCertPath, Just serverCertPath) -> do
          eClientCert <- loadCertificateFromFile clientCertPath
          eServerCert <- loadCertificateFromFile serverCertPath
          case (eClientCert, eServerCert) of
            (Right clientCert, Right serverCert) -> return $ ClientSecurityParams (Just clientCert) (Just serverCert)
            (_, _) -> return $ ClientSecurityParams Nothing Nothing

        _ -> return $ ClientSecurityParams Nothing Nothing

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

