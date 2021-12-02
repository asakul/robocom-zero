{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}

module ATrade.Driver.Junction.RobotDriverThread
  (
  createRobotDriverThread,
  RobotEnv(..),
  RobotM(..),
  RobotDriverHandle,
  onStrategyInstance,
  postNotificationEvent) where

import Prelude hiding (log)
import           ATrade.Broker.Protocol               (Notification (OrderNotification, TradeNotification))
import qualified ATrade.Driver.Junction.BrokerService as Bro
import           ATrade.Driver.Junction.QuoteStream   (QuoteStream (addSubscription),
                                                       QuoteSubscription (QuoteSubscription))
import           ATrade.Driver.Junction.Types         (BigConfig,
                                                       StrategyDescriptor,
                                                       StrategyInstance (StrategyInstance, strategyEventCallback),
                                                       StrategyInstanceDescriptor (configKey),
                                                       confStrategy,
                                                       confTickers,
                                                       eventCallback, stateKey,
                                                       strategyId, tickerId,
                                                       timeframe)
import           ATrade.Logging                       (Message, logDebug,
                                                       logInfo, logWarning, log)
import           ATrade.QuoteSource.Client            (QuoteData (..))
import           ATrade.RoboCom.ConfigStorage         (ConfigStorage)
import           ATrade.RoboCom.Monad                 (Event (NewBar, NewTick, NewTrade, OrderSubmitted, OrderUpdate),
                                                       MonadRobot (..),
                                                       StrategyEnvironment (StrategyEnvironment, _seInstanceId, _seLastTimestamp))
import           ATrade.RoboCom.Persistence           (MonadPersistence)
import           ATrade.RoboCom.Types                 (BarSeriesId (BarSeriesId),
                                                       Bars)
import           ATrade.Types                         (Order (orderId), OrderId,
                                                       OrderState, Trade)
import           Colog                                (HasLog (getLogAction, setLogAction),
                                                       LogAction)
import           Control.Concurrent                   (ThreadId, forkIO)
import           Control.Concurrent.BoundedChan       (BoundedChan,
                                                       newBoundedChan, readChan,
                                                       writeChan)
import           Control.Exception.Safe               (MonadThrow)
import           Control.Monad                        (forM_, forever, void)
import           Control.Monad.IO.Class               (MonadIO, liftIO)
import           Control.Monad.Reader                 (MonadReader (local),
                                                       ReaderT, asks)
import           Data.Aeson                           (FromJSON, ToJSON)
import           Data.Default
import           Data.IORef                           (IORef,
                                                       atomicModifyIORef',
                                                       readIORef, writeIORef)
import qualified Data.Map.Strict                      as M
import qualified Data.Text.Lazy                       as TL
import           Data.Time                            (UTCTime, getCurrentTime)
import           Dhall                                (FromDhall)

data RobotDriverHandle = forall c s. (FromDhall c, Default s, FromJSON s, ToJSON s) =>
                           RobotDriverHandle (StrategyInstance c s) ThreadId ThreadId (BoundedChan RobotDriverEvent)

data RobotDriverRequest

data RobotDriverEvent =
    EventRequest RobotDriverRequest
  | QuoteEvent QuoteData
  | NewTradeEvent Trade
  | OrderEvent OrderId OrderState


robotDriverThread :: (MonadIO m,
                      MonadRobot m c s) =>
  StrategyInstance c s ->
  BoundedChan RobotDriverEvent ->
  m ()

robotDriverThread inst eventQueue =
  forever $ liftIO (readChan eventQueue) >>= handleEvent
  where
    handleEvent (EventRequest _)           = return ()
    handleEvent (QuoteEvent d) =
      case d of
        QDTick tick     -> strategyEventCallback inst (NewTick tick)
        QDBar (tf, bar) -> strategyEventCallback inst (NewBar (tf, bar))
    handleEvent (NewTradeEvent trade)      = strategyEventCallback inst (NewTrade trade)
    handleEvent (OrderEvent oid newState)  = strategyEventCallback inst (OrderUpdate oid newState)

createRobotDriverThread :: (MonadIO m1,
                            ConfigStorage m1,
                            MonadPersistence m1,
                            QuoteStream m1,
                            Default s,
                            FromJSON s,
                            ToJSON s,
                            FromDhall c,
                            MonadIO m,
                            MonadReader (RobotEnv c s) m,
                            MonadRobot m c s) =>
     StrategyInstanceDescriptor
  -> StrategyDescriptor c s
  -> (m () -> IO ())
  -> BigConfig c
  -> IORef c
  -> IORef s
  -> IORef [UTCTime]
  -> m1 RobotDriverHandle

createRobotDriverThread instDesc strDesc runner bigConf rConf rState rTimers = do
  eventQueue <- liftIO $ newBoundedChan 2000

  let inst = StrategyInstance (strategyId instDesc) (eventCallback strDesc) rState rConf rTimers

  quoteQueue <- liftIO $ newBoundedChan 2000
  forM_ (confTickers bigConf) (\x -> addSubscription (QuoteSubscription (tickerId x) (timeframe x)) quoteQueue)
  qthread <- liftIO . forkIO $ forever $ passQuoteEvents eventQueue quoteQueue

  driver <- liftIO . forkIO $ runner  $ robotDriverThread inst eventQueue
  return $ RobotDriverHandle inst driver qthread eventQueue

  where
    passQuoteEvents eventQueue quoteQueue = do
      v <- readChan quoteQueue
      writeChan eventQueue (QuoteEvent v)

onStrategyInstance :: RobotDriverHandle -> forall r. (forall c s. (FromDhall c, Default s, FromJSON s, ToJSON s) => StrategyInstance c s -> r) -> r
onStrategyInstance (RobotDriverHandle inst _ _ _) f = f inst

data RobotEnv c s =
  RobotEnv
  {
    stateRef      :: IORef s,
    configRef     :: IORef c,
    timersRef     :: IORef [UTCTime],
    bars          :: IORef Bars,
    env           :: IORef StrategyEnvironment,
    logAction     :: LogAction (RobotM c s) Message,
    brokerService :: Bro.BrokerService
  }

newtype RobotM c s a = RobotM { unRobotM :: ReaderT (RobotEnv c s) IO a }
  deriving (Functor, Applicative, Monad, MonadReader (RobotEnv c s), MonadIO, MonadThrow)

instance HasLog (RobotEnv c s) Message (RobotM c s) where
  getLogAction = logAction
  setLogAction a e = e { logAction = a }

instance MonadRobot (RobotM c s) c s where
  submitOrder order = do
    instId <- _seInstanceId <$> (asks env >>= liftIO . readIORef)
    bro <- asks brokerService
    Bro.submitOrder bro instId order

  cancelOrder oid = do
    bro <- asks brokerService
    liftIO . void $ Bro.cancelOrder bro oid

  appendToLog s t = do
    instId <- _seInstanceId <$> (asks env >>= liftIO . readIORef)
    log s instId $ TL.toStrict t

  setupTimer t = do
    ref <- asks timersRef
    liftIO $ atomicModifyIORef' ref (\s -> (t : s, ()))

  enqueueIOAction = undefined
  getConfig = asks configRef >>= liftIO . readIORef
  getState = asks stateRef >>= liftIO . readIORef
  setState newState = asks stateRef >>= liftIO . flip writeIORef newState
  getEnvironment = do
    ref <- asks env
    now <- liftIO getCurrentTime
    liftIO $ atomicModifyIORef' ref (\e -> (e { _seLastTimestamp = now }, e { _seLastTimestamp = now}))

  getTicker tid tf = do
    b <- asks bars >>= liftIO . readIORef
    return $ M.lookup (BarSeriesId tid tf) b

postNotificationEvent :: (MonadIO m) => RobotDriverHandle -> Notification -> m ()
postNotificationEvent (RobotDriverHandle _ _ _ eventQueue) notification = liftIO $
  case notification of
    OrderNotification _ oid state -> writeChan eventQueue (OrderEvent oid state)
    TradeNotification _ trade -> writeChan eventQueue (NewTradeEvent trade)


