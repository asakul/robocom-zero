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
  onStrategyInstanceM,
  postNotificationEvent,
  stopRobot,
  getInstanceDescriptor
  ) where

import           ATrade.Broker.Protocol               (Notification (OrderNotification, TradeNotification))
import qualified ATrade.Driver.Junction.BrokerService as Bro
import           ATrade.Driver.Junction.QuoteStream   (QuoteStream (addSubscription, removeSubscription),
                                                       QuoteSubscription (QuoteSubscription),
                                                       SubscriptionId)
import           ATrade.Driver.Junction.Types         (BigConfig,
                                                       StrategyDescriptor,
                                                       StrategyInstance (StrategyInstance, strategyEventCallback),
                                                       StrategyInstanceDescriptor (configKey),
                                                       confStrategy,
                                                       confTickers,
                                                       eventCallback, stateKey,
                                                       strategyId, tickerId,
                                                       timeframe)
import           ATrade.Logging                       (Message, log)
import           ATrade.QuoteSource.Client            (QuoteData (..))
import           ATrade.RoboCom.ConfigStorage         (ConfigStorage)
import           ATrade.RoboCom.Monad                 (Event (NewBar, NewTick, NewTrade, OrderUpdate),
                                                       MonadRobot (..),
                                                       StrategyEnvironment (..))
import           ATrade.RoboCom.Persistence           (MonadPersistence)
import           ATrade.RoboCom.Types                 (BarSeriesId (BarSeriesId),
                                                       Bars, TickerInfoMap)
import           ATrade.Types                         (OrderId, OrderState,
                                                       Tick (value), Trade)
import           Colog                                (HasLog (getLogAction, setLogAction),
                                                       LogAction)
import           Control.Concurrent                   (ThreadId, forkIO,
                                                       killThread)
import           Control.Concurrent.BoundedChan       (BoundedChan,
                                                       newBoundedChan, readChan,
                                                       writeChan)
import           Control.Exception.Safe               (MonadThrow)
import           Control.Monad                        (forM, forM_, forever,
                                                       void, when)
import           Control.Monad.IO.Class               (MonadIO, liftIO)
import           Control.Monad.Reader                 (MonadReader (local),
                                                       ReaderT, asks)
import           Data.Aeson                           (FromJSON, ToJSON)
import           Data.Default                         (Default)
import           Data.IORef                           (IORef,
                                                       atomicModifyIORef',
                                                       readIORef, writeIORef)
import           Data.List.NonEmpty                   (NonEmpty)
import qualified Data.Map.Strict                      as M
import qualified Data.Text.Lazy                       as TL
import           Data.Time                            (UTCTime, getCurrentTime)
import           Dhall                                (FromDhall)
import           Prelude                              hiding (log)

data RobotDriverHandle = forall c s. (FromDhall c, Default s, FromJSON s, ToJSON s) =>
                           RobotDriverHandle StrategyInstanceDescriptor (StrategyInstance c s) ThreadId ThreadId (BoundedChan RobotDriverEvent) [SubscriptionId]

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
        QDTick tick     -> when (value tick /= 0) $ strategyEventCallback inst (NewTick tick)
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
  subIds <- forM (confTickers bigConf) (\x -> addSubscription (QuoteSubscription (tickerId x) (timeframe x)) quoteQueue)
  qthread <- liftIO . forkIO $ forever $ passQuoteEvents eventQueue quoteQueue

  driver <- liftIO . forkIO $ runner  $ robotDriverThread inst eventQueue
  return $ RobotDriverHandle instDesc inst driver qthread eventQueue subIds

  where
    passQuoteEvents eventQueue quoteQueue = do
      v <- readChan quoteQueue
      writeChan eventQueue (QuoteEvent v)

stopRobot :: (MonadIO m, QuoteStream m) => RobotDriverHandle -> m ()
stopRobot (RobotDriverHandle _ _ driver qthread _ subIds) = do
  forM_ subIds removeSubscription
  liftIO $ killThread driver
  liftIO $ killThread qthread

onStrategyInstance :: RobotDriverHandle -> forall r. (forall c s. (FromDhall c, Default s, FromJSON s, ToJSON s) => StrategyInstance c s -> r) -> r
onStrategyInstance (RobotDriverHandle _ inst _ _ _ _) f = f inst

onStrategyInstanceM :: (MonadIO m) => RobotDriverHandle ->
  (forall c s. (FromDhall c, Default s, FromJSON s, ToJSON s) => StrategyInstance c s -> m r) -> m r
onStrategyInstanceM (RobotDriverHandle _ inst _ _ _ _) f = f inst

data RobotEnv c s =
  RobotEnv
  {
    stateRef      :: IORef s,
    configRef     :: IORef c,
    timersRef     :: IORef [UTCTime],
    bars          :: IORef Bars,
    tickerInfoMap :: IORef TickerInfoMap,
    env           :: IORef StrategyEnvironment,
    logAction     :: LogAction (RobotM c s) Message,
    brokerService :: Bro.BrokerService,
    tickers       :: NonEmpty BarSeriesId
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

  getTickerInfo tid = do
    b <- asks tickerInfoMap >>= liftIO . readIORef
    return $ M.lookup tid b

  getAvailableTickers = asks tickers

postNotificationEvent :: (MonadIO m) => RobotDriverHandle -> Notification -> m ()
postNotificationEvent (RobotDriverHandle _ _ _ _ eventQueue _) notification = liftIO $
  case notification of
    OrderNotification _ oid state -> writeChan eventQueue (OrderEvent oid state)
    TradeNotification _ trade -> writeChan eventQueue (NewTradeEvent trade)

getInstanceDescriptor :: RobotDriverHandle -> StrategyInstanceDescriptor
getInstanceDescriptor (RobotDriverHandle instDesc _ _ _ _ _) = instDesc
