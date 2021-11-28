{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RankNTypes                 #-}

module ATrade.Driver.Junction.RobotDriverThread
  (
  createRobotDriverThread,
  RobotEnv(..),
  RobotM(..)
  ) where

import           ATrade.Broker.Client               (BrokerClientHandle (submitOrder))
import qualified ATrade.Broker.Client               as Bro
import           ATrade.Driver.Junction.QuoteStream (QuoteStream (addSubscription),
                                                     QuoteSubscription (QuoteSubscription))
import           ATrade.Driver.Junction.Types       (BigConfig,
                                                     StrategyDescriptor,
                                                     StrategyInstance (StrategyInstance, strategyEventCallback),
                                                     StrategyInstanceDescriptor (configKey),
                                                     confStrategy, confTickers,
                                                     eventCallback, stateKey,
                                                     strategyId, tickerId,
                                                     timeframe)
import           ATrade.QuoteSource.Client          (QuoteData (..))
import           ATrade.RoboCom.ConfigStorage       (ConfigStorage (loadConfig))
import           ATrade.RoboCom.Monad               (Event (NewBar, NewTick, NewTrade, OrderUpdate),
                                                     MonadRobot (..))
import           ATrade.RoboCom.Persistence         (MonadPersistence (loadState))
import           ATrade.RoboCom.Types               (BarSeriesId (BarSeriesId),
                                                     Bars)
import           ATrade.Types                       (OrderId, OrderState, Trade)
import           Control.Concurrent                 (ThreadId, forkIO)
import           Control.Concurrent.BoundedChan     (BoundedChan,
                                                     newBoundedChan, readChan,
                                                     writeChan)
import           Control.Exception.Safe             (MonadThrow)
import           Control.Monad                      (forM_, forever, void)
import           Control.Monad.IO.Class             (MonadIO, liftIO)
import           Control.Monad.Reader               (MonadReader, ReaderT, asks)
import           Data.Aeson                         (FromJSON, ToJSON)
import           Data.IORef                         (IORef, atomicModifyIORef',
                                                     readIORef, writeIORef)
import qualified Data.Map.Strict                    as M
import qualified Data.Text.Lazy                     as TL
import           Data.Time                          (UTCTime)
import           Dhall                              (FromDhall)
import           System.Log.Logger                  (infoM)

data RobotDriverHandle = forall c s. RobotDriverHandle (StrategyInstance c s) ThreadId ThreadId (BoundedChan RobotDriverEvent)

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
                            FromJSON s,
                            ToJSON s,
                            FromDhall c,
                            MonadIO m,
                            MonadRobot m c s) =>
     StrategyInstanceDescriptor
  -> StrategyDescriptor c s
  -> (m () -> IO ())
  -> BigConfig c
  -> IORef c
  -> IORef s
  -> m1 RobotDriverHandle

createRobotDriverThread instDesc strDesc runner bigConf rConf rState = do
  eventQueue <- liftIO $ newBoundedChan 2000

  let inst = StrategyInstance (strategyId instDesc) (eventCallback strDesc) rState rConf

  quoteQueue <- liftIO $ newBoundedChan 2000
  forM_ (confTickers bigConf) (\x -> addSubscription (QuoteSubscription (tickerId x) (timeframe x)) quoteQueue)
  qthread <- liftIO . forkIO $ forever $ passQuoteEvents eventQueue quoteQueue

  driver <- liftIO . forkIO $ runner $ robotDriverThread inst eventQueue
  return $ RobotDriverHandle inst driver qthread eventQueue

  where
    passQuoteEvents eventQueue quoteQueue = do
      v <- readChan quoteQueue
      writeChan eventQueue (QuoteEvent v)

data RobotEnv c s =
  RobotEnv
  {
    stateRef  :: IORef s,
    configRef :: IORef c,
    timersRef :: IORef [UTCTime],
    broker    :: BrokerClientHandle,
    bars      :: IORef Bars
  }

newtype RobotM c s a = RobotM { unRobotM :: ReaderT (RobotEnv c s) IO a }
  deriving (Functor, Applicative, Monad, MonadReader (RobotEnv c s), MonadIO, MonadThrow)

instance MonadRobot (RobotM c s) c s where
  submitOrder order = do
    bro <- asks broker
    liftIO $ void $ Bro.submitOrder bro order

  cancelOrder oid = do
    bro <- asks broker
    liftIO $ void $ Bro.cancelOrder bro oid

  appendToLog = liftIO . infoM "Robot" . TL.unpack

  setupTimer t = do
    ref <- asks timersRef
    liftIO $ atomicModifyIORef' ref (\s -> (t : s, ()))

  enqueueIOAction = undefined
  getConfig = asks configRef >>= liftIO . readIORef
  getState = asks stateRef >>= liftIO . readIORef
  setState newState = asks stateRef >>= liftIO . flip writeIORef newState
  getEnvironment = undefined
  getTicker tid tf = do
    b <- asks bars >>= liftIO . readIORef
    return $ M.lookup (BarSeriesId tid tf) b
