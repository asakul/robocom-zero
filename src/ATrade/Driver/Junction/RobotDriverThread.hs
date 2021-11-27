{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes                #-}

module ATrade.Driver.Junction.RobotDriverThread
  (
  createRobotDriverThread
  ) where

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
                                                     EventCallback, MonadRobot)
import           ATrade.RoboCom.Persistence         (MonadPersistence (loadState))
import           ATrade.Types                       (OrderId, OrderState, Trade)
import           Control.Concurrent                 (ThreadId, forkIO)
import           Control.Concurrent.BoundedChan     (BoundedChan,
                                                     newBoundedChan, readChan,
                                                     writeChan)
import           Control.Monad                      (forM_, forever)
import           Control.Monad.IO.Class             (MonadIO, liftIO)
import           Data.Aeson                         (FromJSON, ToJSON)
import           Data.IORef                         (IORef, newIORef)
import           Dhall                              (FromDhall)

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
