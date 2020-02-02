{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE TypeApplications       #-}
{-# LANGUAGE TypeSynonymInstances   #-}

module ATrade.RoboCom.Monad (
  RState,
  RConfig,
  RActions,
  REnv,
  StrategyEnvironment(..),
  seInstanceId,
  seAccount,
  seVolume,
  seBars,
  seLastTimestamp,
  StrategyElement,
  runStrategyElement,
  EventCallback,
  Event(..),
  StrategyMonad,
  StrategyAction(..),
  tellAction,
  MonadRobot(..),
  also,
  st
) where

import           ATrade.RoboCom.Types
import           ATrade.Types

import           Ether

import           Control.Lens
import           Data.Aeson.Types
import qualified Data.Text            as T
import           Data.Time.Clock
import           Text.Printf.TH

class (Monad m) => MonadRobot m c s | m -> c, m -> s where
  submitOrder :: Order -> m ()
  cancelOrder :: OrderId -> m ()
  appendToLog :: T.Text -> m ()
  setupTimer :: UTCTime -> m ()
  enqueueIOAction :: Int -> IO Value -> m ()
  getConfig :: m c
  getState :: m s
  setState :: s -> m ()
  modifyState :: (s -> s) -> m ()
  modifyState f = do
    oldState <- getState
    setState (f oldState)
  getEnvironment :: m StrategyEnvironment

data RState
data RConfig
data RActions
data REnv

type StrategyMonad c s = WriterT RActions [StrategyAction] (StateT RState s (ReaderT REnv StrategyEnvironment (Reader RConfig c)))
type StrategyElement c s r = (StrategyMonad c s) r

runStrategyElement :: c -> s -> StrategyEnvironment -> StrategyElement c s r -> (s, [StrategyAction], r)
runStrategyElement conf sta env action = (newState, actions, retValue)
  where
    ((retValue, actions), newState) = runReader @RConfig (runReaderT @REnv (runStateT @RState (runWriterT @RActions action) sta) env) conf

type EventCallback c s = forall m . MonadRobot m c s => Event -> m ()

data Event = NewBar Bar
  | NewTick Tick
  | OrderSubmitted Order
  | OrderUpdate OrderId OrderState
  | NewTrade Trade
  | TimerFired UTCTime
  | Shutdown
  | ActionCompleted Int Value
    deriving (Show, Eq)

data StrategyAction = ActionOrder Order
  | ActionCancelOrder OrderId
  | ActionLog T.Text
  | ActionSetupTimer UTCTime
  | ActionIO Int (IO Value)

data StrategyEnvironment = StrategyEnvironment {
  _seInstanceId    :: !T.Text, -- ^ Strategy instance identifier. Should be unique among all strategies (very desirable)
  _seAccount       :: !T.Text, -- ^ Account string to use for this strategy instance. Broker-dependent
  _seVolume        :: !Int, -- ^ Volume to use for this instance (in lots/contracts)
  _seBars          :: !Bars, -- ^ List of tickers which is used by this strategy
  _seLastTimestamp :: !UTCTime
} deriving (Eq)
makeLenses ''StrategyEnvironment


instance Show StrategyAction where
  show (ActionOrder order)     = "ActionOrder " ++ show order
  show (ActionCancelOrder oid) = "ActionCancelOrder " ++ show oid
  show (ActionLog t)           = "ActionLog " ++ show t
  show (ActionIO x _)          = "ActionIO " ++ show x
  show (ActionSetupTimer t)    = "ActionSetupTimer e" ++ show t

tellAction :: StrategyAction -> StrategyElement c s ()
tellAction a = tell @RActions [a]

instance MonadRobot (StrategyMonad c s) c s where
  submitOrder order = tellAction $ ActionOrder order
  cancelOrder oId = tellAction $ ActionCancelOrder oId
  appendToLog = tellAction . ActionLog
  setupTimer = tellAction . ActionSetupTimer
  enqueueIOAction actionId action = tellAction $ ActionIO actionId action
  getConfig = ask @RConfig
  getState = get @RState
  setState = put @RState
  getEnvironment = ask @REnv

also :: EventCallback c s -> EventCallback c s -> EventCallback c s
also cb1 cb2 = (\event -> cb1 event >> cb2 event)

