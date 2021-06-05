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
  StrategyEnvironment(..),
  seInstanceId,
  seAccount,
  seVolume,
  seBars,
  seLastTimestamp,
  EventCallback,
  Event(..),
  MonadRobot(..),
  also,
  t,
  st
) where

import           ATrade.RoboCom.Types
import           ATrade.Types

import           Control.Lens
import           Data.Aeson.Types
import qualified Data.Text                 as T
import qualified Data.Text.Lazy            as TL
import           Data.Time.Clock
import           Language.Haskell.Printf
import           Language.Haskell.TH.Quote (QuasiQuoter)

class (Monad m) => MonadRobot m c s | m -> c, m -> s where
  submitOrder :: Order -> m ()
  cancelOrder :: OrderId -> m ()
  appendToLog :: TL.Text -> m ()
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

st :: QuasiQuoter
st = t

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

data StrategyEnvironment = StrategyEnvironment {
  _seInstanceId    :: !T.Text, -- ^ Strategy instance identifier. Should be unique among all strategies (very desirable)
  _seAccount       :: !T.Text, -- ^ Account string to use for this strategy instance. Broker-dependent
  _seVolume        :: !Int, -- ^ Volume to use for this instance (in lots/contracts)
  _seBars          :: !Bars, -- ^ List of tickers which is used by this strategy
  _seLastTimestamp :: !UTCTime
} deriving (Eq)
makeLenses ''StrategyEnvironment


also :: EventCallback c s -> EventCallback c s -> EventCallback c s
also cb1 cb2 = (\event -> cb1 event >> cb2 event)

