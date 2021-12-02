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
  seLastTimestamp,
  EventCallback,
  Event(..),
  MonadRobot(..),
  also,
  t,
  st,
  getFirstTickerId) where

import           ATrade.RoboCom.Types
import           ATrade.Types

import           Control.Lens
import           Data.Aeson.Types
import qualified Data.Text                 as T
import qualified Data.Text.Lazy            as TL
import           Data.Time.Clock
import           Language.Haskell.Printf
import           Language.Haskell.TH.Quote (QuasiQuoter)
import ATrade.Logging (Severity)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE

class (Monad m) => MonadRobot m c s | m -> c, m -> s where
  submitOrder :: Order -> m OrderId
  cancelOrder :: OrderId -> m ()
  appendToLog :: Severity -> TL.Text -> m ()
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
  getTicker :: TickerId -> BarTimeframe -> m (Maybe BarSeries)
  getAvailableTickers :: m (NonEmpty BarSeriesId)

getFirstTickerId :: forall c s m. (Monad m, MonadRobot m c s) => m BarSeriesId
getFirstTickerId = NE.head <$> getAvailableTickers

st :: QuasiQuoter
st = t

type EventCallback c s = forall m . MonadRobot m c s => Event -> m ()

data Event = NewBar (BarTimeframe, Bar)
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
  _seLastTimestamp :: !UTCTime
} deriving (Eq)
makeLenses ''StrategyEnvironment


also :: EventCallback c s -> EventCallback c s -> EventCallback c s
also cb1 cb2 = (\event -> cb1 event >> cb2 event)

