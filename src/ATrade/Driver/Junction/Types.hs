{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes                #-}

module ATrade.Driver.Junction.Types
  (
    StrategyDescriptor(..),
    TickerConfig(..),
    StrategyInstanceDescriptor(..),
    StrategyInstance(..)
  ) where

import           ATrade.RoboCom.Monad (EventCallback)
import           ATrade.Types         (BarTimeframe, TickerId)
import           Data.Aeson           (FromJSON (..), ToJSON (..))
import qualified Data.ByteString      as B
import           Data.IORef
import qualified Data.Text            as T

data StrategyDescriptor =
  forall c s. (FromJSON s, ToJSON s, FromJSON c) =>
    StrategyDescriptor
    {
      baseStrategyName :: T.Text,
      eventCallback    :: EventCallback c s,
      defaultState     :: s
    }

data TickerConfig =
  TickerConfig
  {
    tickerId  :: TickerId,
    timeframe :: BarTimeframe
  }

data StrategyInstanceDescriptor =
  StrategyInstanceDescriptor
  {
    strategyId   :: T.Text,
    strategyName :: T.Text,
    configKey    :: T.Text,
    stateKey     :: T.Text,
    logPath      :: T.Text,
    tickers      :: [TickerConfig]
  }

data StrategyInstance =
  forall c s. (FromJSON s, ToJSON s, FromJSON c) =>
    StrategyInstance
    {
      strategyInstanceId    :: T.Text,
      strategyEventCallback :: EventCallback c s,
      strategyState         :: IORef s,
      strategyConfig        :: IORef c
    }
