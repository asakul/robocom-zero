{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE DuplicateRecordFields     #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes                #-}

module ATrade.Driver.Junction.Types
  (
    StrategyDescriptor(..),
    TickerConfig(..),
    StrategyInstanceDescriptor(..),
    StrategyInstance(..),
    BigConfig(..)
  ,StrategyDescriptorE(..)) where

import           ATrade.RoboCom.Monad (EventCallback)
import           ATrade.Types         (BarTimeframe (..), TickerId)
import           Data.Aeson           (FromJSON (..), ToJSON (..))
import           Data.Default         (Default)
import           Data.IORef           (IORef)
import qualified Data.Text            as T
import           Dhall                (FromDhall)
import           GHC.Generics         (Generic)

data StrategyDescriptor c s =
    StrategyDescriptor
    {
      baseStrategyName :: T.Text,
      eventCallback    :: EventCallback c s,
      defaultState     :: s
    }

data StrategyDescriptorE = forall c s. (FromDhall c, Default s, FromJSON s, ToJSON s) => StrategyDescriptorE (StrategyDescriptor c s)

data TickerConfig =
  TickerConfig
  {
    tickerId  :: TickerId,
    timeframe :: BarTimeframe
  }
  deriving (Generic)

instance FromDhall BarTimeframe
instance FromDhall TickerConfig

data BigConfig c = BigConfig {
  confTickers  :: [TickerConfig],
  confStrategy :: c
} deriving (Generic)

instance (FromDhall c) => FromDhall (BigConfig c)

data StrategyInstanceDescriptor =
  StrategyInstanceDescriptor
  {
    strategyId       :: T.Text,
    strategyBaseName :: T.Text,
    configKey        :: T.Text,
    stateKey         :: T.Text,
    logPath          :: T.Text
  } deriving (Generic, Show)

instance FromDhall StrategyInstanceDescriptor

data StrategyInstance c s =
    StrategyInstance
    {
      strategyInstanceId    :: T.Text,
      strategyEventCallback :: EventCallback c s,
      strategyState         :: IORef s,
      strategyConfig        :: IORef c
    }
