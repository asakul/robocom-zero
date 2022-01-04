{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE DuplicateRecordFields     #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}

module ATrade.Driver.Junction.Types
  (
    StrategyDescriptor(..),
    TickerConfig(..),
    StrategyInstanceDescriptor(..),
    StrategyInstance(..),
    BigConfig(..),
    StrategyDescriptorE(..),
    StrategyInstanceE(..)
  ) where

import           ATrade.RoboCom.Monad (EventCallback)
import           ATrade.Types         (BarTimeframe (..), TickerId)
import           Data.Aeson           (FromJSON (..), ToJSON (..), withObject,
                                       (.:))
import           Data.Default         (Default)
import           Data.IORef           (IORef)
import qualified Data.Text            as T
import           Data.Time            (UTCTime)
import           Dhall                (FromDhall, autoWith, natural)
import           GHC.Generics         (Generic)

data StrategyDescriptor c s =
    StrategyDescriptor
    {
      baseStrategyName :: T.Text,
      eventCallback    :: EventCallback c s
    }

data StrategyDescriptorE = forall c s. (FromDhall c, Default s, FromJSON s, ToJSON s) => StrategyDescriptorE (StrategyDescriptor c s)

data TickerConfig =
  TickerConfig
  {
    tickerId  :: TickerId,
    timeframe :: BarTimeframe
  }
  deriving (Generic)

instance FromDhall BarTimeframe where
  autoWith _ = BarTimeframe . fromIntegral <$> natural

instance FromDhall TickerConfig

data BigConfig c = BigConfig {
  confTickers  :: [TickerConfig],
  confStrategy :: c
} deriving (Generic)

instance (FromDhall c) => FromDhall (BigConfig c)

data StrategyInstanceDescriptor =
  StrategyInstanceDescriptor
  {
    accountId        :: T.Text,
    strategyId       :: T.Text,
    strategyBaseName :: T.Text,
    configKey        :: T.Text,
    stateKey         :: T.Text,
    logPath          :: T.Text
  } deriving (Generic, Show)

instance FromDhall StrategyInstanceDescriptor

instance FromJSON StrategyInstanceDescriptor where
  parseJSON = withObject "StrategyInstanceDescriptor" $ \obj ->
    StrategyInstanceDescriptor <$>
    obj .: "account_id" <*>
    obj .: "strategy_id" <*>
    obj .: "strategy_base_name" <*>
    obj .: "config_key" <*>
    obj .: "state_key" <*>
    obj .: "log_path"


data StrategyInstance c s =
    StrategyInstance
    {
      strategyInstanceId    :: T.Text,
      strategyEventCallback :: EventCallback c s,
      strategyState         :: IORef s,
      strategyConfig        :: IORef c,
      strategyTimers        :: IORef [UTCTime]
    }

data StrategyInstanceE = forall c s. (FromDhall c, Default s, FromJSON s, ToJSON s) => StrategyInstanceE (StrategyInstance c s)
