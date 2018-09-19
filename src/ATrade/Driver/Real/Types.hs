{-# LANGUAGE RankNTypes #-}

module ATrade.Driver.Real.Types (
  Strategy(..),
  StrategyInstanceParams(..),
  InitializationCallback
) where

import ATrade.RoboCom.Monad
import ATrade.RoboCom.Types

import Data.Time.Clock
import qualified Data.Text as T

-- | Top-level strategy configuration and state
data Strategy c s = BarStrategy {
  downloadDelta :: DiffTime, -- ^ How much history to download at strategy start
  eventCallback :: EventCallback c s, -- ^ Strategy event callback
  currentState :: s, -- ^ Current strategy state. Updated after each 'EventCallback' call
  strategyParams :: c, -- ^ Strategy params
  strategyTimers :: [UTCTime],

  strategyInstanceParams :: StrategyInstanceParams -- ^ Instance params
}

-- | Strategy instance params store few params which are common for all strategies
data StrategyInstanceParams = StrategyInstanceParams {
  strategyInstanceId :: T.Text, -- ^ Strategy instance identifier. Should be unique among all strategies (very desirable)
  strategyAccount :: T.Text, -- ^ Account string to use for this strategy instance. Broker-dependent
  strategyVolume :: Int, -- ^ Volume to use for this instance (in lots/contracts)
  tickers :: [Ticker], -- ^ List of tickers which is used by this strategy
  strategyQuotesourceEp :: T.Text, -- ^ QuoteSource server endpoint
  strategyBrokerEp :: T.Text, -- ^ Broker server endpoint
  strategyHistoryProviderType :: T.Text,
  strategyHistoryProvider :: T.Text,
  strategyQTISEp :: Maybe T.Text
}

type InitializationCallback c = c -> StrategyInstanceParams -> IO c
