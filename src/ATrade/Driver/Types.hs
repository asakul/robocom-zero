{-# LANGUAGE RankNTypes #-}

module ATrade.Driver.Types
(
  StrategyInstanceParams(..),
  InitializationCallback
) where

import           ATrade.RoboCom.Types

import qualified Data.Text            as T

-- | Strategy instance params store few params which are common for all strategies
data StrategyInstanceParams = StrategyInstanceParams {
  strategyInstanceId :: T.Text, -- ^ Strategy instance identifier. Should be unique among all strategies (very desirable)
  strategyAccount    :: T.Text, -- ^ Account string to use for this strategy instance. Broker-dependent
  strategyVolume     :: Int, -- ^ Volume to use for this instance (in lots/contracts)
  tickers            :: [Ticker], -- ^ List of tickers which is used by this strategy
  strategyQTISEp     :: Maybe T.Text
}

type InitializationCallback c = c -> StrategyInstanceParams -> IO c
