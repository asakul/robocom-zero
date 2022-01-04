{-# LANGUAGE DeriveGeneric #-}

module ATrade.Driver.Junction.QuoteStream
  (
    QuoteSubscription(..),
    QuoteStream(..),
    SubscriptionId(..)
  ) where

import           ATrade.QuoteSource.Client      (QuoteData)
import           ATrade.Types                   (BarTimeframe, TickerId)
import           Control.Concurrent.BoundedChan (BoundedChan)
import           Data.Hashable                  (Hashable)
import           GHC.Generics                   (Generic)

data QuoteSubscription =
  QuoteSubscription TickerId BarTimeframe
  deriving (Generic, Eq)

instance Hashable BarTimeframe
instance Hashable QuoteSubscription

newtype SubscriptionId = SubscriptionId { unSubscriptionId :: Int }
  deriving (Show, Eq, Generic)

instance Hashable SubscriptionId

class (Monad m) => QuoteStream m where
  addSubscription :: QuoteSubscription -> BoundedChan QuoteData -> m SubscriptionId
  removeSubscription :: SubscriptionId -> m ()
