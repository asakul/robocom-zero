
{- |
 - Module       : ATrade.Quotes

 - Various historical price series management stuff
-}

module ATrade.Quotes
(
  MonadHistory(..)
) where

import           ATrade.Types    (Bar, BarTimeframe, TickerId)
import           Data.Time.Clock (UTCTime)

class (Monad m) => MonadHistory m where
  -- | 'getHistory tickerId timeframe fromTime toTime' should return requested timeframe between 'fromTime' and 'toTime'
  getHistory :: TickerId -> BarTimeframe -> UTCTime -> UTCTime -> m [Bar]

