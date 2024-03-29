
module ATrade.Quotes.HistoryProvider
  (
    HistoryProvider(..)
  ) where

import           ATrade.RoboCom.Types (Bar)
import           ATrade.Types         (BarTimeframe, TickerId)
import           Data.Time            (UTCTime)

class (Monad m) => HistoryProvider m where
  getHistory :: TickerId -> BarTimeframe -> UTCTime -> UTCTime -> m [Bar]
