
module ATrade.Quotes.HistoryProvider
  (
    HistoryProvider(..)
  ) where

import           ATrade.RoboCom.Types (Bar)
import           ATrade.Types         (BarTimeframe, TickerId)
import           Data.Time            (UTCTime)
newtype HistoryProvider =
  HistoryProvider
 {
   getHistory :: TickerId -> BarTimeframe -> UTCTime -> UTCTime -> IO [Bar]
 }
