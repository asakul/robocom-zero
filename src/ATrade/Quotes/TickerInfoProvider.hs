
module ATrade.Quotes.TickerInfoProvider
  (
    TickerInfoProvider(..)
  ) where

import           ATrade.RoboCom.Types (InstrumentParameters)
import           ATrade.Types         (TickerId)
newtype TickerInfoProvider =
  TickerInfoProvider
 {
   getInstrumentParameters :: [TickerId] -> IO [InstrumentParameters]
 }
