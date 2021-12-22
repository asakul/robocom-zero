
module ATrade.Quotes.TickerInfoProvider
  (
    TickerInfoProvider(..)
  ) where

import           ATrade.RoboCom.Types (InstrumentParameters)
import           ATrade.Types         (TickerId)

class (Monad m) => TickerInfoProvider m where
  getInstrumentParameters :: [TickerId] -> m [InstrumentParameters]

