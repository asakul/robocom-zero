
module Test.Mock.TickerInfoProvider
(
  MockTickerInfoProvider,
  mkMockTickerInfoProvider,
  mockGetInstrumentParameters
) where

import           ATrade.Quotes.TickerInfoProvider
import           ATrade.RoboCom.Types             (InstrumentParameters)
import           ATrade.Types                     (TickerId)
import           Control.Monad.IO.Class           (MonadIO)
import qualified Data.Map.Strict                  as M
import           Data.Maybe                       (catMaybes, mapMaybe)

data MockTickerInfoProvider = MockTickerInfoProvider (M.Map TickerId InstrumentParameters)

mkMockTickerInfoProvider :: (M.Map TickerId InstrumentParameters) -> MockTickerInfoProvider
mkMockTickerInfoProvider = MockTickerInfoProvider

mockGetInstrumentParameters :: MockTickerInfoProvider -> [TickerId] -> IO [InstrumentParameters]
mockGetInstrumentParameters (MockTickerInfoProvider params) = return . mapMaybe (`M.lookup` params)
