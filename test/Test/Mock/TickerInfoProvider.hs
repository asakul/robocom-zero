
module Test.Mock.TickerInfoProvider
(
  mkMockTickerInfoProvider
) where

import           ATrade.Quotes.TickerInfoProvider
import           ATrade.RoboCom.Types             (InstrumentParameters)
import           ATrade.Types                     (TickerId)
import qualified Data.Map.Strict                  as M
import           Data.Maybe                       (catMaybes, mapMaybe)

mkMockTickerInfoProvider :: M.Map TickerId InstrumentParameters -> TickerInfoProvider
mkMockTickerInfoProvider params = TickerInfoProvider $ mockGetInstrumentParameters params

mockGetInstrumentParameters :: M.Map TickerId InstrumentParameters -> [TickerId] -> IO [InstrumentParameters]
mockGetInstrumentParameters params = return . mapMaybe (`M.lookup` params)
