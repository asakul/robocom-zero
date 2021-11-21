
module Test.Mock.HistoryProvider
(
  MockHistoryProvider,
  mkMockHistoryProvider,
  mockGetHistory
) where

import           ATrade.Quotes.HistoryProvider
import           ATrade.RoboCom.Types          (BarSeriesId (BarSeriesId), Bars)
import           ATrade.Types                  (Bar (Bar, barTimestamp),
                                                BarTimeframe (BarTimeframe),
                                                TickerId)
import           Control.Monad.IO.Class        (MonadIO)
import qualified Data.Map.Strict               as M
import           Data.Time                     (UTCTime)

data MockHistoryProvider = MockHistoryProvider (M.Map BarSeriesId [Bar])

mkMockHistoryProvider :: M.Map BarSeriesId [Bar] -> MockHistoryProvider
mkMockHistoryProvider = MockHistoryProvider

mockGetHistory :: (MonadIO m) => MockHistoryProvider -> TickerId -> BarTimeframe -> UTCTime -> UTCTime -> m [Bar]
mockGetHistory (MockHistoryProvider bars) tid tf from to =
  case M.lookup (BarSeriesId tid tf) bars of
    Just series -> return $ filter (\bar -> (barTimestamp bar >= from) && (barTimestamp bar <= to)) series
    Nothing     -> return []
