
module Test.Mock.HistoryProvider
(
  mkMockHistoryProvider
) where

import           ATrade.Quotes.HistoryProvider
import           ATrade.RoboCom.Types          (BarSeriesId (BarSeriesId), Bars)
import           ATrade.Types                  (Bar (Bar, barTimestamp),
                                                BarTimeframe (BarTimeframe),
                                                TickerId)
import qualified Data.Map.Strict               as M
import           Data.Time                     (UTCTime)

mkMockHistoryProvider :: M.Map BarSeriesId [Bar] -> HistoryProvider
mkMockHistoryProvider bars = HistoryProvider $ mockGetHistory bars

mockGetHistory :: M.Map BarSeriesId [Bar] -> TickerId -> BarTimeframe -> UTCTime -> UTCTime -> IO [Bar]
mockGetHistory bars tid tf from to =
  case M.lookup (BarSeriesId tid tf) bars of
    Just series -> return $ filter (\bar -> (barTimestamp bar >= from) && (barTimestamp bar <= to)) series
    Nothing     -> return []



