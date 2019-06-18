{-# LANGUAGE OverloadedStrings #-}

module Test.BarAggregator
(
  unitTests
) where

import           ATrade.BarAggregator
import           ATrade.RoboCom.Types
import           ATrade.Types
import qualified Data.Map.Strict       as M
import qualified Data.Text             as T
import           Data.Time.Calendar
import           Data.Time.Clock

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck as QC
import           Test.Tasty.SmallCheck as SC

unitTests = testGroup "BarAggregator" [
    testUnknownBarSeries
  , testOneTick
  , testTwoTicksInSameBar
  , testTwoTicksInDifferentBars
  ]

testUnknownBarSeries :: TestTree
testUnknownBarSeries = testCase "Tick with unknown ticker id" $ do
  let agg = BarAggregator M.empty M.empty [(0, 86400)]
  let (mbar, newagg) = handleTick tick agg
  mbar @?= Nothing
  (bars newagg) @?= M.empty
  where
    testTimestamp = (UTCTime (fromGregorian 1970 1 1) 100)
    tick = Tick {
            security = "TEST_TICKER",
            datatype = LastTradePrice,
            timestamp = testTimestamp,
            value = fromDouble 12.00,
            volume = 1 }

testOneTick :: TestTree
testOneTick = testCase "One tick" $ do
  let series = BarSeries "TEST_TICKER" (Timeframe 60) []
  let agg = mkAggregatorFromBars (M.fromList [("TEST_TICKER", series)]) [(0, 86400)]
  let (mbar, newagg) = handleTick tick agg
  mbar @?= Nothing
  (bsBars <$> (M.lookup "TEST_TICKER" $ bars newagg)) @?= Just [Bar "TEST_TICKER" testTimestamp 12.00 12.00 12.00 12.00 1]
  where
    testTimestamp = (UTCTime (fromGregorian 1970 1 1) 60)
    tick = Tick {
            security = "TEST_TICKER",
            datatype = LastTradePrice,
            timestamp = testTimestamp,
            value = fromDouble 12.00,
            volume = 1 }

testTwoTicksInSameBar :: TestTree
testTwoTicksInSameBar = testCase "Two ticks - same bar" $ do
  let series = BarSeries "TEST_TICKER" (Timeframe 60) []
  let agg = mkAggregatorFromBars (M.fromList [("TEST_TICKER", series)]) [(0, 86400)]
  let (mbar, newagg) = handleTick (tick testTimestamp1 12.00) agg
  mbar @?= Nothing
  let (mbar', newagg') = handleTick (tick testTimestamp2 14.00) newagg
  mbar' @?= Nothing
  (bsBars <$> (M.lookup "TEST_TICKER" $ bars newagg')) @?= Just [Bar "TEST_TICKER" testTimestamp2 12.00 14.00 12.00 14.00 2]
  where
    testTimestamp1 = (UTCTime (fromGregorian 1970 1 1) 58)
    testTimestamp2 = (UTCTime (fromGregorian 1970 1 1) 59)
    tick ts val = Tick {
            security = "TEST_TICKER",
            datatype = LastTradePrice,
            timestamp = ts,
            value = fromDouble val,
            volume = 1 }

testTwoTicksInDifferentBars :: TestTree
testTwoTicksInDifferentBars = testCase "Two ticks - different bar" $ do
  let series = BarSeries "TEST_TICKER" (Timeframe 60) []
  let agg = mkAggregatorFromBars (M.fromList [("TEST_TICKER", series)]) [(0, 86400)]
  let (mbar, newagg) = handleTick (tick testTimestamp1 12.00) agg
  mbar @?= Nothing
  let (mbar', newagg') = handleTick (tick testTimestamp2 14.00) newagg
  mbar' @?= Just (Bar "TEST_TICKER" testTimestamp1 12.00 12.00 12.00 12.00 1)
  (bsBars <$> (M.lookup "TEST_TICKER" $ bars newagg')) @?= Just [Bar "TEST_TICKER" testTimestamp2 14.00 14.00 14.00 14.00 1, Bar "TEST_TICKER" testTimestamp1 12.00 12.00 12.00 12.00 1]
  where
    testTimestamp1 = (UTCTime (fromGregorian 1970 1 1) 58)
    testTimestamp2 = (UTCTime (fromGregorian 1970 1 1) 61)
    tick ts val = Tick {
            security = "TEST_TICKER",
            datatype = LastTradePrice,
            timestamp = ts,
            value = fromDouble val,
            volume = 1 }
