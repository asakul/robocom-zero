{-# LANGUAGE OverloadedStrings #-}

module Test.BarAggregator
(
  unitTests,
  properties
) where

import           ATrade.BarAggregator
import           ATrade.RoboCom.Types
import           ATrade.Types
import           Data.List
import qualified Data.Map.Strict       as M
import qualified Data.Text             as T
import           Data.Time.Calendar
import           Data.Time.Clock
import           Safe

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck as QC
import           Test.Tasty.SmallCheck as SC

import           ArbitraryInstances


unitTests = testGroup "BarAggregator" [
    testUnknownBarSeries
  , testOneTick
  , testTwoTicksInSameBar
  , testTwoTicksInDifferentBars
  , testOneBar
  , testTwoBarsInSameBar
  , testTwoBarsInSameBarLastBar
  , testNextBarAfterBarClose
  , testUpdateTime
  ]

properties = testGroup "BarAggregator" [
    prop_allTicksInOneBar
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

testOneBar :: TestTree
testOneBar  = testCase "One bar" $ do
  let series = BarSeries "TEST_TICKER" (Timeframe 3600) []
  let agg = mkAggregatorFromBars (M.fromList [("TEST_TICKER", series)]) [(0, 86400)]
  let (mbar, newagg) = handleBar bar agg
  mbar @?= Nothing
  (bsBars <$> (M.lookup "TEST_TICKER" $ bars newagg)) @?= Just [Bar "TEST_TICKER" testTimestamp 12.00 18.00 10.00 12.00 68]
  where
    testTimestamp = (UTCTime (fromGregorian 1970 1 1) 60)
    bar = Bar {
            barSecurity = "TEST_TICKER",
            barTimestamp = testTimestamp,
            barOpen = fromDouble 12.00,
            barHigh = fromDouble 18.00,
            barLow = fromDouble 10.00,
            barClose = fromDouble 12.00,
            barVolume = 68 }


testTwoBarsInSameBar :: TestTree
testTwoBarsInSameBar = testCase "Two bars (smaller timeframe) - same bar" $ do
  let series = BarSeries "TEST_TICKER" (Timeframe 600) []
  let agg = mkAggregatorFromBars (M.fromList [("TEST_TICKER", series)]) [(0, 86400)]
  let (mbar, newagg) = handleBar (bar testTimestamp1 12.00 13.00 10.00 11.00 1) agg
  mbar @?= Nothing
  let (mbar', newagg') = handleBar (bar testTimestamp2 12.00 15.00 11.00 12.00 2) newagg
  mbar' @?= Nothing
  (bsBars <$> (M.lookup "TEST_TICKER" $ bars newagg')) @?= Just [Bar "TEST_TICKER" testTimestamp2 12.00 15.00 10.00 12.00 3]
  where
    testTimestamp1 = (UTCTime (fromGregorian 1970 1 1) 60)
    testTimestamp2 = (UTCTime (fromGregorian 1970 1 1) 120)
    bar ts o h l c v = Bar {
            barSecurity = "TEST_TICKER",
            barTimestamp = ts,
            barOpen = fromDouble o,
            barHigh = fromDouble h,
            barLow = fromDouble l,
            barClose = fromDouble c,
            barVolume = v }

testTwoBarsInSameBarLastBar :: TestTree
testTwoBarsInSameBarLastBar = testCase "Two bars (smaller timeframe) - same bar: last bar is exactly at the end of the bigger tf bar" $ do
  let series = BarSeries "TEST_TICKER" (Timeframe 600) []
  let agg = mkAggregatorFromBars (M.fromList [("TEST_TICKER", series)]) [(0, 86400)]
  let (mbar, newagg) = handleBar (bar testTimestamp1 12.00 13.00 10.00 11.00 1) agg
  mbar @?= Nothing
  let (mbar', newagg') = handleBar (bar testTimestamp2 12.00 15.00 11.00 12.00 2) newagg
  let expectedBar = Bar "TEST_TICKER" testTimestamp2 12.00 15.00 10.00 12.00 3
  mbar' @?= Just expectedBar
  (head . tail <$> bsBars <$> (M.lookup "TEST_TICKER" $ bars newagg')) @?= Just expectedBar
  where
    testTimestamp1 = (UTCTime (fromGregorian 1970 1 1) 560)
    testTimestamp2 = (UTCTime (fromGregorian 1970 1 1) 600)
    bar ts o h l c v = Bar {
            barSecurity = "TEST_TICKER",
            barTimestamp = ts,
            barOpen = fromDouble o,
            barHigh = fromDouble h,
            barLow = fromDouble l,
            barClose = fromDouble c,
            barVolume = v }

testNextBarAfterBarClose :: TestTree
testNextBarAfterBarClose = testCase "Three bars (smaller timeframe) - next bar after bigger tf bar close" $ do
  let series = BarSeries "TEST_TICKER" (Timeframe 600) []
  let agg = mkAggregatorFromBars (M.fromList [("TEST_TICKER", series)]) [(0, 86400)]
  let (_, newagg) = handleBar (bar testTimestamp1 12.00 13.00 10.00 11.00 1) agg
  let (_, newagg') = handleBar (bar testTimestamp2 12.00 15.00 11.00 12.00 2) newagg
  let (_, newagg'') = handleBar (bar testTimestamp3 12.00 15.00 11.00 12.00 12) newagg'
  let expectedBar = Bar "TEST_TICKER" testTimestamp3 12.00 15.00 11.00 12.00 12
  (head <$> bsBars <$> (M.lookup "TEST_TICKER" $ bars newagg'')) @?= Just expectedBar
  where
    testTimestamp1 = (UTCTime (fromGregorian 1970 1 1) 560)
    testTimestamp2 = (UTCTime (fromGregorian 1970 1 1) 600)
    testTimestamp3 = (UTCTime (fromGregorian 1970 1 1) 660)
    bar ts o h l c v = Bar {
            barSecurity = "TEST_TICKER",
            barTimestamp = ts,
            barOpen = fromDouble o,
            barHigh = fromDouble h,
            barLow = fromDouble l,
            barClose = fromDouble c,
            barVolume = v }

testUpdateTime :: TestTree
testUpdateTime = testCase "updateTime - next bar - creates new bar with zero volume" $ do
  let series = BarSeries "TEST_TICKER" (Timeframe 3600) []
  let agg = mkAggregatorFromBars (M.fromList [("TEST_TICKER", series)]) [(0, 86400)]
  let (_, newagg) = handleBar (bar testTimestamp1 12.00 13.00 10.00 11.00 1) agg
  let (_, newagg') = handleBar (bar testTimestamp2 12.00 15.00 11.00 12.00 2) newagg
  let (newBar, newagg'') = updateTime (tick testTimestamp4 13.00 100) newagg'
  let expectedNewBar = Bar "TEST_TICKER" testTimestamp2 12.00 15.00 10.00 12.00 3
  let expectedBar = Bar "TEST_TICKER" testTimestamp4 13.00 13.00 13.00 13.00 0
  (head <$> bsBars <$> (M.lookup "TEST_TICKER" $ bars newagg'')) @?= Just expectedBar
  newBar @?= Just expectedNewBar
  where
    testTimestamp1 = (UTCTime (fromGregorian 1970 1 1) 560)
    testTimestamp2 = (UTCTime (fromGregorian 1970 1 1) 600)
    testTimestamp3 = (UTCTime (fromGregorian 1970 1 1) 3600)
    testTimestamp4 = (UTCTime (fromGregorian 1970 1 1) 3660)
    tick ts v vol = Tick {
              security = "TEST_TICKER"
            , datatype = LastTradePrice
            , timestamp = ts
            , value = v
            , volume = vol }
    bar ts o h l c v = Bar {
            barSecurity = "TEST_TICKER",
            barTimestamp = ts,
            barOpen = fromDouble o,
            barHigh = fromDouble h,
            barLow = fromDouble l,
            barClose = fromDouble c,
            barVolume = v }

prop_allTicksInOneBar :: TestTree
prop_allTicksInOneBar = QC.testProperty "All ticks in one bar" $ QC.forAll (QC.choose (1, 86400)) $ \timeframe ->
  QC.forAll (QC.listOf1 (genTick "TEST_TICKER" baseTime timeframe)) $ \ticks ->
        let ticks' = sortOn timestamp ticks in
          let (newbars, agg) = handleTicks ticks' (mkAggregator "TEST_TICKER" timeframe) in
            null newbars &&
            ((barHigh <$> currentBar "TEST_TICKER" agg) == Just (maximum $ value <$> ticks)) &&
            ((barLow <$> currentBar "TEST_TICKER" agg) == Just (minimum $ value <$> ticks)) &&
            ((barOpen <$> currentBar "TEST_TICKER" agg) == (value <$> headMay ticks')) &&
            ((barClose <$> currentBar "TEST_TICKER" agg) == (value <$> lastMay ticks')) &&
            ((barVolume <$> currentBar "TEST_TICKER" agg) == Just (sum $ volume <$> ticks))
  where
    genTick :: T.Text -> UTCTime -> Integer -> Gen Tick
    genTick tickerId base tf = do
      difftime <- fromRational . toRational . picosecondsToDiffTime <$> choose (0, truncate 1e12 * tf)
      val <- arbitrary
      vol <- arbitrary `suchThat` (> 0)
      return $ Tick tickerId LastTradePrice (difftime `addUTCTime` baseTime) val vol
    mkAggregator tickerId tf = mkAggregatorFromBars (M.singleton tickerId (BarSeries tickerId (Timeframe tf) [])) [(0, 86400)]

    currentBar tickerId agg = headMay =<< (bsBars <$> M.lookup tickerId (bars agg))
    baseTime = UTCTime (fromGregorian 1970 1 1) 0

