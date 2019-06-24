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
import qualified Data.Map.Strict      as M
import qualified Data.Text            as T
import           Data.Time.Calendar
import           Data.Time.Clock
import           Safe

import           Hedgehog             as HH
import qualified Hedgehog.Gen         as Gen
import qualified Hedgehog.Range       as Range

import           Test.Tasty
import           Test.Tasty.Hedgehog
import           Test.Tasty.HUnit


unitTests = testGroup "BarAggregator" [
    testUnknownBarSeries
  , testOneTick
  , testTwoTicksInSameBar
  , testTwoTicksInDifferentBars
  , testOneBar
  , testTwoBarsInSameBar
  , testTwoBarsInSameBarLastBar
  , testNextBarAfterBarClose
  ]

properties = testGroup "BarAggregator" [
    prop_allTicksInOneBar,
    prop_ticksInTwoBars
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

prop_allTicksInOneBar :: TestTree
prop_allTicksInOneBar = testProperty "All ticks in one bar" $ property $ do
  tf <- forAll $ Gen.integral (Range.constant 1 86400)
  ticks <- forAll $ Gen.list (Range.linear 1 100) (genTick "TEST_TICKER" baseTime tf)
  let ticks' = sortOn timestamp ticks
  let (newbars, agg) = handleTicks ticks' (mkAggregator "TEST_TICKER" tf)
  (barHigh <$> currentBar "TEST_TICKER" agg) === Just (maximum $ value <$> ticks)
  (barLow <$> currentBar "TEST_TICKER" agg) === Just (minimum $ value <$> ticks)
  (barOpen <$> currentBar "TEST_TICKER" agg) === (value <$> headMay ticks')
  (barClose <$> currentBar "TEST_TICKER" agg) === (value <$> lastMay ticks')
  (barVolume <$> currentBar "TEST_TICKER" agg) === Just (sum $ volume <$> ticks)
  HH.assert $ null newbars

  where
    genTick :: T.Text -> UTCTime -> Integer -> Gen Tick
    genTick tickerId base tf = do
      difftime <- fromRational . toRational . picosecondsToDiffTime <$> Gen.integral (Range.linear 0 (truncate 1e12 * tf))
      val <- fromDouble <$> Gen.double (Range.exponentialFloat 0.00001 100)
      vol <- Gen.integral (Range.exponential 1 100)
      return $ Tick tickerId LastTradePrice (difftime `addUTCTime` base) val vol
    mkAggregator tickerId tf = mkAggregatorFromBars (M.singleton tickerId (BarSeries tickerId (Timeframe tf) [])) [(0, 86400)]

    currentBar tickerId agg = headMay =<< (bsBars <$> M.lookup tickerId (bars agg))
    baseTime = UTCTime (fromGregorian 1970 1 1) 0


prop_ticksInTwoBars :: TestTree
prop_ticksInTwoBars = testProperty "Ticks in one bar, then in next bar" $ property $ do
  tf <- forAll $ Gen.integral (Range.constant 1 86400)
  ticks1 <- forAll $ Gen.list (Range.linear 1 100) (genTick "TEST_TICKER" (baseTime 0) tf)
  ticks2 <- forAll $ Gen.list (Range.linear 1 100) (genTick "TEST_TICKER" (baseTime $ secondsToDiffTime tf) tf)
  let ticks1' = sortOn timestamp ticks1
  let ticks2' = sortOn timestamp ticks2
  let (_, agg) = handleTicks ticks1' (mkAggregator "TEST_TICKER" tf)
  let ([newbar], agg') = handleTicks ticks2' agg
  barSecurity newbar === "TEST_TICKER"
  (barHigh newbar) === (maximum $ value <$> ticks1)
  (barLow newbar) === (minimum $ value <$> ticks1)
  (barOpen newbar) === (value . head $ ticks1')
  (barClose newbar) === (value . last $ ticks1')
  (barVolume newbar) === (sum $ volume <$> ticks1)
  (barHigh <$> currentBar "TEST_TICKER" agg') === Just (maximum $ value <$> ticks2)
  (barLow <$> currentBar "TEST_TICKER" agg') === Just (minimum $ value <$> ticks2)
  (barOpen <$> currentBar "TEST_TICKER" agg') === (value <$> headMay ticks2')
  (barClose <$> currentBar "TEST_TICKER" agg') === (value <$> lastMay ticks2')
  (barVolume <$> currentBar "TEST_TICKER" agg') === Just (sum $ volume <$> ticks2)

  where
    genTick :: T.Text -> UTCTime -> Integer -> Gen Tick
    genTick tickerId base tf = do
      difftime <- fromRational . toRational . picosecondsToDiffTime <$> Gen.integral (Range.linear 0 (truncate 1e12 * tf))
      val <- fromDouble <$> Gen.double (Range.exponentialFloat 0.00001 100)
      vol <- Gen.integral (Range.exponential 1 100)
      return $ Tick tickerId LastTradePrice (difftime `addUTCTime` base) val vol
    mkAggregator tickerId tf = mkAggregatorFromBars (M.singleton tickerId (BarSeries tickerId (Timeframe tf) [])) [(0, 86400)]

    currentBar tickerId agg = headMay =<< (bsBars <$> M.lookup tickerId (bars agg))
    baseTime offset = UTCTime (fromGregorian 1970 1 1) offset

