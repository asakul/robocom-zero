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
  ]

properties = testGroup "BarAggregator" [
    prop_allTicksInOneBar,
    prop_ticksInTwoBars
  ]

secParams = InstrumentParameters 1 0.01

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
  let series = BarSeries "TEST_TICKER" (BarTimeframe 60) [] secParams
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
  let series = BarSeries "TEST_TICKER" (BarTimeframe 60) [] secParams
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
  let series = BarSeries "TEST_TICKER" (BarTimeframe 60) [] secParams
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
    genTick :: T.Text -> UTCTime -> Int -> Gen Tick
    genTick tickerId base tf = do
      difftime <- fromRational . toRational . picosecondsToDiffTime <$> choose (0, truncate 1e12 * fromIntegral tf)
      val <- arbitrary
      vol <- arbitrary `suchThat` (> 0)
      return $ Tick tickerId LastTradePrice (difftime `addUTCTime` baseTime) val vol
    mkAggregator tickerId tf = mkAggregatorFromBars (M.singleton tickerId (BarSeries tickerId (BarTimeframe tf) [] secParams)) [(0, 86400)]

    currentBar tickerId agg = headMay =<< (bsBars <$> M.lookup tickerId (bars agg))
    baseTime = UTCTime (fromGregorian 1970 1 1) 0

