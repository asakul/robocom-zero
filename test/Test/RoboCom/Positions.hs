
{-# LANGUAGE OverloadedStrings #-}

module Test.RoboCom.Positions
(
  unitTests
) where

import           Test.Tasty
import           Test.Tasty.HUnit

import           ATrade.Types
import qualified Data.List                as L
import qualified Data.Map.Strict          as M
import qualified Data.Text                as T
import           Data.Time.Calendar
import           Data.Time.Clock

import           ATrade.RoboCom.Monad
import           ATrade.RoboCom.Positions
import           ATrade.RoboCom.Types

data TestState = TestState
  {
    positions :: [Position],
    testInt   :: Int
  }

defaultState = TestState {
  positions = [],
  testInt = 0
  }

data TestConfig = TestConfig

instance ParamsHasMainTicker TestConfig where
  mainTicker _ = "TEST_TICKER"

instance StateHasPositions TestState where
  getPositions = positions
  setPositions a p = a { positions = p }

defaultStrategyEnvironment = StrategyEnvironment
  {
    seInstanceId = "test_instance",
    seAccount = "test_account",
    seVolume = 1,
    seBars = M.empty,
    seLastTimestamp = (UTCTime (fromGregorian 1970 1 1) 0)
  }

unitTests = testGroup "RoboCom.Positions" [
  testEnterAtMarket,
  testEnterAtMarketSendsAction,
  testDefaultHandlerSubmissionDeadline,
  testDefaultHandlerAfterSubmissionPositionIsWaitingOpen,
  testDefaultHandlerPositionWaitingOpenOrderOpenExecuted1
  ]

testEnterAtMarket = testCase "enterAtMarket creates position in PositionWaitingOpenSubmission state" $ do
  let (newState, actions, _) = runStrategyElement TestConfig defaultState defaultStrategyEnvironment element
  assertBool "Should be exactly 1 position" ((length . positions) newState == 1)
  let pos = head . positions $ newState
  assertBool "Should be in PositionWaitingOpenSubmission" (isPositionWaitingOpenSubmission . posState $ pos)
  let (PositionWaitingOpenSubmission order) = posState pos
  assertBool "Account should be 'test_account'" (orderAccountId order == "test_account")
  assertBool "Security should be 'TEST_TICKER'" (orderSecurity order == "TEST_TICKER")
  assertBool "Order price should be Market" (orderPrice order == Market)
  assertBool "Order quantity should be 1" (orderQuantity order == 1)
  assertBool "Executed order quantity should be 0" (orderExecutedQuantity order == 0)
  assertBool "Order operation should be Buy" (orderOperation order == Buy)
  assertBool "Order signal id should be correct" (orderSignalId order == (SignalId "test_instance" "long" ""))
  where
    element = enterAtMarket "long" Buy

    isPositionWaitingOpenSubmission (PositionWaitingOpenSubmission _) = True
    isPositionWaitingOpenSubmission _                                 = False

testEnterAtMarketSendsAction = testCase "enterAtMarket sends ActionSubmitOrder" $ do
  let (newState, actions, _) = runStrategyElement TestConfig defaultState defaultStrategyEnvironment element
  case (L.find isActionOrder actions) of
    Just (ActionOrder order) -> do
      assertBool "Account should be 'test_account'" (orderAccountId order == "test_account")
      assertBool "Security should be 'TEST_TICKER'" (orderSecurity order == "TEST_TICKER")
      assertBool "Order price should be Market" (orderPrice order == Market)
      assertBool "Order quantity should be 1" (orderQuantity order == 1)
      assertBool "Executed order quantity should be 0" (orderExecutedQuantity order == 0)
      assertBool "Order operation should be Buy" (orderOperation order == Buy)
      assertBool "Order signal id should be correct" (orderSignalId order == (SignalId "test_instance" "long" ""))
    Nothing -> assertFailure "Should be exactly 1 ActionOrder"
  where
    element = enterAtMarket "long" Buy

    isActionOrder (ActionOrder _) = True
    isActionOrder _               = False

testDefaultHandlerSubmissionDeadline = testCase "defaultHandler after submission deadline marks position as cancelled" $ do
  let (newState, actions, _) = runStrategyElement TestConfig defaultState defaultStrategyEnvironment element
  let (newState', actions', _) = runStrategyElement TestConfig newState defaultStrategyEnvironment { seLastTimestamp = afterDeadline } $ defaultHandler (NewTick tick)
  let pos = head . positions $ newState'
  assertBool "Cancelled position" (posState pos == PositionCancelled)
  where
    element = enterAtMarket "long" Buy
    afterDeadline = (UTCTime (fromGregorian 1970 1 1) 100)
    tick = Tick {
            security = "TEST_TICKER",
            datatype = LastTradePrice,
            timestamp = afterDeadline,
            value = fromDouble 12.00,
            volume = 1 }

testDefaultHandlerAfterSubmissionPositionIsWaitingOpen = testCase "defaultHandler after successful submission sets position state as PositionWaitingOpen" $ do
  let (newState, actions, _) = runStrategyElement TestConfig defaultState defaultStrategyEnvironment element
  let pos = head . positions $ newState
  let (PositionWaitingOpenSubmission order) = posState pos
  let (newState', actions', _) = runStrategyElement TestConfig newState defaultStrategyEnvironment { seLastTimestamp = beforeDeadline } $ defaultHandler (OrderSubmitted order {orderId = 1 })
  let pos' = head . positions $ newState'
  assertEqual "New position state should be PositionWaitingOpen" (posState pos') PositionWaitingOpen
  where
    element = enterAtMarket "long" Buy
    beforeDeadline = (UTCTime (fromGregorian 1970 1 1) 1)

testDefaultHandlerPositionWaitingOpenOrderCancelledExecuted0 = testCase "defaultHandler in PositionWaitingOpen, if order is cancelled and nothing is executed, marks position as cancelled" $ do
  let (newState, actions, _) = runStrategyElement TestConfig defaultState defaultStrategyEnvironment element
  let pos = head . positions $ newState
  let (PositionWaitingOpenSubmission order) = posState pos
  let (newState', actions', _) = runStrategyElement TestConfig newState defaultStrategyEnvironment { seLastTimestamp = ts1 } $ defaultHandler (OrderSubmitted order {orderId = 1 })
  let (newState'', actions'', _) = runStrategyElement TestConfig newState defaultStrategyEnvironment { seLastTimestamp = ts2 } $ defaultHandler (OrderUpdate 1 Cancelled)
  let pos = head . positions $ newState''
  assertEqual "New position state should be PositionCancelled" (posState pos) PositionCancelled
  where
    element = enterAtMarket "long" Buy
    ts1 = (UTCTime (fromGregorian 1970 1 1) 1)
    ts2 = (UTCTime (fromGregorian 1970 1 1) 2)

testDefaultHandlerPositionWaitingOpenOrderOpenExecuted1 = testCase "defaultHandler in PositionWaitingOpen, if order is cancelled and something is executed, marks position as open" $ do
  let (newState, actions, _) = runStrategyElement TestConfig defaultState defaultStrategyEnvironment element
  let pos = head . positions $ newState
  let (PositionWaitingOpenSubmission order) = posState pos
  let (newState', actions', _) = runStrategyElement TestConfig newState defaultStrategyEnvironment { seLastTimestamp = ts1, seVolume = 2 } $ defaultHandler (OrderSubmitted order {orderId = 1 })
  let (newState'', actions'', _) = runStrategyElement TestConfig newState' defaultStrategyEnvironment { seLastTimestamp = ts2 } $ defaultHandler (NewTrade trade)
  let (newState''', actions''', _) = runStrategyElement TestConfig newState'' defaultStrategyEnvironment { seLastTimestamp = ts3 } $ defaultHandler (OrderUpdate 1 Cancelled)
  let pos = head . positions $ newState'''
  assertEqual "New position state should be PositionOpen" (posState pos) PositionOpen
  where
    element = enterAtMarket "long" Buy
    ts1 = (UTCTime (fromGregorian 1970 1 1) 1)
    ts2 = (UTCTime (fromGregorian 1970 1 1) 2)
    ts3 = (UTCTime (fromGregorian 1970 1 1) 3)
    trade = Trade
            {
              tradeOrderId = 1,
              tradePrice = fromDouble 10,
              tradeQuantity = 1,
              tradeVolume = fromDouble 10,
              tradeVolumeCurrency = "FOO",
              tradeOperation = Buy,
              tradeAccount = "test_account",
              tradeSecurity = "TEST_TICKER",
              tradeTimestamp = ts3,
              tradeCommission = fromDouble 0,
              tradeSignalId = SignalId "test_instance" "long" ""
            }


