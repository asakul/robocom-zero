{-# LANGUAGE OverloadedStrings #-}

module ATrade.Backtest.Execution (
  mkExecutionAgent,
  ExecutionAgent(..),
  executePending,
  executeStep
) where

import qualified Data.Text as T
import qualified Data.Map as M
import qualified Data.List as L
import ATrade.Types
import ATrade.Strategy.Types
import ATrade.Strategy
import Control.Monad.State
import Control.Monad.Trans.Writer
import Data.Decimal
import Data.Time.Clock
import Data.Time.Calendar

data Position = Position {
  ticker :: T.Text,
  balance :: Int }

data ExecutionAgent = ExecutionAgent {
  pendingOrders :: [Order],
  cash :: Decimal,
  currentTime :: UTCTime,
  orderIdCounter :: Integer
}

mkExecutionAgent startCash = ExecutionAgent { pendingOrders = [],
  cash = startCash,
  currentTime = UTCTime (fromGregorian 1970 1 1) 0,
  orderIdCounter = 1 }

executeAtPrice :: Order -> Decimal -> WriterT [Event] (State ExecutionAgent) ()
executeAtPrice order price = do
  when (orderState order == Unsubmitted) $ tell [OrderSubmitted order]
  tell [OrderUpdate (orderId order) Executed]
  timestamp <- gets currentTime
  tell [NewTrade (mkTradeForOrder timestamp order price)]

  case orderOperation order of
    Buy -> modify' (\agent -> agent { cash = cash agent - price * realFracToDecimal 10 (toRational $ orderQuantity order) })
    Sell -> modify' (\agent -> agent { cash = cash agent + price * realFracToDecimal 10 (toRational $ orderQuantity order) })

mkTradeForOrder timestamp order price = Trade { tradeOrderId = orderId order,
  tradePrice = price,
  tradeQuantity = orderQuantity order,
  tradeVolume = price * realFracToDecimal 10 (toRational $ orderQuantity order),
  tradeVolumeCurrency = "TEST_CURRENCY",
  tradeOperation = orderOperation order,
  tradeAccount = orderAccountId order,
  tradeSecurity = orderSecurity order,
  tradeTimestamp = timestamp,
  tradeSignalId = orderSignalId order }


executePending :: Bars -> WriterT [Event] (State ExecutionAgent) ()
executePending bars = do
  orders <- gets pendingOrders
  let (executedOrders, leftover) = L.partition shouldExecute orders

  mapM_ executeAtOrdersPrice executedOrders
  modify' (\s -> s { pendingOrders = leftover } )
  where
    executeAtOrdersPrice order = case orderPrice order of
      Limit price -> executeAtPrice order price
      _ -> return () -- TODO handle stops

    shouldExecute order = case M.lookup (orderSecurity order) bars of
      Just (DataSeries ((ts, bar) : _)) -> case orderPrice order of
        Limit price -> crosses bar price
        _ -> False
      Nothing -> False

    crosses bar price = (barClose bar > price && barOpen bar < price) || (barClose bar < price && barOpen bar > price)

executeStep :: Bars -> [Order] -> WriterT [Event] (State ExecutionAgent) ()
executeStep bars orders = do
  -- Assign consecutive IDs
  orders' <- mapM (\o -> do
    id <- gets orderIdCounter
    modify(\s -> s { orderIdCounter = id + 1 })
    return o { orderId = id }) orders

  let (executableNow, pending) = L.partition isExecutableNow orders'
  mapM_ (executeOrderAtLastPrice bars) executableNow
  modify' (\s -> s { pendingOrders = pending ++ pendingOrders s })

  where
    isExecutableNow order = case M.lookup (orderSecurity order) bars of
      Just (DataSeries (x:xs)) -> case orderPrice order of
        Limit price -> (orderOperation order == Buy && price >= (barClose . snd) x) || (orderOperation order == Sell && price <= (barClose . snd) x)
        Market -> True
      _ -> False

    executeOrderAtLastPrice bars order = case M.lookup (orderSecurity order) bars of
      Just (DataSeries ((ts, bar) : _)) -> executeAtPrice order (barClose bar)
      _ -> return ()
