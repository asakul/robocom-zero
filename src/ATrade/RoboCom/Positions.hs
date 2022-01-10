{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TypeApplications  #-}

{-|
 - Module       : ATrade.RoboCom.Combinators
 - Description  : Reusable behavioural components of strategies
 - Copyright    : (c) Denis Tereshkin 2021
 - License      : BSD 3-clause
 - Maintainer   : denis@kasan.ws
 - Stability    : experimental
 - Portability  : POSIX
 -
 - A lot of behaviour is common for most of the strategies.
 - This module contains those common blocks which can be composed to avoid boilerplate in main strategy code.
 -}

module ATrade.RoboCom.Positions
(
  StateHasPositions(..),
  ParamsSize(..),
  PositionState(..),
  Position(..),
  posIsOpen,
  posIsDead,
  posIsLong,
  posIsShort,
  posOrderId,
  posEqByIds,
  modifyPositions,
  defaultHandler,
  modifyPosition,
  getCurrentTicker,
  getCurrentTickerSeries,
  getLastActivePosition,
  getAllActivePositions,
  getAllActiveAndPendingPositions,
  onNewBarEvent,
  onNewTickEvent,
  onNewTickEventWithDatatype,
  onTimerFiredEvent,
  onOrderSubmittedEvent,
  onOrderUpdateEvent,
  onTradeEvent,
  onActionCompletedEvent,
  enterAtMarket,
  enterAtMarketForTicker,
  enterAtMarketWithParams,
  enterAtLimit,
  enterAtLimitForTicker,
  enterAtLimitForTickerWithParams,
  enterLongAtMarket,
  enterShortAtMarket,
  enterLongAtLimit,
  enterShortAtLimit,
  enterLongAtLimitForTicker,
  enterShortAtLimitForTicker,
  exitAtMarket,
  exitAtLimit,
  doNothing,
  setStopLoss,
  setLimitStopLoss,
  setTakeProfit,
  setStopLossAndTakeProfit,

  handlePositions,
  calculateSizeIVS,
  calculateSizeIVSWith,
  calculateSizeFixed,
  calculateSizeFixedCash,
  calculateSizeFixedCashWith,
  calculateSizeIVSWithMinimum) where

import           GHC.Generics

import           ATrade.RoboCom.Monad
import           ATrade.RoboCom.Types
import           ATrade.Types

import           Control.Lens hiding (op)
import           Control.Monad

import           ATrade.Logging            (Severity (Trace, Warning))
import qualified ATrade.RoboCom.Indicators as I
import           Data.Aeson
import qualified Data.List                 as L
import qualified Data.List.NonEmpty        as NE
import qualified Data.Text                 as T
import qualified Data.Text.Lazy            as TL
import           Data.Time.Clock
import           GHC.Records               (HasField (..))

data PositionState = PositionWaitingOpenSubmission Order
  | PositionWaitingOpen
  | PositionOpen
  | PositionWaitingPendingCancellation
  | PositionWaitingCloseSubmission Order
  | PositionWaitingClose
  | PositionClosed
  | PositionCancelled
    deriving (Show, Eq, Generic)

data Position = Position {
  posId                 :: T.Text,
  posAccount            :: T.Text,
  posTicker             :: TickerId,
  posBalance            :: Integer,
  posState              :: PositionState,
  posNextState          :: Maybe PositionState,
  posStopPrice          :: Maybe Price,
  posStopLimitPrice     :: Maybe Price,
  posTakeProfitPrice    :: Maybe Price,
  posCurrentOrder       :: Maybe Order,
  posSubmissionDeadline :: Maybe UTCTime,
  posExecutionDeadline  :: Maybe UTCTime,
  posEntryTime          :: Maybe UTCTime,
  posExitTime           :: Maybe UTCTime
} deriving (Show, Eq, Generic)

posEqByIds :: Position -> Position -> Bool
posEqByIds p1 p2 = posId p1 == posId p2

posIsOpen :: Position -> Bool
posIsOpen pos = posState pos == PositionOpen

posIsDead :: Position -> Bool
posIsDead pos = posState pos == PositionClosed || posState pos == PositionCancelled

instance FromJSON Position
instance FromJSON PositionState
instance ToJSON Position
instance ToJSON PositionState

posIsLong :: Position -> Bool
posIsLong pos = 0 < posBalance pos

posIsShort :: Position -> Bool
posIsShort pos = 0 > posBalance pos

posOrderId :: Position -> Maybe Integer
posOrderId pos = orderId <$> posCurrentOrder pos

class StateHasPositions a where
  getPositions :: a -> [Position]
  setPositions :: a -> [Position] -> a

-- | Helper function, modifies position list.
modifyPositions :: (StateHasPositions s, MonadRobot m c s) => ([Position] -> [Position]) -> m ()
modifyPositions f = do
  pos <- getPositions <$> getState
  modifyState (\s -> setPositions s (f pos))

class ParamsSize a where
  getPositionSize :: a -> BarSeries -> Operation -> Int

calculateSizeIVS :: (HasField "riskSize" a Double,
                     HasField "stopSize" a Double,
                     HasField "atrPeriod" a Int) =>
  a -> BarSeries -> Operation -> Int

calculateSizeIVS cfg = calculateSizeIVSWith (getField @"atrPeriod" cfg) (getField @"riskSize" cfg) (getField @"stopSize" cfg) cfg

calculateSizeIVSWithMinimum :: (HasField "riskSize" a Double,
                     HasField "stopSize" a Double,
                     HasField "atrPeriod" a Int) =>
  Int -> a -> BarSeries -> Operation -> Int
calculateSizeIVSWithMinimum minVolume cfg series op = max (calculateSizeIVS cfg series op) minVolume

calculateSizeIVSWith :: Int -> Double -> Double -> a -> BarSeries -> Operation -> Int
calculateSizeIVSWith atrPeriod riskSize stopSize _ series _ =
  let atr = I.atr atrPeriod (bsBars series) in
    truncate (riskSize / (atr * stopSize))

calculateSizeFixed :: (HasField "positionSize" a Int) =>
  a -> BarSeries -> Operation -> Int

calculateSizeFixed cfg _ _ = getField @"positionSize" cfg

calculateSizeFixedCash :: ( HasField "totalCash" a Double,
                            HasField "maxPositions" a Int) =>
                          a -> BarSeries -> Operation -> Int
calculateSizeFixedCash cfg = calculateSizeFixedCashWith (getField @"totalCash" cfg) (getField @"maxPositions" cfg) cfg

calculateSizeFixedCashWith :: Double -> Int -> a -> BarSeries -> Operation -> Int
calculateSizeFixedCashWith totalCash maxPositions cfg series _ =
  case bsBars $ series of
    (lastBar:_) ->
      let cashPerPosition = totalCash / fromIntegral maxPositions in
        truncate (cashPerPosition / ((toDouble $ barClose lastBar) * (fromIntegral $ ipLotSize . bsParams $ series)))
    _ -> 0

-- | Helper function. Finds first element in list which satisfies predicate 'p' and if found, applies 'm' to it, leaving other elements intact.
findAndModify :: (a -> Bool) -> (a -> a) -> [a] -> [a]
findAndModify p m (x:xs) = if p x
  then m x : xs
  else x : findAndModify p m xs

findAndModify _ _ [] = []

handlePositions :: (StateHasPositions s) => EventCallback c s
handlePositions event = do
  positions <- getPositions <$> getState
  positions' <- mapM (dispatchPosition event) positions
  modifyState (`setPositions` positions')

orderCorrespondsTo :: Order -> Order -> Bool
orderCorrespondsTo o1 o2 =
  orderAccountId o1 == orderAccountId o2  &&
  orderSecurity  o1 == orderSecurity  o2  &&
  orderQuantity  o1 == orderQuantity  o2  &&
  orderOperation o1 == orderOperation o2  &&
  orderPrice     o1 == orderPrice     o2

orderDeadline :: Maybe UTCTime -> UTCTime -> Bool
orderDeadline maybeDeadline lastTs =
  case maybeDeadline of
    Just deadline -> lastTs > deadline
    Nothing       -> False


dispatchPosition :: (StateHasPositions s, MonadRobot m c s) => Event -> Position -> m Position
dispatchPosition event pos =
  case posState pos of
    PositionWaitingOpenSubmission pendingOrder -> handlePositionWaitingOpenSubmission pendingOrder
    PositionWaitingOpen -> handlePositionWaitingOpen
    PositionOpen -> handlePositionOpen
    PositionWaitingPendingCancellation -> handlePositionWaitingPendingCancellation
    PositionWaitingCloseSubmission pendingOrder -> handlePositionWaitingCloseSubmission pendingOrder
    PositionWaitingClose -> handlePositionWaitingClose
    PositionClosed -> handlePositionClosed pos
    PositionCancelled -> handlePositionCancelled pos
  where
    handlePositionWaitingOpenSubmission pendingOrder = do
      lastTs <- view seLastTimestamp <$> getEnvironment
      if orderDeadline (posSubmissionDeadline pos) lastTs
        then do
          appendToLog Warning $ [t|Submission deadline: %?, %?|] lastTs (posSubmissionDeadline pos)
          return $ pos { posState = PositionCancelled } -- TODO call TimeoutHandler if present
        else case event of
          OrderUpdate oid Submitted -> do
            return $ if orderId pendingOrder == oid
              then pos { posCurrentOrder = Just pendingOrder,
                posState = PositionWaitingOpen,
                posSubmissionDeadline = Nothing }
              else pos
          _ -> return pos

    handlePositionWaitingOpen = do
      lastTs <- view seLastTimestamp <$> getEnvironment
      case posCurrentOrder pos of
        Just order -> if orderDeadline (posExecutionDeadline pos) lastTs
          then
            if posBalance pos == 0
              then do
                cancelOrder $ orderId order
                return $ pos { posState = PositionWaitingPendingCancellation, posNextState = Just PositionCancelled }
              else do
                appendToLog Trace $ [t|Order executed (partially, %? / %?): %?|] (posBalance pos) (orderQuantity order) order
                return pos { posState = PositionOpen, posCurrentOrder = Nothing, posExecutionDeadline = Nothing, posEntryTime = Just lastTs}
          else case event of
            OrderUpdate oid newstate ->
              if oid == orderId order
                then case newstate of
                  Cancelled -> do
                    appendToLog Trace $ [t|Order cancelled in PositionWaitingOpen: balance %d, max %d|] (posBalance pos) (orderQuantity order)
                    if posBalance pos /= 0
                      then return pos { posState = PositionOpen, posCurrentOrder = Nothing, posExecutionDeadline = Nothing, posEntryTime = Just lastTs}
                      else return pos { posState = PositionCancelled }
                  Executed -> do
                    appendToLog Trace $ [t|Order executed: %?|] order
                    return pos { posState = PositionOpen,
                                 posCurrentOrder = Nothing,
                                 posExecutionDeadline = Nothing,
                                 posBalance = balanceForOrder order,
                                 posEntryTime = Just lastTs }
                  Rejected -> do
                    appendToLog Trace $ [t|Order rejected: %?|] order
                    return pos { posState = PositionCancelled, posCurrentOrder = Nothing, posExecutionDeadline = Nothing, posBalance = 0, posEntryTime = Nothing }
                  _ -> do
                    appendToLog Trace $ [t|In PositionWaitingOpen: order state update: %?|] newstate
                    return pos
                else return pos -- Update for another position's order
            NewTrade trade -> do
              appendToLog Trace $ [t|Order new trade: %?/%?|] order trade
              return $ if tradeOrderId trade == orderId order
                then pos { posBalance = if tradeOperation trade == Buy then posBalance pos + tradeQuantity trade else posBalance pos - tradeQuantity trade }
                else pos
            _ -> return pos
        Nothing -> do
          appendToLog Warning $ [t|W: No current order in PositionWaitingOpen state: %?|] pos
          return pos

    handlePositionOpen = do
      lastTs <- view seLastTimestamp <$> getEnvironment
      if
        | orderDeadline (posSubmissionDeadline pos) lastTs -> do
            appendToLog Warning $ [t|PositionId: %? : Missed submission deadline: %?, remaining in PositionOpen state|] (posId pos) (posSubmissionDeadline pos)
            return pos { posSubmissionDeadline = Nothing, posExecutionDeadline = Nothing }
        | orderDeadline (posExecutionDeadline pos) lastTs -> do
            appendToLog Warning $ [t|PositionId: %? : Missed execution deadline: %?, remaining in PositionOpen state|] (posId pos) (posExecutionDeadline pos)
            return pos { posExecutionDeadline = Nothing }
        | otherwise ->  case event of
          NewTick tick -> if
            | datatype tick == LastTradePrice && stopLoss tick -> case posStopLimitPrice pos of
              Nothing  -> exitAtMarket pos "stop"
              Just lim -> exitAtLimit 86400 lim pos "stop"
            | datatype tick == LastTradePrice && takeProfit tick -> exitAtMarket pos "take_profit"
            | otherwise -> return pos
          NewTrade trade -> case posCurrentOrder pos of
            Just order -> return $ if tradeOrderId trade == orderId order
              then pos { posBalance = if tradeOperation trade == Buy then posBalance pos + tradeQuantity trade else posBalance pos - tradeQuantity trade }
              else pos
            Nothing -> return pos
          _ -> return pos

    handlePositionWaitingPendingCancellation = do
      lastTs <- view seLastTimestamp <$> getEnvironment
      if not $ orderDeadline (posSubmissionDeadline pos) lastTs
        then case (event, posCurrentOrder pos, posNextState pos) of
            (OrderUpdate _ newstate, Just _, Just (PositionWaitingCloseSubmission nextOrder)) ->
              if newstate == Cancelled
                then do
                  oid <- submitOrder nextOrder
                  return pos
                           { posState = PositionWaitingCloseSubmission nextOrder { orderId = oid },
                             posSubmissionDeadline = Just (10 `addUTCTime` lastTs),
                             posExecutionDeadline = Nothing }
                else return pos
            (OrderUpdate _ newstate, Just _, Just PositionCancelled) ->
              if newstate == Cancelled
                then return pos { posState = PositionCancelled, posSubmissionDeadline = Nothing, posExecutionDeadline = Nothing }
                else return pos
            _ -> return pos
        else do
          appendToLog Warning "Deadline when cancelling pending order"
          return pos { posState = PositionCancelled }

    handlePositionWaitingCloseSubmission pendingOrder = do
      lastTs <- view seLastTimestamp <$> getEnvironment
      if orderDeadline (posSubmissionDeadline pos) lastTs
        then do
          case posCurrentOrder pos of
            Just order -> cancelOrder (orderId order)
            Nothing    -> doNothing
          return $ pos { posCurrentOrder = Nothing, posState = PositionOpen, posSubmissionDeadline = Nothing } -- TODO call TimeoutHandler if present
        else case event of
          OrderUpdate oid Submitted ->
            return $ if orderId pendingOrder == oid
              then pos { posCurrentOrder = Just pendingOrder,
                posState = PositionWaitingClose,
                posSubmissionDeadline = Nothing }
              else pos
          _ -> return pos

    handlePositionWaitingClose = do
      lastTs <- view seLastTimestamp <$> getEnvironment
      if orderDeadline (posExecutionDeadline pos) lastTs
        then do
          case posCurrentOrder pos of
            Just order -> cancelOrder (orderId order)
            _          -> doNothing
          appendToLog Warning $ [t|Was unable to close position, remaining balance: %?|] (posBalance pos)
          return $ pos { posState = PositionOpen, posSubmissionDeadline = Nothing, posExecutionDeadline = Nothing } -- TODO call TimeoutHandler if present
        else case (event, posCurrentOrder pos) of
          (OrderUpdate oid newstate, Just order) ->
            return $ if orderId order == oid && newstate == Executed
              then pos { posCurrentOrder = Just order,
                posState = PositionClosed,
                posBalance = 0,
                posSubmissionDeadline = Nothing }
              else pos
          (NewTrade trade, Just order) ->
             return $ if (tradeOrderId trade == orderId order)
               then pos { posBalance = if tradeOperation trade == Buy then posBalance pos + tradeQuantity trade else posBalance pos - tradeQuantity trade }
               else pos

          _ -> return pos

    handlePositionClosed = return
    handlePositionCancelled = return

    stopLoss tick =
      if posTicker pos == security tick
        then case posStopPrice pos of
          Just stop -> if posIsLong pos then value tick <= stop else value tick >= stop
          Nothing -> False
      else False

    takeProfit tick =
      if posTicker pos == security tick
        then case posTakeProfitPrice pos of
          Just tp -> if posIsLong pos then value tick >= tp else value tick <= tp
          Nothing -> False
        else False

    balanceForOrder order = if orderOperation order == Buy then orderQuantity order else - orderQuantity order

newPosition :: (StateHasPositions s, MonadRobot m c s) => Order -> T.Text -> TickerId -> Operation -> Int -> NominalDiffTime -> m Position
newPosition order account tickerId operation quantity submissionDeadline = do
  lastTs <- view seLastTimestamp <$> getEnvironment
  let position = Position {
    posId = TL.toStrict $ [t|%?/%?/%?/%?/%?|] account tickerId operation quantity lastTs,
    posAccount = account,
    posTicker = tickerId,
    posBalance = 0,
    posState = PositionWaitingOpenSubmission order,
    posNextState = Just PositionOpen,
    posStopPrice = Nothing,
    posStopLimitPrice = Nothing,
    posTakeProfitPrice = Nothing,
    posCurrentOrder = Nothing,
    posSubmissionDeadline = Just $ submissionDeadline `addUTCTime` lastTs,
    posExecutionDeadline = Nothing,
    posEntryTime = Nothing,
    posExitTime = Nothing
  }
  modifyPositions (\p -> position : p)
  return position

rejectedPosition :: (StateHasPositions s, MonadRobot m c s) => m Position
rejectedPosition =
  return Position {
    posId = "Rejected",
    posAccount = "",
    posTicker = "",
    posBalance = 0,
    posState = PositionCancelled,
    posNextState = Nothing,
    posStopPrice = Nothing,
    posStopLimitPrice = Nothing,
    posTakeProfitPrice = Nothing,
    posCurrentOrder = Nothing,
    posSubmissionDeadline = Nothing,
    posExecutionDeadline = Nothing,
    posEntryTime = Nothing,
    posExitTime = Nothing
  }

reapDeadPositions :: (StateHasPositions s) => EventCallback c s
reapDeadPositions _ = modifyPositions (L.filter (not . posIsDead))

defaultHandler :: (StateHasPositions s) => EventCallback c s
defaultHandler = reapDeadPositions `also` handlePositions

-- | Searches given position and alters it using given function.
modifyPosition :: (StateHasPositions s, MonadRobot m c s) => (Position -> Position) -> Position -> m Position
modifyPosition f oldpos = do
  positions <- getPositions <$> getState
  case L.find (posEqByIds oldpos) positions of
    Just _ -> do
      modifyState (`setPositions` findAndModify (posEqByIds oldpos) f positions)
      return $ f oldpos
    Nothing -> return oldpos

getCurrentTicker :: (MonadRobot m c s) => m [Bar]
getCurrentTicker = do
  (BarSeriesId mainTicker' tf) <- NE.head <$> getAvailableTickers
  maybeBars <- getTicker mainTicker' tf
  case maybeBars of
    Just b -> return $ bsBars b
    _      -> return []

getCurrentTickerSeries :: (MonadRobot m c s) => m (Maybe BarSeries)
getCurrentTickerSeries = do
  (BarSeriesId mainTicker' tf) <- NE.head <$> getAvailableTickers
  getTicker mainTicker' tf

getLastActivePosition :: (StateHasPositions s, MonadRobot m c s) => m (Maybe Position)
getLastActivePosition = L.find (\pos -> posState pos == PositionOpen) . getPositions <$> getState

getAllActivePositions :: (StateHasPositions s, MonadRobot m c s) => m [Position]
getAllActivePositions = L.filter (\pos -> posState pos == PositionOpen) . getPositions <$> getState

getAllActiveAndPendingPositions :: (StateHasPositions s, MonadRobot m c s) => m [Position]
getAllActiveAndPendingPositions = L.filter
  (\pos ->
    posState pos == PositionOpen ||
    posState pos == PositionWaitingOpen ||
    isPositionWaitingOpenSubmission pos) . getPositions <$> getState
  where
    isPositionWaitingOpenSubmission pos = case posState pos of
      PositionWaitingOpenSubmission _ -> True
      _                               -> False

onNewBarEvent :: (MonadRobot m c s) => Event -> (Bar -> m ()) -> m ()
onNewBarEvent event f = case event of
  NewBar (_, bar) -> f bar
  _               -> doNothing

onNewTickEvent :: (MonadRobot m c s) => Event -> (Tick -> m ()) -> m ()
onNewTickEvent event f = case event of
  NewTick tick -> f tick
  _            -> doNothing

onNewTickEventWithDatatype :: (MonadRobot m c s) => Event -> DataType -> (Tick -> m ()) -> m ()
onNewTickEventWithDatatype event dtype f = case event of
  NewTick tick -> when (datatype tick == dtype) $ f tick
  _            -> doNothing

onTimerFiredEvent :: (MonadRobot m c s) => Event -> (UTCTime -> m ()) -> m ()
onTimerFiredEvent event f = case event of
  TimerFired timer -> f timer
  _                -> doNothing


onOrderSubmittedEvent :: (MonadRobot m c s) => Event -> (Order -> m ()) -> m ()
onOrderSubmittedEvent event f = case event of
  OrderSubmitted order -> f order
  _                    -> doNothing

onOrderUpdateEvent :: (MonadRobot m c s) => Event -> (OrderId -> OrderState -> m ()) -> m ()
onOrderUpdateEvent event f = case event of
  OrderUpdate oid newstate -> f oid newstate
  _                        -> doNothing

onTradeEvent :: (MonadRobot m c s) => Event -> (Trade -> m ()) -> m ()
onTradeEvent event f = case event of
  NewTrade trade -> f trade
  _              -> doNothing

onActionCompletedEvent :: (MonadRobot m c s) => Event -> (Int -> Value -> m ()) -> m ()
onActionCompletedEvent event f = case event of
  ActionCompleted tag v -> f tag v
  _                     -> doNothing

roundTo :: Price -> Price -> Price
roundTo quant v = quant * (fromIntegral . floor . toDouble) (v / quant)

enterAtMarket :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => T.Text -> Operation -> m Position
enterAtMarket operationSignalName operation = do
  bsId <- getFirstTickerId
  enterAtMarketForTicker operationSignalName bsId operation

enterAtMarketForTicker :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => T.Text -> BarSeriesId -> Operation -> m Position
enterAtMarketForTicker operationSignalName (BarSeriesId tid tf) operation = do
  maybeSeries <- getTicker tid tf
  case maybeSeries of
    Just series -> do
      env <- getEnvironment
      cfg <- getConfig
      let quantity = getPositionSize cfg series operation
      enterAtMarketWithParams (env ^. seAccount) tid quantity (SignalId (env ^. seInstanceId) operationSignalName "") operation
    Nothing -> do
      appendToLog Warning $ "Unable to get ticker series: " <> TL.fromStrict tid
      rejectedPosition

enterAtMarketWithParams :: (StateHasPositions s, MonadRobot m c s) => T.Text -> TickerId -> Int -> SignalId -> Operation -> m Position
enterAtMarketWithParams account tid quantity signalId operation = do
  oid <- submitOrder $ order tid
  newPosition ((order tid) { orderId = oid }) account tid operation quantity 20
  where
   order tickerId = mkOrder {
    orderAccountId = account,
    orderSecurity = tickerId,
    orderQuantity = toInteger quantity,
    orderPrice = Market,
    orderOperation = operation,
    orderSignalId = signalId
  }

enterAtLimit :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => T.Text -> Price -> Operation -> m Position
enterAtLimit operationSignalName price operation = do
  bsId <- getFirstTickerId
  env <- getEnvironment
  enterAtLimitForTicker bsId operationSignalName price operation

enterAtLimitForTicker :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => BarSeriesId -> T.Text -> Price -> Operation -> m Position
enterAtLimitForTicker (BarSeriesId tid tf) operationSignalName price operation = do
  acc <- view seAccount <$> getEnvironment
  inst <- view seInstanceId <$> getEnvironment
  maybeSeries <- getTicker tid tf
  case maybeSeries of
    Just series -> do
      cfg <- getConfig
      let quantity = getPositionSize cfg series operation
      let roundedPrice = roundTo (ipTickSize . bsParams $ series) price
      enterAtLimitForTickerWithParams tid (fromIntegral $ unBarTimeframe tf) acc quantity (SignalId inst operationSignalName "") roundedPrice operation
    Nothing -> rejectedPosition

enterAtLimitForTickerWithParams ::
  (StateHasPositions s,
   MonadRobot m c s) =>
     TickerId
  -> NominalDiffTime
  -> T.Text
  -> Int
  -> SignalId
  -> Price
  -> Operation
  -> m Position
enterAtLimitForTickerWithParams tickerId timeToCancel account quantity signalId price operation = do
  lastTs <- view seLastTimestamp <$> getEnvironment
  oid <- submitOrder order
  appendToLog Trace $ [t|enterAtLimit: %?, deadline: %?|] tickerId (timeToCancel `addUTCTime` lastTs)
  newPosition (order {orderId = oid}) account tickerId operation quantity 20 >>=
    modifyPosition (\p -> p { posExecutionDeadline = Just $ timeToCancel `addUTCTime` lastTs })
  where
    order = mkOrder {
    orderAccountId = account,
    orderSecurity = tickerId,
    orderQuantity = toInteger quantity,
    orderPrice = Limit price,
    orderOperation = operation,
    orderSignalId = signalId
  }

enterLongAtMarket :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => T.Text -> m Position
enterLongAtMarket operationSignalName = enterAtMarket operationSignalName Buy

enterShortAtMarket :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => T.Text -> m Position
enterShortAtMarket operationSignalName = enterAtMarket operationSignalName Sell

enterLongAtLimit :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => Price -> T.Text -> m Position
enterLongAtLimit price operationSignalName = enterAtLimit operationSignalName price Buy

enterLongAtLimitForTicker :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => BarSeriesId -> Price -> T.Text -> m Position
enterLongAtLimitForTicker tickerId price operationSignalName = enterAtLimitForTicker tickerId operationSignalName price Buy

enterShortAtLimit :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => Price -> T.Text -> m Position
enterShortAtLimit price operationSignalName = enterAtLimit operationSignalName price Sell

enterShortAtLimitForTicker :: (StateHasPositions s, ParamsSize c, MonadRobot m c s) => BarSeriesId -> Price -> T.Text -> m Position
enterShortAtLimitForTicker tickerId price operationSignalName = enterAtLimitForTicker tickerId operationSignalName price Sell

exitAtMarket :: (StateHasPositions s, MonadRobot m c s) => Position -> T.Text -> m Position
exitAtMarket position operationSignalName = do
  inst <- view seInstanceId <$> getEnvironment
  lastTs <- view seLastTimestamp <$> getEnvironment
  case posCurrentOrder position of
    Just order -> do
      cancelOrder (orderId order)
      modifyPosition (\pos ->
        pos { posState = PositionWaitingPendingCancellation,
          posNextState = Just $ PositionWaitingCloseSubmission (closeOrder inst),
          posSubmissionDeadline = Just $ 10 `addUTCTime` lastTs,
          posExecutionDeadline = Nothing }) position

    Nothing -> do
      oid <- submitOrder (closeOrder inst)
      modifyPosition (\pos ->
        pos { posCurrentOrder = Nothing,
          posState = PositionWaitingCloseSubmission (closeOrder inst) { orderId = oid },
          posNextState = Just PositionClosed,
          posSubmissionDeadline = Just $ 10 `addUTCTime` lastTs,
          posExecutionDeadline = Nothing }) position
  where
    closeOrder inst = mkOrder {
        orderAccountId = posAccount position,
        orderSecurity = posTicker position,
        orderQuantity = (abs . posBalance) position,
        orderPrice = Market,
        orderOperation = if posBalance position > 0 then Sell else Buy,
        orderSignalId = (SignalId inst operationSignalName "")
      }

exitAtLimit :: (StateHasPositions s, MonadRobot m c s) => NominalDiffTime -> Price -> Position -> T.Text -> m Position
exitAtLimit timeToCancel price position operationSignalName = do
  lastTs <- view seLastTimestamp <$> getEnvironment
  inst <- view seInstanceId <$> getEnvironment
  cfg <- getConfig
  (BarSeriesId tid tf) <- getFirstTickerId
  maybeSeries <- getTicker tid tf
  case maybeSeries of
    Just series -> do
      let roundedPrice = roundTo (ipTickSize . bsParams $ series) price
      case posCurrentOrder position of
        Just order -> cancelOrder (orderId order)
        Nothing    -> doNothing
      oid <- submitOrder (closeOrder inst roundedPrice)
      appendToLog Trace $ [t|exitAtLimit: %?, deadline: %?|] (posTicker position) (timeToCancel `addUTCTime` lastTs)
      modifyPosition (\pos ->
        pos { posCurrentOrder = Nothing,
          posState = PositionWaitingCloseSubmission (closeOrder inst roundedPrice) { orderId = oid },
          posNextState = Just PositionClosed,
          posSubmissionDeadline = Just $ 10 `addUTCTime` lastTs,
          posExecutionDeadline = Just $ timeToCancel `addUTCTime` lastTs }) position
    Nothing -> do
      appendToLog Warning $ "Unable to locate first bar series"
      return position
  where
    closeOrder inst roundedPrice = mkOrder {
      orderAccountId = posAccount position,
      orderSecurity = posTicker position,
      orderQuantity = (abs . posBalance) position,
      orderPrice = Limit roundedPrice,
      orderOperation = if posBalance position > 0 then Sell else Buy,
      orderSignalId = SignalId inst operationSignalName ""
    }

doNothing :: (MonadRobot m c s) => m ()
doNothing = return ()

setStopLoss :: Price -> Position -> Position
setStopLoss sl pos = pos { posStopPrice = Just sl }

setLimitStopLoss :: Price -> Price -> Position -> Position
setLimitStopLoss sl lim pos = pos { posStopPrice = Just sl, posStopLimitPrice = Just lim }

setTakeProfit :: Price -> Position -> Position
setTakeProfit tp pos = pos { posTakeProfitPrice = Just tp }

setStopLossAndTakeProfit :: Price -> Price -> Position -> Position
setStopLossAndTakeProfit sl tp = setStopLoss sl . setTakeProfit tp

