{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MultiWayIf   #-}

{-|
 - Module       : ATrade.BarAggregator
 - Description  : Aggregates incoming tick stream to bars
 - Copyright    : (c) Denis Tereshkin 2016-2017
 - License      : BSD
 - Maintainer   : denis@kasan.ws
 - Stability    : experimental
 - Portability  : POSIX
 -
 - This module defines a set of functions that help to convert stream of ticks into bars.
 -}

module ATrade.BarAggregator (
  lBars,
  lLastTicks,
  BarAggregator(..),
  mkAggregatorFromBars,
  handleTicks,
  handleTick,
  updateTime,
  handleBar,
  hmsToDiffTime
) where

import           ATrade.RoboCom.Types
import           ATrade.RoboCom.Utils
import           ATrade.Types
import           Control.Lens
import           Control.Monad.State
import qualified Data.Map.Strict      as M
import           Data.Time.Clock
import           Debug.Trace

-- | Bar aggregator state
data BarAggregator = BarAggregator {
  bars            :: !(M.Map TickerId BarSeries),
  lastTicks       :: !(M.Map (TickerId, DataType) Tick),
  tickTimeWindows :: [(DiffTime, DiffTime)]
} deriving (Show)

-- | Creates `BarAggregator` from history
mkAggregatorFromBars :: M.Map TickerId BarSeries -> [(DiffTime, DiffTime)] -> BarAggregator
mkAggregatorFromBars myBars timeWindows = BarAggregator {
  bars = myBars,
  lastTicks = M.empty,
  tickTimeWindows = timeWindows }

lBars :: (M.Map TickerId BarSeries -> Identity (M.Map TickerId BarSeries)) -> BarAggregator -> Identity BarAggregator
lBars = lens bars (\s b -> s { bars = b })

lLastTicks :: (M.Map (TickerId, DataType) Tick -> Identity (M.Map (TickerId, DataType) Tick)) -> BarAggregator -> Identity BarAggregator
lLastTicks = lens lastTicks (\s b -> s { lastTicks = b })

hmsToDiffTime :: Int -> Int -> Int -> DiffTime
hmsToDiffTime h m s = secondsToDiffTime $ toInteger $ h * 3600 + m * 60 + s

handleTicks :: [Tick] -> BarAggregator -> ([Bar], BarAggregator)
handleTicks ticks aggregator = foldl f ([], aggregator) ticks
  where
    f (bars', agg) tick = let (mbar, agg') = handleTick tick agg in
      case mbar of
        Just bar -> (bar : bars', agg')
        _        -> (bars', agg')

-- | main logic of bar aggregator
handleTick :: Tick -> BarAggregator -> (Maybe Bar, BarAggregator)
handleTick tick = runState $ do
  lLastTicks %= M.insert (security tick, datatype tick) tick
  tws <- gets tickTimeWindows
  mybars <- gets bars
  if (any (isInTimeInterval tick) tws)
    then
      case M.lookup (security tick) mybars of
        Just series -> case bsBars series of
          (b:bs) -> do
            let currentBn = barNumber (barTimestamp b) (tfSeconds $ bsTimeframe series)
            case datatype tick of
              LastTradePrice ->
                if volume tick > 0
                  then
                    if currentBn == barNumber (timestamp tick) (tfSeconds $ bsTimeframe series)
                      then do
                        lBars %= M.insert (security tick) series { bsBars = updateBar b tick : bs }
                        return Nothing
                      else do
                        lBars %= M.insert (security tick) series { bsBars = barFromTick tick : b : bs }
                        return . Just $ b
                  else
                    return Nothing
              _ ->
                if currentBn == barNumber (timestamp tick) (tfSeconds $ bsTimeframe series)
                  then do
                    lBars %= M.insert (security tick) series { bsBars = updateBarTimestamp b tick : bs }
                    return Nothing
                  else
                    return Nothing
          _ -> case datatype tick of
            LastTradePrice -> do
              if volume tick > 0
                then do
                  lBars %= M.insert (security tick) series { bsBars = barFromTick tick : [] }
                  return Nothing
                else
                  return Nothing
            _ -> return Nothing
        _ -> return Nothing
    else
      return Nothing
  where
    isInTimeInterval tick (a, b) = (utctDayTime . timestamp) tick >= a && (utctDayTime . timestamp) tick <= b
    barFromTick !newtick = Bar { barSecurity = security newtick,
      barTimestamp = timestamp newtick,
      barOpen = value newtick,
      barHigh = value newtick,
      barLow = value newtick,
      barClose = value newtick,
      barVolume = abs . volume $ newtick }
    updateBar !bar newtick =
      let newHigh = max (barHigh bar) (value newtick)
          newLow = min (barLow bar) (value newtick) in
        if timestamp newtick >= barTimestamp bar
          then bar {
            barTimestamp = timestamp newtick,
            barHigh = newHigh,
            barLow = newLow,
            barClose = value newtick,
            barVolume = barVolume bar + (abs . volume $ newtick) }
          else bar

    updateBarTimestamp !bar newtick = bar { barTimestamp = newTimestamp }
      where
        newTimestamp = timestamp newtick

    emptyBarFrom !bar newtick = newBar
      where
        newTimestamp = timestamp newtick
        newBar = Bar {
          barSecurity = barSecurity bar,
          barTimestamp = newTimestamp,
          barOpen = barClose bar,
          barHigh = barClose bar,
          barLow = barClose bar,
          barClose = barClose bar,
          barVolume = 0 }

updateTime :: Tick -> BarAggregator -> (Maybe Bar, BarAggregator)
updateTime tick = runState $ do
  lLastTicks %= M.insert (security tick, datatype tick) tick
  tws <- gets tickTimeWindows
  mybars <- gets bars
  if (any (isInTimeInterval tick) tws)
    then
      case M.lookup (security tick) mybars of
        Just series -> case bsBars series of
          (b:bs) -> do
            let currentBn = barNumber (barTimestamp b) (tfSeconds $ bsTimeframe series)
            let thisBn = barNumber (timestamp tick) (tfSeconds $ bsTimeframe series)
            if
              | currentBn == thisBn -> do
                  lBars %= M.insert (security tick) series { bsBars = updateBarTimestamp b tick : bs }
                  return Nothing
              | currentBn < thisBn -> do
                  lBars %= M.insert (security tick) series { bsBars = emptyBarFromTick tick : b : bs }
                  return $ Just b
              | otherwise -> return Nothing
          _ -> return Nothing
        _ -> return Nothing
    else
      return Nothing
  where
    isInTimeInterval t (a, b) = (utctDayTime . timestamp) t >= a && (utctDayTime . timestamp) t <= b
    emptyBarFromTick !newtick = Bar { barSecurity = security newtick,
      barTimestamp = timestamp newtick,
      barOpen = value newtick,
      barHigh = value newtick,
      barLow = value newtick,
      barClose = value newtick,
      barVolume = 0 }

    updateBarTimestamp !bar newtick = bar { barTimestamp = newTimestamp }
      where
        newTimestamp = timestamp newtick

handleBar :: Bar -> BarAggregator -> (Maybe Bar, BarAggregator)
handleBar bar = runState $ do
  tws <- gets tickTimeWindows
  mybars <- gets bars
  if (any (isInTimeInterval bar) tws)
    then
      case M.lookup (barSecurity bar) mybars of
        Just series -> case bsBars series of
          (b:bs) -> do
            let currentBn = barNumber (barTimestamp b) (tfSeconds $ bsTimeframe series)
            if
              | currentBn == barNumber (barTimestamp bar) (tfSeconds $ bsTimeframe series) -> do
                lBars %= M.insert (barSecurity bar) series { bsBars = updateBar b bar : bs }
                return Nothing
              | currentBn < barNumber (barTimestamp bar) (tfSeconds $ bsTimeframe series) -> do
                if barEndTime b (tfSeconds $ bsTimeframe series) == barTimestamp bar
                  then do
                    lBars %= M.insert (barSecurity bar) series { bsBars = emptyBarFrom bar : (updateBar b bar : bs) }
                    return . Just $ updateBar b bar
                  else do
                    lBars %= M.insert (barSecurity bar) series { bsBars = bar : b : bs }
                    return . Just $ b
              | otherwise -> return Nothing
          _      -> do
            lBars %= M.insert (barSecurity bar) series { bsBars = [bar] }
            return Nothing
        _ -> return Nothing
    else
      return Nothing
  where
    isInTimeInterval bar' (a, b) = (utctDayTime . barTimestamp) bar' >= a && (utctDayTime . barTimestamp) bar' <= b
    updateBar !bar' newbar =
      let newHigh = max (barHigh bar') (barHigh newbar)
          newLow = min (barLow bar') (barLow newbar) in
        bar' {
            barTimestamp = barTimestamp newbar,
            barHigh = newHigh,
            barLow = newLow,
            barClose = barClose newbar,
            barVolume = barVolume bar' + (abs . barVolume $ newbar) }

    emptyBarFrom bar' = Bar {
          barSecurity = barSecurity bar',
          barTimestamp = 0.000001 `addUTCTime` barTimestamp bar',
          barOpen = barClose bar',
          barHigh = barClose bar',
          barLow = barClose bar',
          barClose = barClose bar',
          barVolume = 0 }

