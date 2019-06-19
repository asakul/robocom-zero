{-# LANGUAGE BangPatterns #-}

module ATrade.Driver.Real.QuoteSourceThread
(
  startQuoteSourceThread
) where

import           ATrade.BarAggregator
import           ATrade.Driver.Real.Types
import           ATrade.QuoteSource.Client
import           ATrade.RoboCom.Monad
import           ATrade.RoboCom.Types
import           ATrade.Types

import           Data.IORef
import           Data.Maybe
import qualified Data.Text                      as T

import           Control.Concurrent             hiding (readChan, writeChan,
                                                 writeList2Chan, yield)
import           Control.Concurrent.BoundedChan
import           Control.Exception
import           Control.Monad

import           System.Log.Logger
import           System.ZMQ4                    hiding (Event)

startQuoteSourceThread :: Context -> T.Text -> Strategy c s -> BoundedChan Event -> IORef BarAggregator -> (Tick -> Bool) -> Maybe Int -> IO ThreadId
startQuoteSourceThread ctx qsEp strategy eventChan agg tickFilter maybeSourceTimeframe = forkIO $ do
  tickChan <- newBoundedChan 1000
  bracket (startQuoteSourceClient tickChan tickersList ctx qsEp)
    (\qs -> do
      stopQuoteSourceClient qs
      debugM "Strategy" "Quotesource client: stop")
    (\_ -> forever $ do
      qdata <- readChan tickChan
      case qdata of
        QDTick tick -> when (goodTick tick) $ do
          writeChan eventChan (NewTick tick)
          when (isNothing maybeSourceTimeframe) $ do
            aggValue <- readIORef agg
            case handleTick tick aggValue of
              (Just bar, !newAggValue) -> writeChan eventChan (NewBar bar) >> writeIORef agg newAggValue
              (Nothing, !newAggValue) -> writeIORef agg newAggValue
        QDBar (_, bar) -> do
          aggValue <- readIORef agg
          when (isJust maybeSourceTimeframe) $ do
            case handleBar bar aggValue of
              (Just bar', !newAggValue) -> writeChan eventChan (NewBar bar') >> writeIORef agg newAggValue
              (Nothing, !newAggValue) -> writeIORef agg newAggValue)
  where
    goodTick tick = tickFilter tick &&
      (datatype tick /= LastTradePrice || (datatype tick == LastTradePrice && volume tick > 0))

    tickersList = fmap code . (tickers . strategyInstanceParams) $ strategy

