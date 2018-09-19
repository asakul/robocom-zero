{-# LANGUAGE BangPatterns #-}

module ATrade.Driver.Real.QuoteSourceThread
(
  startQuoteSourceThread
) where

import ATrade.BarAggregator
import ATrade.QuoteSource.Client
import ATrade.RoboCom.Monad
import ATrade.RoboCom.Types
import ATrade.Types
import ATrade.Driver.Real.Types

import Data.IORef
import qualified Data.Text as T

import Control.Concurrent.BoundedChan
import Control.Concurrent hiding (writeChan, readChan, writeList2Chan, yield)
import Control.Exception
import Control.Monad

import System.Log.Logger
import System.ZMQ4 hiding (Event)

startQuoteSourceThread :: Context -> T.Text -> Strategy c s -> BoundedChan Event -> IORef BarAggregator -> (Tick -> Bool) -> IO ThreadId
startQuoteSourceThread ctx qsEp strategy eventChan agg tickFilter = forkIO $ do
  tickChan <- newBoundedChan 1000
  bracket (startQuoteSourceClient tickChan (fmap code . (tickers . strategyInstanceParams) $ strategy) ctx qsEp)
    (\qs -> do
      stopQuoteSourceClient qs
      debugM "Strategy" "Quotesource client: stop")
    (\_ -> forever $ do
      tick <- readChan tickChan
      when (goodTick tick) $ do
        writeChan eventChan (NewTick tick)
        aggValue <- readIORef agg
        case handleTick tick aggValue of
          (Just bar, !newAggValue) -> writeChan eventChan (NewBar bar) >> writeIORef agg newAggValue
          (Nothing, !newAggValue) -> writeIORef agg newAggValue)
  where
    goodTick tick = tickFilter tick &&
      (datatype tick /= LastTradePrice || (datatype tick == LastTradePrice && volume tick > 0))

