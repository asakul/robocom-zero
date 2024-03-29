{-# LANGUAGE BangPatterns #-}

module ATrade.Driver.Real.QuoteSourceThread
(
  startQuoteSourceThread
) where

import           ATrade.BarAggregator
import           ATrade.Driver.Types
import           ATrade.QuoteSource.Client
import           ATrade.RoboCom.Monad
import           ATrade.RoboCom.Types
import           ATrade.Types

import           Data.IORef
import qualified Data.Text                      as T

import           Control.Concurrent             hiding (readChan, writeChan,
                                                 writeList2Chan, yield)
import           Control.Concurrent.BoundedChan
import           Control.Exception
import           Control.Monad

import           System.Log.Logger
import           System.ZMQ4                    hiding (Event)

startQuoteSourceThread :: Context -> T.Text -> StrategyInstanceParams -> BoundedChan Event -> IORef BarAggregator -> (Tick -> Bool) -> Maybe Int -> IO ThreadId
startQuoteSourceThread ctx qsEp instanceParams eventChan agg tickFilter maybeSourceTimeframe = forkIO $ do
  tickChan <- newBoundedChan 1000
  bracket (startQuoteSourceClient tickChan tickersList ctx qsEp defaultClientSecurityParams)
    (\qs -> do
      stopQuoteSourceClient qs
      debugM "QSThread" "Quotesource client: stop")
    (\_ -> forever $ do
      qdata <- readChan tickChan
      case qdata of
        QDTick tick -> when (goodTick tick) $ do
          writeChan eventChan (NewTick tick)
          case maybeSourceTimeframe of
            Nothing -> do
              aggValue <- readIORef agg
              case handleTick tick aggValue of
                (Just bar, !newAggValue) -> writeIORef agg newAggValue >> writeChan eventChan (NewBar bar)
                (Nothing, !newAggValue) -> writeIORef agg newAggValue
            Just _ -> return ()
        QDBar (incomingTf, bar) -> do
          aggValue <- readIORef agg
          -- debugM "QSThread" $ "Incoming bar: " ++ show incomingTf ++ ": " ++ show bar
          case maybeSourceTimeframe of
            Just tf -> when (tf == unBarTimeframe incomingTf) $
              case handleBar bar aggValue of
                (Just bar', !newAggValue) -> writeIORef agg newAggValue >> writeChan eventChan (NewBar bar')
                (Nothing, !newAggValue) -> writeIORef agg newAggValue
            _ -> return ())
  where
    goodTick tick = tickFilter tick &&
      (datatype tick /= LastTradePrice || (datatype tick == LastTradePrice && volume tick > 0))

    tickersList = fmap code . tickers $ instanceParams

