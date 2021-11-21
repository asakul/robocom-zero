{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeSynonymInstances       #-}

module Test.Driver.Junction.QuoteThread
(
  unitTests
) where

import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck              as QC
import           Test.Tasty.SmallCheck              as SC

import           ATrade.Driver.Junction.QuoteThread (addSubscription,
                                                     startQuoteThread,
                                                     stopQuoteThread)
import           ATrade.Quotes.HistoryProvider      (HistoryProvider (..))
import           ATrade.Quotes.TickerInfoProvider   (TickerInfoProvider (..))
import           ATrade.QuoteSource.Client          (QuoteData (QDBar))
import           ATrade.QuoteSource.Server          (QuoteSourceServerData (..),
                                                     startQuoteSourceServer,
                                                     stopQuoteSourceServer)
import           ATrade.RoboCom.Types               (BarSeries (bsBars),
                                                     BarSeriesId (BarSeriesId),
                                                     InstrumentParameters (InstrumentParameters))
import           ATrade.Types
import           Control.Concurrent                 (forkIO, threadDelay)
import           Control.Concurrent.BoundedChan     (newBoundedChan, readChan,
                                                     writeChan)
import           Control.Exception                  (bracket)
import           Control.Monad                      (forever)
import           Control.Monad.Reader
import           Data.IORef                         (newIORef, readIORef)
import qualified Data.Map.Strict                    as M
import qualified Data.Text                          as T
import           Data.Time                          (UTCTime (UTCTime),
                                                     fromGregorian)
import           System.IO                          (BufferMode (LineBuffering),
                                                     hSetBuffering, stderr)
import           System.Log.Formatter
import           System.Log.Handler                 (setFormatter)
import           System.Log.Handler.Simple
import           System.Log.Logger
import           System.ZMQ4                        (withContext)
import           Test.Mock.HistoryProvider          (MockHistoryProvider,
                                                     mkMockHistoryProvider,
                                                     mockGetHistory)
import           Test.Mock.TickerInfoProvider       (MockTickerInfoProvider,
                                                     mkMockTickerInfoProvider,
                                                     mockGetInstrumentParameters)

data TestEnv =
  TestEnv
  {
    historyProvider    :: MockHistoryProvider,
    tickerInfoProvider :: MockTickerInfoProvider
  }

type TestM = ReaderT TestEnv IO

instance HistoryProvider TestM where
  getHistory tid tf from to = do
    hp <- asks historyProvider
    liftIO $ mockGetHistory hp tid tf from to

instance TickerInfoProvider TestM where
  getInstrumentParameters tickers = do
    tip <- asks tickerInfoProvider
    liftIO $ mockGetInstrumentParameters tip tickers

qsEndpoint = "inproc://qs"

mockHistoryProvider = mkMockHistoryProvider $ M.fromList [(BarSeriesId "FOO" (BarTimeframe 3600), bars)]
  where
    bars = []

mockTickerInfoProvider = mkMockTickerInfoProvider $ M.fromList [("FOO", InstrumentParameters 10 0.1)]

unitTests = testGroup "Driver.Junction.QuoteThread" [
  testSubscription
  ]

testSubscription :: TestTree
testSubscription = testCase "Subscription" $ withContext $ \ctx -> do
  barsRef <- newIORef M.empty
  serverChan <- newBoundedChan 2000
  bracket
    (startQuoteSourceServer serverChan ctx qsEndpoint defaultServerSecurityParams)
    stopQuoteSourceServer $ \_ ->
      bracket
        (startQuoteThread barsRef ctx qsEndpoint Nothing Nothing (`runReaderT` (TestEnv mockHistoryProvider mockTickerInfoProvider)))

        stopQuoteThread $ \qt -> do
          chan <- newBoundedChan 2000
          addSubscription qt "FOO" (BarTimeframe 3600) chan

          forkIO $ forever $ threadDelay 50000 >> writeChan serverChan (QSSBar (BarTimeframe 3600, bar))

          clientData <- readChan chan
          assertEqual "Invalid client data" clientData (QDBar (BarTimeframe 3600, bar))

          bars <- readIORef barsRef
          case M.lookup (BarSeriesId "FOO" (BarTimeframe 3600)) bars of
            Just series -> assertBool "Length should be >= 1" $ (not . null . bsBars) series
            Nothing -> assertFailure "Bar Series should be present"
  where
    bar =
      Bar {
          barSecurity="FOO", barTimestamp=UTCTime (fromGregorian 2021 11 20) 7200, barOpen=10, barHigh=12, barLow=9, barClose=11, barVolume=100
          }
