{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}

module ATrade.Driver.Backtest (
  backtestMain
) where

import           ATrade.Driver.Types      (InitializationCallback,
                                           Strategy (..),
                                           StrategyInstanceParams (..))
import           ATrade.Exceptions
import           ATrade.Quotes.Finam      as QF
import           ATrade.RoboCom.Monad     (Event (..), EventCallback,
                                           MonadRobot (..),
                                           StrategyEnvironment (..),
                                           appendToLog, seBars, seLastTimestamp,
                                           st)
import           ATrade.RoboCom.Positions
import           ATrade.RoboCom.Types     (BarSeries (..), Ticker (..),
                                           Timeframe (..))
import           ATrade.Types
import           Conduit                  (awaitForever, runConduit, yield,
                                           (.|))
import           Control.Exception.Safe
import           Control.Lens
import           Control.Monad.ST         (runST)
import           Control.Monad.State
import           Data.Aeson               (FromJSON (..), Result (..),
                                           Value (..), decode)
import           Data.Aeson.Types         (parseMaybe)
import           Data.ByteString.Lazy     (readFile, toStrict)
import           Data.Default
import           Data.HashMap.Strict      (lookup)
import           Data.List                (concat, filter, find, partition)
import           Data.List.Split          (splitOn)
import qualified Data.Map.Strict          as M
import           Data.Semigroup           ((<>))
import           Data.STRef               (newSTRef, readSTRef, writeSTRef)
import qualified Data.Text                as T
import           Data.Text.IO             (putStrLn)
import           Data.Time.Calendar       (fromGregorian)
import           Data.Time.Clock          (DiffTime, UTCTime (..))
import           Data.Vector              ((!), (!?), (//))
import qualified Data.Vector              as V
import           Options.Applicative      hiding (Success)
import           Prelude                  hiding (lookup, putStrLn, readFile)
import           Safe                     (headMay)

data Feed = Feed TickerId FilePath
  deriving (Show, Eq)

data Params = Params {
  strategyConfigFile :: FilePath,
  qtisEndpoint       :: Maybe String,
  paramsFeeds        :: [Feed]
} deriving (Show, Eq)

data BacktestState c s = BacktestState {
  _cash                :: Double,
  _robotState          :: s,
  _robotParams         :: c,
  _strategyEnvironment :: StrategyEnvironment,
  _pendingOrders       :: [Order],
  _pendingEvents       :: [Event],
  _tradesLog           :: [Trade],
  _orderIdCounter      :: Integer,
  _pendingTimers       :: [UTCTime],
  _logs                :: [T.Text]
}

makeLenses ''BacktestState

paramsParser :: Parser Params
paramsParser = Params
  <$> strOption (
      long "config" <> short 'c'
    )
  <*> optional ( strOption
    ( long "qtis" <> short 'q' <> metavar "ENDPOINT/ID" ))
  <*> some (option feedArgParser (
      long "feed" <> short 'f'
    ))

feedArgParser :: ReadM Feed
feedArgParser = eitherReader (\s -> case splitOn ":" s of
  [tid, fpath] -> Right $ Feed (T.pack tid) fpath
  _            -> Left $ "Unable to parse feed id: " ++ s)

backtestMain :: (FromJSON c, StateHasPositions s) => DiffTime -> s -> Maybe (InitializationCallback c) -> EventCallback c s -> IO ()
backtestMain dataDownloadDelta defaultState initCallback callback = do
  params <- execParser opts
  (tickerList, config) <- loadStrategyConfig params

  let instanceParams = StrategyInstanceParams {
    strategyInstanceId = "foo",
    strategyAccount = "foo",
    strategyVolume = 1,
    tickers = tickerList,
    strategyQuotesourceEp = "",
    strategyBrokerEp = "",
    strategyHistoryProviderType = "",
    strategyHistoryProvider = "",
    strategyQTISEp = T.pack <$> qtisEndpoint params}

  updatedConfig <- case initCallback of
    Just cb -> cb config instanceParams
    Nothing -> return config

  feeds <- loadFeeds (paramsFeeds params)

  runBacktestDriver feeds config tickerList
  where
    opts = info (helper <*> paramsParser)
      ( fullDesc <> header "ATrade strategy backtesting framework" )

    runBacktestDriver feeds params tickerList = do
      let s = runConduit $ barStreamFromFeeds feeds .| backtestLoop
      let finalState = execState (unBacktestingMonad s) $ defaultBacktestState defaultState params tickerList
      print $ finalState ^. cash
      print $ finalState ^. tradesLog
      forM_ (reverse $ finalState ^. logs) putStrLn

    loadStrategyConfig :: (FromJSON c) => Params -> IO ([Ticker], c)
    loadStrategyConfig params = do
      content <- readFile (strategyConfigFile params)
      case loadStrategyConfig' content of
        Just (tickersList, config) -> return (tickersList, config)
        _ -> throw $ UnableToLoadConfig (T.pack . strategyConfigFile $ params)

    loadStrategyConfig' content = do
      v <- decode content
      case v of
        Object o -> do
          mbTickers <- "tickers" `lookup` o
          mbParams <- "params" `lookup` o
          tickers <- parseMaybe parseJSON mbTickers
          params <- parseMaybe parseJSON mbParams
          return (tickers, params)
        _ -> Nothing

    resultToMaybe (Error _)   = Nothing
    resultToMaybe (Success a) = Just a

    barStreamFromFeeds feeds = case nextBar feeds of
      Just (bar, feeds') -> yield bar >> barStreamFromFeeds feeds'
      _                  -> return ()

    nextBar :: V.Vector [Bar] -> Maybe (Bar, V.Vector [Bar])
    nextBar feeds = case indexOfNextFeed feeds of
      Just ix -> do
        f <- feeds !? ix
        h <- headMay f
        return (h, feeds // [(ix, tail f)])
      _ -> Nothing

    indexOfNextFeed feeds = runST $ do
      minTs <- newSTRef Nothing
      minIx <- newSTRef Nothing
      forM_ [0..(V.length feeds-1)] (\ix -> do
        let feed = feeds ! ix
        curIx <- readSTRef minIx
        curTs <- readSTRef minTs
        case feed of
          x:_ -> case curTs of
            Just ts -> when (barTimestamp x < ts) $ do
              writeSTRef minIx $ Just ix
              writeSTRef minTs $ Just (barTimestamp x)
            _ -> do
              writeSTRef minIx $ Just ix
              writeSTRef minTs $ Just (barTimestamp x)
          _ -> return ())
      readSTRef minIx

    backtestLoop = awaitForever (\bar -> do
      _curState <- use robotState
      _env <- gets _strategyEnvironment
      let newTimestamp = barTimestamp bar
      strategyEnvironment . seBars %= (flip updateBars bar)
      strategyEnvironment . seLastTimestamp .= newTimestamp
      enqueueEvent (NewBar bar)
      lift handleEvents)

    handleEvents = do
      events <- use pendingEvents
      case events of
        (x:xs) -> do
          pendingEvents .= xs
          handleEvent x
          handleEvents
        _ -> return ()

    executePendingOrders bar = do
      executeMarketOrders bar
      executeLimitOrders bar

    executeLimitOrders bar = do
      (limitOrders, otherOrders'') <- partition
        (\o -> case orderPrice o of
          Limit _ -> True
          _       -> False) <$> use pendingOrders
      let (executableOrders, otherOrders') = partition (isExecutable bar) limitOrders
      pendingOrders .= otherOrders' ++ otherOrders''
      forM_ executableOrders $ \order -> order `executeAtPrice` priceForLimitOrder order bar

    isExecutable bar order = case orderPrice order of
      Limit price -> if orderOperation order == Buy
                        then price >= barLow bar
                        else price <= barHigh bar
      _ -> True

    priceForLimitOrder order bar = case orderPrice order of
      Limit price -> if orderOperation order == Buy
                        then if price >= barOpen bar
                                then barOpen bar
                                else price
                        else if price <= barOpen bar
                                then barOpen bar
                                else price
      _ -> error "Should've been limit order"

    executeMarketOrders bar = do
      (marketOrders, otherOrders) <- partition (\o -> orderPrice o == Market) <$> use pendingOrders
      pendingOrders .= otherOrders
      forM_ marketOrders $ \order ->
        order `executeAtPrice` barOpen bar

    executeAtPrice order price = do
      ts <- use $ strategyEnvironment . seLastTimestamp
      let thisTrade = mkTrade order price ts
      tradesLog %= (\log' -> thisTrade : log')
      pendingEvents %= ((:) (OrderUpdate (orderId order) Executed))
      pendingEvents %= ((:) (NewTrade thisTrade))

    mkTrade :: Order -> Price -> UTCTime -> Trade
    mkTrade order price ts = Trade {
      tradeOrderId = orderId order,
      tradePrice = price,
      tradeQuantity = orderQuantity order,
      tradeVolume = (fromIntegral . orderQuantity $ order) * price,
      tradeVolumeCurrency = "pt",
      tradeOperation = orderOperation order,
      tradeAccount = orderAccountId order,
      tradeSecurity = orderSecurity order,
      tradeTimestamp = ts,
      tradeCommission = 0,
      tradeSignalId = orderSignalId order
                                }

    handleEvent event@(NewBar bar) = do
      executePendingOrders bar
      firedTimers <- fireTimers (barTimestamp bar)
      mapM_ (\x -> enqueueEvent (TimerFired x)) firedTimers
      handleEvent' event
      return ()

    handleEvent event = handleEvent' event

    handleEvent' event = callback event

    updateBars barMap newbar = M.alter (\case
      Nothing -> Just BarSeries { bsTickerId = barSecurity newbar,
        bsTimeframe = Timeframe 60,
        bsBars = [newbar, newbar] }
      Just bs -> Just bs { bsBars = updateBarList newbar (bsBars bs) }) (barSecurity newbar) barMap

    updateBarList newbar (_:bs) = newbar:newbar:bs
    updateBarList newbar _      = newbar:[newbar]

    fireTimers ts = do
      (firedTimers, otherTimers) <- partition (< ts) <$> use pendingTimers
      pendingTimers .= otherTimers
      return firedTimers

    loadFeeds :: [Feed] -> IO (V.Vector [Bar])
    loadFeeds feeds = V.fromList <$> mapM loadFeed feeds
    loadFeed (Feed tid path) = do
      content <- readFile path
      case QF.parseQuotes $ toStrict content of
        Just quotes -> return $ fmap (rowToBar tid) quotes
        _           -> throw $ UnableToLoadFeed (T.pack path)

    rowToBar tid r = Bar tid (rowTime r) (rowOpen r) (rowHigh r) (rowLow r) (rowClose r) (rowVolume r)


    enqueueEvent event = pendingEvents %= ((:) event)

instance (Default c, Default s) => Default (BacktestState s c)
  where
    def = defaultBacktestState def def []

defaultBacktestState s c tickerList = BacktestState 0 s c (StrategyEnvironment "" "" 1 tickers (UTCTime (fromGregorian 1970 1 1) 0)) [] [] [] 1 [] []
  where
    tickers = M.fromList $ map (\x -> (code x, BarSeries (code x) (Timeframe (timeframeSeconds x)) [])) tickerList

newtype BacktestingMonad s c a = BacktestingMonad { unBacktestingMonad :: State (BacktestState s c) a }
  deriving (Functor, Applicative, Monad, MonadState (BacktestState s c))

nextOrderId :: BacktestingMonad s c OrderId
nextOrderId = do
  orderIdCounter += 1
  use orderIdCounter

instance MonadRobot (BacktestingMonad c s) c s where
  submitOrder order = do
    oid <- nextOrderId
    let orderWithId = order { orderId = oid }
    pendingOrders %= ((:) orderWithId)
    pendingEvents %= ((:) (OrderSubmitted orderWithId))
  cancelOrder oid = do
    orders <- use pendingOrders
    let (matchingOrders, otherOrders) = partition (\o -> orderId o == oid) orders
    case matchingOrders of
      [] -> return ()
      xs -> do
        mapM_ (\o -> pendingEvents %= ((:) (OrderUpdate (orderId o) Cancelled))) xs
        pendingOrders .= otherOrders
  appendToLog txt = logs %= ((:) txt)
  setupTimer time = pendingTimers %= ((:) time)
  enqueueIOAction actionId action = error "Backtesting io actions is not supported"
  getConfig = use robotParams
  getState = use robotState
  setState s = robotState .= s
  getEnvironment = use strategyEnvironment

