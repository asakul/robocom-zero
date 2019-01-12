{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module ATrade.Driver.Backtest (
  backtestMain
) where

import           ATrade.Driver.Real.Types (InitializationCallback,
                                           Strategy (..),
                                           StrategyInstanceParams (..))
import           ATrade.Exceptions
import           ATrade.Quotes.Finam      as QF
import           ATrade.RoboCom.Monad     (Event (..), EventCallback,
                                           StrategyAction (..),
                                           StrategyEnvironment (..),
                                           runStrategyElement)
import           ATrade.RoboCom.Positions
import           ATrade.RoboCom.Types     (BarSeries (..), Ticker (..),
                                           Timeframe (..))
import           ATrade.Types
import           Conduit                  (awaitForever, runConduit, yield,
                                           (.|))
import           Control.Exception.Safe
import           Control.Monad.ST         (runST)
import           Control.Monad.State
import           Data.Aeson               (FromJSON (..), Result (..),
                                           Value (..), decode)
import           Data.Aeson.Types         (parseMaybe)
import           Data.ByteString.Lazy     (readFile, toStrict)
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
      print $ cash finalState
      print $ tradesLog finalState
      forM_ (reverse . logs $ finalState) putStrLn

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
      env <- gets strategyEnvironment
      let oldTimestamp = seLastTimestamp env
      let newTimestamp = barTimestamp bar
      let newenv = env { seBars = updateBars (seBars env) bar, seLastTimestamp = newTimestamp }
      curState <- gets robotState
      modify' (\s -> s { strategyEnvironment = newenv })
      handleEvents [NewBar bar])

    handleEvents events = do
      newActions <- mapM handleEvent events
      newEvents <- executeActions (concat newActions)
      unless (null newEvents) $ handleEvents newEvents

    executeActions actions = concat <$> mapM executeAction actions

    executeAction (ActionOrder order) = do
      oid <- nextOrderId
      let submittedOrder = order { orderState = Submitted, orderId = oid }
      modify' (\s -> s { pendingOrders = submittedOrder : pendingOrders s })
      return [OrderSubmitted submittedOrder]

    executeAction (ActionCancelOrder oid) = do
      mbOrder <- find (\o -> orderId o == oid && orderState o == Submitted) <$> gets pendingOrders
      case mbOrder of
        Just _ -> do
          modify' (\s -> s { pendingOrders = filter (\o -> orderId o == oid) (pendingOrders s)})
          return [OrderUpdate oid Cancelled]
        _ -> return []

    executeAction (ActionLog t) = modify' (\s -> s { logs = t : logs s }) >> return []
    executeAction (ActionSetupTimer t) = modify' (\s -> s { pendingTimers = t : pendingTimers s }) >> return []
    executeAction (ActionIO _ _) = return []

    executePendingOrders bar = do
      ev1 <- executeMarketOrders bar
      ev2 <- executeLimitOrders bar
      return $ ev1 ++ ev2

    executeLimitOrders bar = do
      (limitOrders, otherOrders) <- partition
        (\o -> case orderPrice o of
          Limit _ -> True
          _       -> False) <$> gets pendingOrders
      let (executableOrders, otherOrders) = partition (isExecutable bar) limitOrders
      modify' (\s -> s { pendingOrders = otherOrders } )
      forM executableOrders $ \order ->
        order `executeAtPrice` priceForLimitOrder order bar

    isExecutable bar order = case orderPrice order of
      Limit price -> if orderOperation order == Buy
                        then price <= barLow bar
                        else price >= barHigh bar
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
      (marketOrders, otherOrders) <- partition (\o -> orderPrice o == Market) <$> gets pendingOrders
      modify' (\s -> s { pendingOrders = otherOrders })
      forM marketOrders $ \order ->
        order `executeAtPrice` barOpen bar

    executeAtPrice order price = do
      ts <- seLastTimestamp <$> gets strategyEnvironment
      modify' (\s -> s { tradesLog = mkTrade order price ts : tradesLog s })
      return $ OrderUpdate (orderId order) Executed

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
      events <- executePendingOrders bar
      firedTimers <- fireTimers (barTimestamp bar)
      actions <- concat <$> mapM handleEvent (events ++ map TimerFired firedTimers)
      actions' <- handleEvent' event
      return $ actions ++ actions'

    handleEvent event = handleEvent' event

    handleEvent' event = do
      env <- gets strategyEnvironment
      params <- gets robotParams
      curState <- gets robotState
      let (newState, actions, _) = runStrategyElement params curState env $ callback event
      modify' (\s -> s { robotState = newState } )
      return actions

    updateBars barMap newbar = M.alter (\case
      Nothing -> Just BarSeries { bsTickerId = barSecurity newbar,
        bsTimeframe = Timeframe 60,
        bsBars = [newbar] }
      Just bs -> Just bs { bsBars = newbar : bsBars bs }) (barSecurity newbar) barMap

    fireTimers ts = do
      (firedTimers, otherTimers) <- partition (< ts) <$> gets pendingTimers
      modify' (\s -> s { pendingTimers = otherTimers })
      return firedTimers

    loadFeeds :: [Feed] -> IO (V.Vector [Bar])
    loadFeeds feeds = V.fromList <$> mapM loadFeed feeds
    loadFeed (Feed tid path) = do
      content <- readFile path
      case QF.parseQuotes $ toStrict content of
        Just quotes -> return $ fmap (rowToBar tid) quotes
        _           -> throw $ UnableToLoadFeed (T.pack path)

    rowToBar tid r = Bar tid (rowTime r) (rowOpen r) (rowHigh r) (rowLow r) (rowClose r) (rowVolume r)

    nextOrderId = do
      oid <- gets orderIdCounter
      modify' (\s -> s { orderIdCounter = oid + 1 })
      return oid


data BacktestState s c = BacktestState {
  cash                :: Double,
  robotState          :: s,
  robotParams         :: c,
  strategyEnvironment :: StrategyEnvironment,
  pendingOrders       :: [Order],
  tradesLog           :: [Trade],
  orderIdCounter      :: Integer,
  pendingTimers       :: [UTCTime],
  logs                :: [T.Text]
}

defaultBacktestState s c tickerList = BacktestState 0 s c (StrategyEnvironment "" "" 1 tickers (UTCTime (fromGregorian 1970 1 1) 0)) [] [] 1 [] []
  where
    tickers = M.fromList $ map (\x -> (code x, BarSeries (code x) (Timeframe (timeframeSeconds x)) [])) tickerList

newtype BacktestingMonad s c a = BacktestingMonad { unBacktestingMonad :: State (BacktestState s c) a }
  deriving (Functor, Applicative, Monad, MonadState (BacktestState s c))

