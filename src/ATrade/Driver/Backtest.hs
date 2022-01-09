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

import           ATrade.Driver.Junction.Types (StrategyDescriptor (StrategyDescriptor),
                                               StrategyDescriptorE (StrategyDescriptorE),
                                               TickerConfig, confStrategy,
                                               confTickers, eventCallback,
                                               strategyBaseName, tickerId,
                                               timeframe)
import           ATrade.Exceptions            (RoboComException (UnableToLoadConfig, UnableToLoadFeed))
import           ATrade.Logging               (Message, Severity (Error, Trace),
                                               fmtMessage, logWith)
import           ATrade.Quotes.QTIS           (TickerInfo (tiLotSize, tiTickSize),
                                               qtisGetTickersInfo)
import           ATrade.RoboCom.ConfigStorage (ConfigStorage (loadConfig))
import           ATrade.RoboCom.Monad         (Event (..), MonadRobot (..),
                                               StrategyEnvironment (..),
                                               appendToLog, seLastTimestamp)
import           ATrade.RoboCom.Types         (BarSeries (..),
                                               BarSeriesId (BarSeriesId), Bars,
                                               InstrumentParameters (InstrumentParameters),
                                               Ticker (..))
import           ATrade.Types                 (Bar (Bar, barHigh, barLow, barOpen, barSecurity, barTimestamp),
                                               BarTimeframe (BarTimeframe),
                                               Operation (Buy),
                                               Order (orderAccountId, orderId, orderOperation, orderPrice, orderQuantity, orderSecurity, orderSignalId),
                                               OrderId,
                                               OrderPrice (Limit, Market),
                                               OrderState (Cancelled, Executed, Submitted),
                                               Price, TickerId, Trade (..),
                                               fromDouble)
import           Colog                        (LogAction, (>$<))
import           Colog.Actions                (logTextStdout)
import           Conduit                      (ConduitT, Void, awaitForever,
                                               runConduit, yield, (.|))
import           Control.Exception.Safe       (catchAny, throw)
import           Control.Lens                 (makeLenses, use, (%=), (+=),
                                               (.=), (^.))
import           Control.Monad.ST             (runST)
import           Control.Monad.State          (MonadIO, MonadPlus (mzero),
                                               MonadState, MonadTrans (lift),
                                               State, StateT (StateT),
                                               execState, forM_, gets, when)
import           Data.Aeson                   (FromJSON (..), Value (..),
                                               decode)
import           Data.Aeson.Types             (parseMaybe)
import qualified Data.ByteString              as B
import qualified Data.ByteString.Char8        as B8
import           Data.ByteString.Lazy         (readFile, toStrict)
import qualified Data.ByteString.Lazy         as BL
import           Data.Csv                     (FromField (parseField),
                                               FromRecord (parseRecord),
                                               HasHeader (HasHeader), (.!))
import qualified Data.Csv                     as Csv
import           Data.Default                 (Default (def))
import           Data.HashMap.Strict          (lookup)
import           Data.IORef                   (newIORef)
import           Data.List                    (partition)
import qualified Data.List                    as L
import           Data.List.NonEmpty           (NonEmpty ((:|)))
import           Data.List.Split              (splitOn)
import qualified Data.Map.Strict              as M
import           Data.Sequence                (Seq (..), (<|), (|>))
import qualified Data.Sequence                as Seq
import           Data.STRef                   (newSTRef, readSTRef, writeSTRef)
import qualified Data.Text                    as T
import           Data.Text.Encoding           (decodeUtf8)
import           Data.Text.IO                 (putStrLn)
import qualified Data.Text.Lazy               as TL
import           Data.Time                    (defaultTimeLocale, parseTimeM)
import           Data.Time.Calendar           (fromGregorian)
import           Data.Time.Clock              (UTCTime (..), addUTCTime)
import           Data.Vector                  ((!), (!?), (//))
import qualified Data.Vector                  as V
import           Dhall                        (FromDhall, auto, input)
import           Options.Applicative          (Alternative (some), Parser,
                                               ReadM, eitherReader, execParser,
                                               fullDesc, header, helper, info,
                                               long, metavar, option, short,
                                               strOption)
import           Prelude                      hiding (log, lookup, putStrLn,
                                               readFile)
import           Safe                         (headMay)
import           System.IO                    (IOMode (ReadMode), withFile)
import           System.ZMQ4                  (withContext)

data Feed = Feed TickerId FilePath
  deriving (Show, Eq)

data Params = Params {
  strategyBasename   :: String,
  strategyConfigFile :: FilePath,
  qtisEndpoint       :: String,
  paramsFeeds        :: [Feed]
} deriving (Show, Eq)

data BacktestState c s = BacktestState {
  _descriptor          :: StrategyDescriptor c s,
  _cash                :: Double,
  _robotState          :: s,
  _robotParams         :: c,
  _strategyEnvironment :: StrategyEnvironment,
  _pendingOrders       :: [Order],
  _pendingEvents       :: Seq Event,
  _tradesLog           :: [Trade],
  _orderIdCounter      :: Integer,
  _pendingTimers       :: [UTCTime],
  _logs                :: [T.Text],
  _barsMap             :: M.Map BarSeriesId BarSeries,
  _availableTickers    :: NonEmpty BarSeriesId
}

makeLenses ''BacktestState

data Row = Row {
  rowTicker    :: T.Text,
  rowTimeframe :: Int,
  rowTime      :: UTCTime,
  rowOpen      :: Price,
  rowHigh      :: Price,
  rowLow       :: Price,
  rowClose     :: Price,
  rowVolume    :: Integer
} deriving (Show, Eq)

instance FromField Price where
  parseField s = fromDouble <$> (parseField s :: Csv.Parser Double)

instance FromRecord Row where
  parseRecord v
    | length v == 9 = do
      tkr <- v .! 0
      tf <- v .! 1
      date <- v .! 2
      time <- v .! 3
      dt <- addUTCTime (-3 * 3600) <$> parseDt date time
      open <- v .! 4
      high <- v .! 5
      low <- v .! 6
      close <- v .! 7
      vol <- v .! 8
      return $ Row tkr tf dt open high low close vol
    | otherwise     = mzero
    where
      parseDt :: B.ByteString -> B.ByteString -> Csv.Parser UTCTime
      parseDt d t = case parseTimeM True defaultTimeLocale "%Y%m%d %H%M%S" $ B8.unpack d ++ " " ++ B8.unpack t of
        Just dt -> return dt
        Nothing -> fail "Unable to parse date/time"

parseQuotes :: B.ByteString -> Maybe [Row]
parseQuotes csvData = case Csv.decode HasHeader $ BL.fromStrict csvData of
  Left _  -> Nothing
  Right d -> Just $ V.toList d

paramsParser :: Parser Params
paramsParser = Params
  <$> strOption (
      long "strategy-name" <> short 'n')
  <*> strOption (
      long "config" <> short 'c')
  <*> strOption
    ( long "qtis" <> short 'q' <> metavar "ENDPOINT/ID")
  <*> some (option feedArgParser (
      long "feed" <> short 'f'))

feedArgParser :: ReadM Feed
feedArgParser = eitherReader (\s -> case splitOn ":" s of
  [tid, fpath] -> Right $ Feed (T.pack tid) fpath
  _            -> Left $ "Unable to parse feed id: " ++ s)

logger :: (MonadIO m) => LogAction m Message
logger = fmtMessage >$< logTextStdout

backtestMain :: M.Map T.Text StrategyDescriptorE -> IO ()
backtestMain descriptors = do
  params <- execParser opts
  let log = logWith logger
  let strategyName = T.pack $ strategyBasename params

  feeds <- loadFeeds (paramsFeeds params)

  case M.lookup strategyName descriptors of
    Just (StrategyDescriptorE desc) -> flip catchAny (\e -> log Error "Backtest" $ "Exception: " <> (T.pack . show $ e)) $
      runBacktestDriver desc feeds params
    Nothing -> log Error "Backtest" $ "Can't find strategy: " <> strategyName
  where
    opts = info (helper <*> paramsParser)
      ( fullDesc <> header "ATrade strategy backtesting framework" )

    makeBars :: T.Text -> [TickerConfig] -> IO (M.Map BarSeriesId BarSeries)
    makeBars qtisEp confs =
      withContext $ \ctx ->
        M.fromList <$> mapM (mkBarEntry ctx qtisEp) confs

    mkBarEntry ctx qtisEp conf = do
      info <- qtisGetTickersInfo ctx qtisEp (tickerId conf)
      return (BarSeriesId (tickerId conf) (timeframe conf),
              BarSeries
                (tickerId conf)
                (timeframe conf)
                []
                (InstrumentParameters (tickerId conf) (fromInteger $ tiLotSize info) (tiTickSize info)))

    runBacktestDriver desc feeds params = do
      bigConf <- loadConfig (T.pack $ strategyConfigFile params)
      case confTickers bigConf of
        tickerList@(firstTicker:restTickers) -> do
          bars <- makeBars (T.pack $ qtisEndpoint params) tickerList
          let s = runConduit $ barStreamFromFeeds feeds .| backtestLoop desc
          let finalState =
                execState (unBacktestingMonad s) $ defaultBacktestState def (confStrategy bigConf) desc bars (fmap toBarSeriesId (firstTicker :| restTickers))
          print $ finalState ^. cash
          print $ finalState ^. tradesLog
          forM_ (reverse $ finalState ^. logs) putStrLn
        _ -> return ()

    toBarSeriesId conf = BarSeriesId (tickerId conf) (timeframe conf)

    barStreamFromFeeds :: (Monad m) => V.Vector (BarTimeframe, [Bar]) -> ConduitT () (BarSeriesId, Bar) m ()
    barStreamFromFeeds feeds = case nextBar feeds of
      Just (tf, bar, feeds') -> yield (BarSeriesId (barSecurity bar) tf, bar) >> barStreamFromFeeds feeds'
      _                  -> return ()

    nextBar :: V.Vector (BarTimeframe, [Bar]) -> Maybe (BarTimeframe, Bar, V.Vector (BarTimeframe, [Bar]))
    nextBar feeds = case indexOfNextFeed feeds of
      Just ix -> do
        (tf, f) <- feeds !? ix
        h <- headMay f
        return (tf, h, feeds // [(ix, (tf, tail f))])
      _ -> Nothing

    indexOfNextFeed feeds = runST $ do
      minTs <- newSTRef Nothing
      minIx <- newSTRef Nothing
      forM_ [0..(V.length feeds-1)] (\ix -> do
        let (_, feed) = feeds ! ix
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

    backtestLoop :: StrategyDescriptor c s -> ConduitT (BarSeriesId, Bar) Void (BacktestingMonad c s) ()
    backtestLoop desc =
      awaitForever (\(bsId, bar) -> do
      _curState <- use robotState
      _env <- gets _strategyEnvironment
      let newTimestamp = barTimestamp bar
      barsMap %= updateBars bsId bar
      strategyEnvironment . seLastTimestamp .= newTimestamp
      enqueueEvent (NewBar (bsIdTf bsId, bar))
      lift (handleEvents desc))

    bsIdTf (BarSeriesId _ tf) = tf


handleEvents :: StrategyDescriptor c s -> BacktestingMonad c s ()
handleEvents desc = do
  events <- use pendingEvents
  case events of
    x :<| xs -> do
      pendingEvents .= xs
      handleEvent desc x
      handleEvents desc
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
  tradesLog %= (thisTrade :)
  pendingEvents %= (\s -> OrderUpdate (orderId order) Executed <| s)
  pendingEvents %= (\s -> NewTrade thisTrade <| s)

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

handleEvent :: StrategyDescriptor c s -> Event -> BacktestingMonad c s ()
handleEvent desc event@(NewBar (_, bar)) = do
  executePendingOrders bar
  handleEvents desc -- This should pass OrderUpdate events to the callback before NewBar events
  firedTimers <- fireTimers (barTimestamp bar)
  mapM_ (enqueueEvent . TimerFired) firedTimers
  handleEvent' desc event
  return ()

handleEvent desc event = handleEvent' desc event

handleEvent' desc event = eventCallback desc event

updateBars bsId newbar barMap = M.adjust (\bs -> bs { bsBars = newbar : bsBars bs }) bsId barMap

fireTimers ts = do
  (firedTimers, otherTimers) <- partition (< ts) <$> use pendingTimers
  pendingTimers .= otherTimers
  return firedTimers

loadFeeds :: [Feed] -> IO (V.Vector (BarTimeframe, [Bar]))
loadFeeds feeds = V.fromList <$> mapM loadFeed feeds
loadFeed (Feed tid path) = do
  content <- readFile path
  case parseQuotes $ toStrict content of
    Just quotes -> case headMay quotes of
      Just first -> return (BarTimeframe (rowTimeframe first), fmap (rowToBar tid) quotes)
      Nothing    -> throw $ UnableToLoadFeed (T.pack path)
    _           -> throw $ UnableToLoadFeed (T.pack path)

rowToBar tid r = Bar tid (rowTime r) (rowOpen r) (rowHigh r) (rowLow r) (rowClose r) (rowVolume r)

enqueueEvent :: MonadState (BacktestState c s) m => Event -> m ()
enqueueEvent event = pendingEvents %= (|> event)

defaultBacktestState :: s -> c -> StrategyDescriptor c s -> M.Map BarSeriesId BarSeries -> NonEmpty BarSeriesId -> BacktestState c s
defaultBacktestState s c desc = BacktestState desc 0 s c (StrategyEnvironment "" "" 1 (UTCTime (fromGregorian 1970 1 1) 0)) [] Seq.empty [] 1 [] []

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
    pendingOrders %= (orderWithId :)
    pendingEvents %= (\s -> s |> OrderUpdate oid Submitted)
    return oid
  cancelOrder oid = do
    orders <- use pendingOrders
    let (matchingOrders, otherOrders) = partition (\o -> orderId o == oid) orders
    case matchingOrders of
      [] -> return ()
      xs -> do
        mapM_ (\o -> pendingEvents %= (\s -> s |> OrderUpdate (orderId o) Cancelled)) xs
        pendingOrders .= otherOrders
  appendToLog _ txt = logs %= ((TL.toStrict txt) :)
  setupTimer time = pendingTimers %= (time :)
  enqueueIOAction _actionId _action = error "Backtesting io actions is not supported"
  getConfig = use robotParams
  getState = use robotState
  setState s = robotState .= s
  getEnvironment = use strategyEnvironment
  getTicker tid tf = do
    m <- gets _barsMap
    return $ M.lookup (BarSeriesId tid tf) m
  getTickerInfo tid = do
    tickers <- getAvailableTickers
    case L.find (\(BarSeriesId t _) -> t == tid) tickers of
      Just (BarSeriesId t tf) -> do
        ticker <- getTicker t tf
        return (bsParams <$> ticker)
      Nothing                 -> return Nothing
  getAvailableTickers = use availableTickers

instance ConfigStorage IO where
  loadConfig filepath = do
    cfg <- B.readFile $ T.unpack filepath
    input auto (decodeUtf8 cfg)

