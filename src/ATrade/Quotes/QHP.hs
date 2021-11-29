{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module ATrade.Quotes.QHP (
  Period(..),
  RequestParams(..),
  QHPHandle,
  mkQHPHandle,
  requestHistoryFromQHP
  ) where

import           ATrade.Exceptions
import           ATrade.Logging          (Message, logInfo)
import           ATrade.Types
import           Colog                   (WithLog)
import           Control.Exception.Safe  (MonadThrow, throw)
import           Control.Monad.IO.Class  (MonadIO, liftIO)
import           Data.Aeson
import           Data.Binary.Get
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Text               as T
import           Data.Time.Calendar
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import           Data.Time.Format
import           Language.Haskell.Printf (t)
import           System.ZMQ4

data Period =
  Period1Min  |
  Period5Min  |
  Period15Min |
  Period30Min |
  PeriodHour  |
  PeriodDay   |
  PeriodWeek  |
  PeriodMonth
  deriving (Eq)

instance Show Period where
  show Period1Min  = "M1"
  show Period5Min  = "M5"
  show Period15Min = "M15"
  show Period30Min = "M30"
  show PeriodHour  = "H1"
  show PeriodDay   = "D"
  show PeriodWeek  = "W"
  show PeriodMonth = "MN"

data QHPHandle = QHPHandle
  {
      qhpContext  :: Context
    , qhpEndpoint :: T.Text
  }

mkQHPHandle :: Context -> T.Text -> QHPHandle
mkQHPHandle = QHPHandle

requestHistoryFromQHP :: (WithLog env Message m, MonadThrow m, MonadIO m) => QHPHandle -> TickerId -> BarTimeframe -> UTCTime -> UTCTime -> m [Bar]
requestHistoryFromQHP qhp tickerId timeframe fromTime toTime =
  case parseQHPPeriod (unBarTimeframe timeframe) of
    Just tf -> getQuotes (qhpContext qhp) (params tf)
    _       -> throw $ BadParams "QHP: Unable to parse timeframe"
  where
    params tf = RequestParams
      {
        endpoint = qhpEndpoint qhp,
        ticker = tickerId,
        startDate = utctDay fromTime,
        endDate = utctDay toTime,
        period = tf
      }

    parseQHPPeriod x
      | x == 60 = Just Period1Min
      | x == 5 * 60 = Just Period5Min
      | x == 15 * 60 = Just Period15Min
      | x == 30 * 60 = Just Period30Min
      | x == 60 * 60 = Just PeriodHour
      | x == 24 * 60 * 60 = Just PeriodDay
      | otherwise = Nothing

data RequestParams =
  RequestParams
    {
    endpoint  :: T.Text,
    ticker    :: T.Text,
    startDate :: Day,
    endDate   :: Day,
    period    :: Period
    } deriving (Show, Eq)

printDatetime :: UTCTime -> String
printDatetime = formatTime defaultTimeLocale (iso8601DateFormat (Just "%H:%M:%S"))

instance ToJSON RequestParams where
  toJSON p = object [ "ticker" .= ticker p,
    "from" .= printDatetime (UTCTime (startDate p) 0),
    "to" .=  printDatetime (UTCTime (endDate p) 0),
    "timeframe" .= show (period p) ]

getQuotes :: (WithLog env Message m, MonadIO m) => Context -> RequestParams -> m [Bar]
getQuotes ctx params = do
  logInfo "QHP" $ "Connecting to ep: " <> endpoint params
  liftIO $ withSocket ctx Req $ \sock -> do
    connect sock $ (T.unpack . endpoint) params
    send sock [] (BL.toStrict $ encode params)
    response <- receiveMulti sock
    case response of
      [header, rest] -> if header == "OK"
                          then return $ reverse $ parseBars (ticker params) $ BL.fromStrict rest
                          else return []
      _ -> return []

parseBars :: TickerId -> BL.ByteString -> [Bar]
parseBars tickerId input =
  case runGetOrFail parseBar input of
    Left _               -> []
    Right (rest, _, bar) -> bar : parseBars tickerId rest
  where
    parseBar = do
      rawTimestamp <- realToFrac <$> getWord64le
      baropen <- getDoublele
      barhigh <- getDoublele
      barlow <- getDoublele
      barclose <- getDoublele
      barvolume <- getWord64le
      return Bar
        {
          barSecurity = tickerId,
          barTimestamp = posixSecondsToUTCTime rawTimestamp,
          barOpen = fromDouble baropen,
          barHigh = fromDouble barhigh,
          barLow = fromDouble barlow,
          barClose = fromDouble barclose,
          barVolume = toInteger barvolume
        }
