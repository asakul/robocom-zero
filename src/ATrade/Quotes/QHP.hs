{-# LANGUAGE OverloadedStrings #-}

module ATrade.Quotes.QHP (
  getQuotes,
  Period(..),
  RequestParams(..)
  ) where

import           ATrade.Types
import           Data.Aeson
import           Data.Binary.Get
import           Data.Binary.IEEE754
import qualified Data.ByteString.Lazy  as BL
import qualified Data.Text             as T
import           Data.Time.Calendar
import           Data.Time.Clock.POSIX
import           System.Log.Logger
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

data RequestParams =
  RequestParams
    {
    endpoint  :: T.Text,
    ticker    :: T.Text,
    startDate :: Day,
    endDate   :: Day,
    period    :: Period
    } deriving (Show, Eq)

instance ToJSON RequestParams where
  toJSON p = object [ "ticker" .= ticker p,
    "from" .= showGregorian (startDate p),
    "to" .=  showGregorian (endDate p),
    "timeframe" .= show (period p) ]

getQuotes :: Context -> RequestParams -> IO [Bar]
getQuotes ctx params =
  withSocket ctx Req $ \sock -> do
    debugM "QHP" $ "Connecting to ep: " ++ show (endpoint params)
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
