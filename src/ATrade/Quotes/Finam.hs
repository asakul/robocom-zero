{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TypeSynonymInstances #-}

module ATrade.Quotes.Finam (
  downloadFinamSymbols,
  Symbol(..),
  Period(..),
  DateFormat(..),
  TimeFormat(..),
  FieldSeparator(..),
  RequestParams(..),
  defaultParams,
  downloadQuotes,
  parseQuotes,
  downloadAndParseQuotes,
  Row(..)
) where

import           ATrade.Types
import           Control.Error.Util
import           Control.Exception
import           Control.Lens
import           Control.Monad
import qualified Data.ByteString                      as B
import qualified Data.ByteString.Char8                as B8
import qualified Data.ByteString.Lazy                 as BL
import           Data.Csv                             hiding (Options)
import           Data.List
import qualified Data.Map                             as M
import           Data.Maybe
import qualified Data.Text                            as T
import qualified Data.Text.ICU.Convert                as TC
import           Data.Time.Calendar
import           Data.Time.Clock
import           Data.Time.Format
import qualified Data.Vector                          as V
import           Network.Wreq
import           Safe
import           System.Log.Logger
import           Text.Parsec
import           Text.ParserCombinators.Parsec.Number

data Period =
  PeriodTick  |
  Period1Min  |
  Period5Min  |
  Period10Min |
  Period15Min |
  Period30Min |
  PeriodHour  |
  PeriodDay   |
  PeriodWeek  |
  PeriodMonth
  deriving (Show, Eq)

instance Enum Period where
  fromEnum PeriodTick  = 1
  fromEnum Period1Min  = 2
  fromEnum Period5Min  = 3
  fromEnum Period10Min = 4
  fromEnum Period15Min = 5
  fromEnum Period30Min = 6
  fromEnum PeriodHour  = 7
  fromEnum PeriodDay   = 8
  fromEnum PeriodWeek  = 9
  fromEnum PeriodMonth = 10

  toEnum 1  = PeriodTick
  toEnum 2  = Period1Min
  toEnum 3  = Period5Min
  toEnum 4  = Period10Min
  toEnum 5  = Period15Min
  toEnum 6  = Period30Min
  toEnum 7  = PeriodHour
  toEnum 8  = PeriodDay
  toEnum 9  = PeriodWeek
  toEnum 10 = PeriodMonth
  toEnum _  = PeriodDay

data DateFormat =
  FormatYYYYMMDD |
  FormatYYMMDD   |
  FormatDDMMYY   |
  FormatDD_MM_YY |
  FormatMM_DD_YY
  deriving (Show, Eq)

instance Enum DateFormat where
  fromEnum FormatYYYYMMDD = 1
  fromEnum FormatYYMMDD   = 2
  fromEnum FormatDDMMYY   = 3
  fromEnum FormatDD_MM_YY = 4
  fromEnum FormatMM_DD_YY = 5

  toEnum 1 = FormatYYYYMMDD
  toEnum 2 = FormatYYMMDD
  toEnum 3 = FormatDDMMYY
  toEnum 4 = FormatDD_MM_YY
  toEnum 5 = FormatMM_DD_YY
  toEnum _ = FormatYYYYMMDD


data TimeFormat =
  FormatHHMMSS   |
  FormatHHMM     |
  FormatHH_MM_SS |
  FormatHH_MM
  deriving (Show, Eq)

instance Enum TimeFormat where
  fromEnum FormatHHMMSS   = 1
  fromEnum FormatHHMM     = 2
  fromEnum FormatHH_MM_SS = 3
  fromEnum FormatHH_MM    = 4

  toEnum 1 = FormatHHMMSS
  toEnum 2 = FormatHHMM
  toEnum 3 = FormatHH_MM_SS
  toEnum 4 = FormatHH_MM
  toEnum _ = FormatHHMMSS

data FieldSeparator =
  SeparatorComma      |
  SeparatorPeriod     |
  SeparatorSemicolon  |
  SeparatorTab        |
  SeparatorSpace
  deriving (Show, Eq)

instance Enum FieldSeparator where
  fromEnum SeparatorComma     = 1
  fromEnum SeparatorPeriod    = 2
  fromEnum SeparatorSemicolon = 3
  fromEnum SeparatorTab       = 4
  fromEnum SeparatorSpace     = 5

  toEnum 1 = SeparatorComma
  toEnum 2 = SeparatorPeriod
  toEnum 3 = SeparatorSemicolon
  toEnum 4 = SeparatorTab
  toEnum 5 = SeparatorSpace
  toEnum _ = SeparatorComma

data RequestParams = RequestParams {
  ticker         :: T.Text,
  startDate      :: Day,
  endDate        :: Day,
  period         :: Period,
  dateFormat     :: DateFormat,
  timeFormat     :: TimeFormat,
  fieldSeparator :: FieldSeparator,
  includeHeader  :: Bool,
  fillEmpty      :: Bool
}

defaultParams :: RequestParams
defaultParams = RequestParams {
  ticker = "",
  startDate = fromGregorian 1970 1 1,
  endDate = fromGregorian 1970 1 1,
  period = PeriodDay,
  dateFormat = FormatYYYYMMDD,
  timeFormat = FormatHHMMSS,
  fieldSeparator = SeparatorComma,
  includeHeader = True,
  fillEmpty = False
}

data Symbol = Symbol {
  symCode       :: T.Text,
  symName       :: T.Text,
  symId         :: Integer,
  symMarketCode :: Integer,
  symMarketName :: T.Text
}
  deriving (Show, Eq)

data Row = Row {
  rowTicker :: T.Text,
  rowTime   :: UTCTime,
  rowOpen   :: Price,
  rowHigh   :: Price,
  rowLow    :: Price,
  rowClose  :: Price,
  rowVolume :: Integer
} deriving (Show, Eq)

instance FromField Price where
  parseField s = fromDouble <$> (parseField s :: Parser Double)

instance FromRecord Row where
  parseRecord v
    | length v == 9 = do
      tkr <- v .! 0
      date <- v .! 2
      time <- v .! 3
      dt <- addUTCTime (-3 * 3600) <$> (parseDt date time)
      open <- v .! 4
      high <- v .! 5
      low <- v .! 6
      close <- v .! 7
      volume <- v .! 8
      return $ Row tkr dt open high low close volume
    | otherwise     = mzero
    where
      parseDt :: B.ByteString -> B.ByteString -> Parser UTCTime
      parseDt d t = case parseTimeM True defaultTimeLocale "%Y%m%d %H%M%S" $ B8.unpack d ++ " " ++ B8.unpack t of
        Just dt -> return dt
        Nothing -> fail "Unable to parse date/time"

downloadAndParseQuotes :: RequestParams -> IO (Maybe [Row])
downloadAndParseQuotes requestParams = downloadAndParseQuotes' 3
  where
    downloadAndParseQuotes' iter = do
      raw <- downloadQuotes requestParams `catch` (\e -> do
        debugM "History" $ "exception: " ++ show (e :: SomeException)
        return Nothing)
      case raw of
        Just r -> return $ parseQuotes r
        Nothing -> if iter <= 0 then return Nothing else downloadAndParseQuotes' (iter - 1)

parseQuotes :: B.ByteString -> Maybe [Row]
parseQuotes csvData = case decode HasHeader $ BL.fromStrict csvData of
  Left _  -> Nothing
  Right d -> Just $ V.toList d

downloadQuotes :: RequestParams -> IO (Maybe B.ByteString)
downloadQuotes requestParams =  do
  symbols <- downloadFinamSymbols
  case requestUrl symbols requestParams of
    Just (url, options') -> do
      resp <- getWith options' url
      return $ Just $ BL.toStrict $ resp ^. responseBody
    Nothing -> return Nothing

requestUrl :: [Symbol] -> RequestParams -> Maybe (String, Options)
requestUrl symbols requestParams = case getFinamCode symbols (ticker requestParams) of
  Just (sym, market) -> Just ("http://export.finam.ru/export9.out", getOptions sym market)
  Nothing -> Nothing
  where
    getOptions sym market = defaults &
      param "market" .~ [T.pack . show $ market] &
      param "f" .~ [ticker requestParams] &
      param "e" .~ [".csv"] &
      param "dtf" .~ [T.pack . show . fromEnum . dateFormat $ requestParams] &
      param "tmf" .~ [T.pack . show . fromEnum . dateFormat $ requestParams] &
      param "MSOR" .~ ["0"] &
      param "mstime" .~ ["on"] &
      param "mstimever" .~ ["1"] &
      param "sep" .~ [T.pack . show . fromEnum . fieldSeparator $ requestParams] &
      param "sep2" .~ ["1"] &
      param "at" .~ [if includeHeader requestParams then "1" else "0"] &
      param "fsp" .~ [if fillEmpty requestParams then "1" else "0"] &
      param "p" .~ [T.pack . show . fromEnum $ period requestParams] &
      param "em" .~ [T.pack . show $ sym ] &
      param "df" .~ [T.pack . show $ dayFrom] &
      param "mf" .~ [T.pack . show $ (monthFrom - 1)] &
      param "yf" .~ [T.pack . show $ yearFrom] &
      param "dt" .~ [T.pack . show $ dayTo] &
      param "mt" .~ [T.pack . show $ (monthTo - 1)] &
      param "yt" .~ [T.pack . show $ yearTo] &
      param "code" .~ [ticker requestParams] &
      param "datf" .~ if period requestParams == PeriodTick then ["11"] else ["1"]
    (yearFrom, monthFrom, dayFrom) = toGregorian $ startDate requestParams
    (yearTo, monthTo, dayTo) = toGregorian $ endDate requestParams

getFinamCode :: [Symbol] -> T.Text -> Maybe (Integer, Integer)
getFinamCode symbols tickerCode = case find (\x -> symCode x == tickerCode && symMarketCode x `notElem` archives) symbols of
  Just sym -> Just (symId sym, symMarketCode sym)
  Nothing  -> Nothing

downloadFinamSymbols :: IO [Symbol]
downloadFinamSymbols = do
  conv <- TC.open "cp1251" Nothing
  result <- get "http://www.finam.ru/cache/icharts/icharts.js"
  if result ^. responseStatus . statusCode == 200
    then return $ parseSymbols . T.lines $ TC.toUnicode conv $ BL.toStrict $ result ^. responseBody
    else return []
  where
    parseSymbols :: [T.Text] -> [Symbol]
    parseSymbols strs = zipWith5 Symbol codes names ids marketCodes marketNames
      where
        getWithParser parser pos = fromMaybe [] $ do
          s <- T.unpack <$> strs `atMay` pos
          hush $ parse parser "" s

        ids :: [Integer]
        ids = getWithParser intlist 0

        names :: [T.Text]
        names = T.pack <$> getWithParser strlist 1

        codes :: [T.Text]
        codes = T.pack <$> getWithParser strlist 2

        marketCodes :: [Integer]
        marketCodes = getWithParser intlist 3

        marketNames :: [T.Text]
        marketNames = fmap (\code -> fromMaybe "" $ M.lookup code codeToName) marketCodes

        intlist = do
          _ <- string "var"
          spaces
          skipMany1 alphaNum
          spaces
          _ <- char '='
          spaces
          _ <- char '['
          manyTill (do
            i <- int
            _ <- char ',' <|> char ']'
            return i) (char '\'' <|> char ';')

        strlist = do
          _ <- string "var"
          spaces
          skipMany1 alphaNum
          spaces
          _ <- char '='
          spaces
          _ <- char '['
          (char '\'' >> manyTill ((char '\\' >> char '\'') <|> anyChar) (char '\'')) `sepBy` char ','

codeToName :: M.Map Integer T.Text
codeToName = M.fromList [
  (200, "МосБиржа топ"),
  (1 , "МосБиржа акции"),
  (14 , "МосБиржа фьючерсы"),
  (41, "Курс рубля"),
  (45, "МосБиржа валютный рынок"),
  (2, "МосБиржа облигации"),
  (12, "МосБиржа внесписочные облигации"),
  (29, "МосБиржа пифы"),
  (8, "Расписки"),
  (6, "Мировые Индексы"),
  (24, "Товары"),
  (5, "Мировые валюты"),
  (25, "Акции США(BATS)"),
  (7, "Фьючерсы США"),
  (27, "Отрасли экономики США"),
  (26, "Гособлигации США"),
  (28, "ETF"),
  (30, "Индексы мировой экономики"),
  (3, "РТС"),
  (20, "RTS Board"),
  (10, "РТС-GAZ"),
  (17, "ФОРТС Архив"),
  (31, "Сырье Архив"),
  (38, "RTS Standard Архив"),
  (16, "ММВБ Архив"),
  (18, "РТС Архив"),
  (9, "СПФБ Архив"),
  (32, "РТС-BOARD Архив"),
  (39, "Расписки Архив"),
  (-1, "Отрасли") ]


archives :: [Integer]
archives = [3, 8, 16, 17, 18, 31, 32, 38, 39, 517]
