{-# OPTIONS_GHC -Wno-type-defaults #-}

module ATrade.Forums.Smartlab (
  NewsItem(..),
  IndexItem(..),
  getIndex,
  getItem
) where

import qualified Data.ByteString.Lazy as BL
import qualified Data.List            as L
import           Data.Maybe
import qualified Data.Text            as T
import           Data.Text.Encoding
import           Data.Time.Calendar
import           Data.Time.Clock
import           Network.HTTP.Simple
import           Safe
import           Text.HTML.TagSoup
import           Text.Parsec
import           Text.Parsec.Text
import           Text.StringLike

import           Debug.Trace

data NewsItem = NewsItem {
  niUrl     :: !T.Text,
  niHeader  :: !T.Text,
  niText    :: !T.Text,
  niAuthor  :: !T.Text,
  niPubTime :: !UTCTime
} deriving (Show, Eq)

data IndexItem = IndexItem {
  iiUrl     :: !T.Text,
  iiTitle   :: !T.Text,
  iiPubTime :: !UTCTime
} deriving (Show, Eq)

monthNames :: [T.Text]
monthNames = fmap T.pack ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"]

extractBetween :: StringLike str => String -> [Tag str] -> [Tag str]
extractBetween tagName = takeWhile (~/= closeTag) . dropWhile (~/= openTag)
  where
    openTag = "<" ++ tagName ++ ">"
    closeTag = "</" ++ tagName ++ ">"

matchClass :: T.Text -> T.Text -> Tag T.Text -> Bool
matchClass _ className (TagOpen _ attrs) = case L.lookup (T.pack "class") attrs of
  Just klass -> className `L.elem` T.words klass
  Nothing    -> False

matchClass _ _ _ = False

parseTimestamp :: T.Text -> Maybe UTCTime
parseTimestamp text = case parse timestampParser "" text of
  Left _    -> Nothing
  Right val -> Just val
  where
    timestampParser :: Parser UTCTime
    timestampParser = do
      spaces
      day <- read <$> many1 digit
      spaces
      monthName <- T.pack <$> many1 letter
      case L.elemIndex monthName monthNames of
        Nothing -> fail "Can't parse month"
        Just month -> do
          spaces
          year <- fromIntegral . read <$> many1 digit
          _ <- char ','
          spaces
          hour <- fromIntegral . read <$> many1 digit
          _ <- char ':'
          minute <- fromIntegral . read <$> many1 digit
          return $ UTCTime (fromGregorian year (month + 1) day) (hour * 3600 + minute * 60)

getItem :: IndexItem -> IO (Maybe NewsItem)
getItem indexItem = do
  rq <- parseRequest $ T.unpack (iiUrl indexItem)
  resp <- httpLBS rq
  if getResponseStatusCode resp == 200
    then return . parseItem . decodeUtf8 . BL.toStrict . getResponseBody $ resp
    else return Nothing
  where
    parseItem rawHtml = case parseTimestamp timestamp of
      Just itemPubtime -> Just NewsItem {
        niUrl = iiUrl indexItem,
        niHeader = itemHeader,
        niText = itemText,
        niAuthor = itemAuthor,
        niPubTime = itemPubtime
      }
      Nothing -> Nothing
      where
        itemHeader = innerText .
          extractBetween "span" .
          extractBetween "h1" .
          dropWhile (not . matchClass (T.pack "div") (T.pack "topic")) $ tags

        itemText = innerText .
          extractBetween "div" .
          dropWhile (not . matchClass (T.pack "div") (T.pack "content")) .
          dropWhile (~/= "<div id=content_box>") $ tags

        itemAuthor = innerText .
          extractBetween "li" .
          dropWhile (not . matchClass (T.pack "li") (T.pack "author")) $ tags

        timestamp = traceShowId $ innerText .
          extractBetween "li" .
          dropWhile (not . matchClass (T.pack "li") (T.pack "date")) $ tags

        tags = parseTags rawHtml


getIndex :: T.Text -> Int -> IO ([IndexItem], Bool)
getIndex rootUrl pageNumber = do
  rq <- parseRequest $ T.unpack $ makeUrl rootUrl pageNumber
  resp <- httpLBS rq
  return $ if getResponseStatusCode resp == 200
    then parseIndex . decodeUtf8 . BL.toStrict . getResponseBody $ resp
    else ([], False)
  where
    parseIndex :: T.Text -> ([IndexItem], Bool)
    parseIndex x = (mapMaybe parseIndexEntry $ partitions (matchClass (T.pack "div") (T.pack "topic")) $ parseTags x, hasNextPage $ parseTags x)

    parseIndexEntry :: [Tag T.Text] -> Maybe IndexItem
    parseIndexEntry divTag = do
      a <- headMay . dropWhile (~/= "<a>") $ divTag
      let text = innerText . takeWhile (~/= "</a>") . dropWhile (~/= "<a>") $ divTag
      case a of
        TagOpen _ attr -> do
          href <- L.lookup (T.pack "href") attr
          ts <- parseTimestamp (innerText $ takeWhile (~/= "</li>") . dropWhile (not . matchClass (T.pack "li") (T.pack "date")) $ divTag)
          Just IndexItem { iiUrl = href,
            iiTitle = text,
            iiPubTime = ts }
        _ -> Nothing


    makeUrl root pagenumber
      | pagenumber == 0 || pagenumber == 1 = root
      | otherwise = root `T.append` (T.pack "/page") `T.append` T.pack (show pagenumber)

    hasNextPage tags = if pageNumber <= 1
      then paginationLinksCount > 0
      else paginationLinksCount > 1
      where
        paginationLinksCount = length . filter (~== "<a>") . extractBetween "p" . dropWhile (~/= "<div id=pagination>") $ tags


