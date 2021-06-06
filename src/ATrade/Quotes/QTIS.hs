{-# LANGUAGE OverloadedStrings #-}

module ATrade.Quotes.QTIS
(
  TickerInfo(..),
  qtisGetTickersInfo
) where

import           ATrade.Exceptions
import           ATrade.Types
import           Control.Exception.Safe
import           Data.Aeson
import qualified Data.ByteString.Char8  as BC8
import qualified Data.ByteString.Lazy   as BL
import qualified Data.Text              as T
import           System.Log.Logger
import           System.ZMQ4

data TickerInfo = TickerInfo {
  tiTicker   :: T.Text,
  tiLotSize  :: Integer,
  tiTickSize :: Price
} deriving (Show, Eq)

instance FromJSON TickerInfo where
  parseJSON = withObject "object" (\obj ->
    TickerInfo <$>
      obj .: "ticker" <*>
      obj .: "lot_size" <*>
      obj .: "tick_size")

instance ToJSON TickerInfo where
  toJSON ti = object [ "ticker" .= tiTicker ti,
    "lot_size" .= tiLotSize ti,
    "tick_size" .= tiTickSize ti ]

qtisGetTickersInfo :: Context -> T.Text -> TickerId -> IO TickerInfo
qtisGetTickersInfo ctx endpoint tickerId =
  withSocket ctx Req $ \sock -> do
    debugM "QTIS" $ "Connecting to: " ++ T.unpack endpoint
    connect sock $ T.unpack endpoint
    debugM "QTIS" $ "Requesting: " ++ T.unpack tickerId
    send sock [] $ BL.toStrict tickerRequest
    response <- receiveMulti sock
    let r = parseResponse response
    debugM "QTIS" $ "Got response: " ++ show r
    case r of
      Just resp -> return resp
      Nothing   -> throw $ QTISFailure "Can't parse response"
  where
    tickerRequest = encode $ object ["ticker" .= tickerId]
    parseResponse :: [BC8.ByteString] -> Maybe TickerInfo
    parseResponse (header:payload:_) = if header == "OK"
      then decode $ BL.fromStrict payload
      else Nothing
    parseResponse _ = Nothing

