{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module ATrade.Quotes.QTIS
(
  TickerInfo(..),
  qtisGetTickersInfo
) where

import           ATrade.Exceptions
import           ATrade.Logging         (Message, logInfo)
import           ATrade.Types
import           Colog                  (WithLog)
import           Control.Exception.Safe
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Data.Aeson
import qualified Data.ByteString.Char8  as BC8
import qualified Data.ByteString.Lazy   as BL
import qualified Data.Text              as T
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

qtisGetTickersInfo :: (MonadIO m) => Context -> T.Text -> TickerId -> m TickerInfo
qtisGetTickersInfo ctx endpoint tickerId = do
  liftIO $ withSocket ctx Req $ \sock -> do
    connect sock $ T.unpack endpoint
    send sock [] $ BL.toStrict tickerRequest
    response <- receiveMulti sock
    let r = parseResponse response
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

