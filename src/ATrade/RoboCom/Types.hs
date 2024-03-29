{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeSynonymInstances #-}

module ATrade.RoboCom.Types (
  Bar(..),
  BarSeriesId(..),
  BarSeries(..),
  Ticker(..),
  Bars,
  TickerInfoMap,
  InstrumentParameters(..),
  bsidTickerId,
  barSeriesId
) where

import           ATrade.Types
import           Data.Aeson
import           Data.Aeson.Types
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict     as M
import qualified Data.Text           as T
import           GHC.Generics        (Generic)


data InstrumentParameters =
  InstrumentParameters {
    ipTickerId :: TickerId,
    ipLotSize  :: Int,
    ipTickSize :: Price
  } deriving (Show, Eq)

type TickerInfoMap = M.Map TickerId InstrumentParameters

data BarSeries =
  BarSeries {
    bsTickerId  :: TickerId,
    bsTimeframe :: BarTimeframe,
    bsBars      :: [Bar],
    bsParams    :: InstrumentParameters
  } deriving (Show, Eq)

barSeriesId :: BarSeries -> BarSeriesId
barSeriesId s = BarSeriesId (bsTickerId s) (bsTimeframe s)

-- | Ticker description record
data Ticker = Ticker {
  code             :: T.Text, -- ^ Main ticker code, which is used to make orders and tick parsing
  aliases          :: [(String, String)], -- ^ List of aliases for this tick in the form ("alias-name", "alias").
                                 -- This is needed when other data providers use different codcodes for the same tick.
                                 -- For now, only "finam" alias is used
  timeframeSeconds :: Integer -- ^ Data timeframe. Will be used by 'BarAggregator'
} deriving (Show)

instance FromJSON Ticker where
  parseJSON = withObject "object" (\obj -> do
    nm <- obj .: "name"
    als <- obj .: "aliases"
    als' <- parseAliases als
    tf <- obj .: "timeframe"
    return $ Ticker nm als' tf)
    where
      parseAliases :: Value -> Parser [(String, String)]
      parseAliases = withObject "object1" (mapM parseAlias . HM.toList)
      parseAlias :: (T.Text, Value) -> Parser (String, String)
      parseAlias (k, v) = withText "string1" (\s -> return (T.unpack k, T.unpack s)) v

instance ToJSON Ticker where
  toJSON t = object [ "name" .= code t,
    "timeframe" .= timeframeSeconds t,
    "aliases" .= Object (HM.fromList $ fmap (\(x, y) -> (T.pack x, String $ T.pack y)) $ aliases t) ]

data BarSeriesId = BarSeriesId TickerId BarTimeframe
  deriving (Show, Eq, Generic, Ord)

bsidTickerId :: BarSeriesId -> TickerId
bsidTickerId (BarSeriesId tid _) = tid

type Bars = M.Map BarSeriesId BarSeries

