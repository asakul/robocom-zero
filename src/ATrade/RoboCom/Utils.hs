{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE TypeSynonymInstances #-}

module ATrade.RoboCom.Utils (
  barStartTime,
  barEndTime,
  rescaleToDaily,
  barNumber,
  getHMS,
  getHMS',
  fromHMS,
  fromHMS',
  parseTime
) where

import           ATrade.Types

import qualified Data.Text          as T
import           Data.Time.Calendar
import           Data.Time.Clock

import           Data.Int           (Int64)
import           Text.Read          hiding (String)

rescaleToDaily :: [Bar] -> [Bar]
rescaleToDaily (firstBar:restBars) = rescaleToDaily' restBars firstBar
  where
    rescaleToDaily' (b:bars) currentBar =
      if (utctDay . barTimestamp) b == (utctDay . barTimestamp) currentBar
        then rescaleToDaily' bars $ currentBar { barOpen = barOpen b,
          barHigh = max (barHigh b) (barHigh currentBar),
          barLow = min (barLow b) (barLow currentBar),
          barVolume = barVolume currentBar + barVolume b}
        else currentBar : rescaleToDaily' bars b
    rescaleToDaily' [] currentBar = [currentBar]

rescaleToDaily [] = []

barEndTime :: Bar -> Int64 -> UTCTime
barEndTime bar tframe = addUTCTime (fromIntegral $ (1 + barNumber (barTimestamp bar) tframe) * tframe) epoch

barStartTime :: Bar -> Int64 -> UTCTime
barStartTime bar tframe = addUTCTime (fromIntegral $ barNumber (barTimestamp bar) tframe * tframe) epoch

barNumber :: UTCTime -> Int64 -> Int64
barNumber ts barlen = floor (diffUTCTime ts epoch) `div` barlen

epoch :: UTCTime
epoch = UTCTime (fromGregorian 1970 1 1) 0

-- | Helper function, converts 'UTCTime' to 3-tuple: (hours, minutes, seconds). Date part is discarded.
getHMS :: UTCTime -> (Int, Int, Int)
getHMS (UTCTime _ diff) = (intsec `div` 3600, (intsec `mod` 3600) `div` 60, intsec `mod` 60)
  where
    intsec = floor diff

-- | Helper function, converts 'UTCTime' to integer of the form "HHMMSS"
getHMS' :: UTCTime -> Int
getHMS' t = h * 10000 + m * 100 + s
  where
    (h, m, s) = getHMS t

fromHMS' :: Int -> DiffTime
fromHMS' hms = fromIntegral $ h * 3600 + m * 60 + s
  where
    h = hms `div` 10000
    m = (hms `mod` 10000) `div` 100
    s = (hms `mod` 100)

fromHMS :: Int -> Int -> Int -> DiffTime
fromHMS h m s = fromIntegral $ h * 3600 + m * 60 + s

parseTime :: T.Text -> Maybe DiffTime
parseTime x = case readMaybe (T.unpack x) of
  Just t -> let h = t `div` 10000
                m = (t `mod` 10000) `div` 100
                s = t `mod` 100
            in Just $ fromInteger $ h * 3600 + m * 60 + s
  Nothing -> Nothing
