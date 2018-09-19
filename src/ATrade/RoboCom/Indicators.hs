
module ATrade.RoboCom.Indicators
(
  cmf,
  cci,
  atr,
  rsi,
  highest,
  lowest,
  highestOf,
  lowestOf,
  sma,
  ema,
  intradayBarNumber,
  hVolumeAt,
  getMaxHVol,
  bbandUpper,
  percentRank
) where

import           ATrade.Types

import qualified Data.List       as L
import           Data.Time.Clock
import           Safe

cmf :: Int -> [Bar] -> Double
cmf period bars = sum (toDouble . clv <$> take period bars) / toDouble (sum (fromInteger . barVolume <$> bars))
  where
    clv bar = fromInteger (barVolume bar) * (barClose bar - barOpen bar) / (barHigh bar - barLow bar + 0.000001)

cci :: Int -> [Bar] -> Double
cci period bars = (head tp - tpMean) / (0.015 * meanDev)
  where
    meanDev = sma period diff
    diff = zipWith (\x y -> abs (x - y)) tp tpSma
    tpMean = sma period tp
    tpSma = fmap (sma period) $ take (2 * period) $ L.tails tp
    tp = zipWith3 typicalPrice (toDouble . barClose <$> bars) (toDouble . barHigh <$> bars) (toDouble . barLow <$> bars)
    typicalPrice a b c = (a + b + c) / 3

atr :: Int -> [Bar] -> Double
atr period bars = foldl (\x y -> (x * (period' - 1) + y) / period') 0 (reverse $ take (5 * period) trueranges)
  where
    trueranges :: [Double]
    trueranges = zipWith trueRange bars (tail bars)
    trueRange b1 b2 = toDouble $ maximum [ barHigh b1 - barLow b1, abs (barHigh b1 - barClose b2), abs (barLow b1 - barClose b2) ]
    period' = fromIntegral period

rsi :: Int -> [Double] -> Double
rsi period values = 100 - (100 / (1 + rs))
  where
    rs = if emaWithAlpha (1 / fromIntegral period) downbars /= 0 then emaWithAlpha (1 / fromIntegral period) upbars / emaWithAlpha (1 / fromIntegral period) downbars else 100000000
    upbars = (\(bar1,bar2) -> if bar1 < bar2 then bar2 - bar1 else 0) <$> zip (tail values) values
    downbars = (\(bar1,bar2) -> if bar1 > bar2 then bar1 - bar2 else 0) <$> zip (tail values) values

lastNValues :: Int -> (Bar -> Price) -> [Bar] -> [Double]
lastNValues period f bars = toDouble . f <$> take period bars

highest :: Int -> [Double] -> Maybe Double
highest period values = maximumMay $ take period values

lowest :: Int -> [Double] -> Maybe Double
lowest period values = minimumMay $ take period values

highestOf :: (Bar -> Price) -> Int -> [Bar] -> Double
highestOf f period bars = maximum $ lastNValues period f bars

lowestOf :: (Bar -> Price) -> Int -> [Bar] -> Double
lowestOf f period bars = minimum $ lastNValues period f bars

sma :: Int -> [Double] -> Double
sma period values = if period > 0 && (not . null) actualValues
  then sum actualValues / fromIntegral (length actualValues)
  else 0
    where
      actualValues = take period values

ema :: Int -> [Double] -> Double
ema period values = if period > 0
  then foldl (\x y -> y * alpha + x * (1 - alpha)) (sma period (drop (2 * period) values)) $ reverse $ take (2 * period) values
  else 0
    where
      alpha = 2.0 / (fromIntegral period + 1.0)

emaWithAlpha :: Double -> [Double] -> Double
emaWithAlpha alpha values = foldl (\x y -> x * (1 - alpha) + y * alpha) 0 $ reverse values

intradayBarNumber :: [Bar] -> Int
intradayBarNumber bars = case headMay bars of
  Just bar -> intradayBarNumber' bar bars - 1
  Nothing  -> 0
  where
    intradayBarNumber' :: Bar -> [Bar] -> Int
    intradayBarNumber' bar bars' = case headMay bars' of
      Just bar' -> if dayOf bar /= dayOf bar'
        then 0
        else 1 + intradayBarNumber' bar (tail bars')
      Nothing -> 0

    dayOf = utctDay . barTimestamp

hVolumeAt :: Price -> Int -> [Bar] -> Double
hVolumeAt price period bars =
  sum $ fmap (fromInteger . barVolume) $ L.filter (\x -> barHigh x >= price && barLow x <= price) $ take period bars

getMaxHVol :: Price -> Price -> Int -> Int -> [Bar] -> Maybe Price
getMaxHVol start step steps period bars = fmap fst $ minimumByMay (\x y -> snd x `compare` snd y) $ (\price -> (price, hVolumeAt price period bars)) <$> range step start (start + fromIntegral steps * step)
  where
    range step' start' end = takeWhile (<= end) $ iterate (+ step') start'

bbandUpper :: Int -> Double -> [Double] -> Double
bbandUpper period devs values = sma period values + devs * sigma
  where
    sigma = stddev $ take period values
    stddev vs
      | length vs > 1 = sqrt ((sum (map (\x -> (x - mean vs) * (x - mean vs)) vs)) / (fromIntegral $ length vs - 1))
      | otherwise = 0
    mean = sma period

percentRank :: Int -> [Double] -> Double
percentRank period values@(v:vs) = fromIntegral (length (filter (\x -> x < v) $ take period values)) / fromIntegral (length (take period values))
percentRank period [] = 0

