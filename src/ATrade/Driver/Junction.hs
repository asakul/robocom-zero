{-# LANGUAGE OverloadedStrings #-}

module ATrade.Driver.Junction
  (
    junctionMain
  ) where

import           ATrade.Driver.Junction.Types (StrategyDescriptor (..),
                                               StrategyInstance (..),
                                               StrategyInstanceDescriptor (..))
import           ATrade.RoboCom.Types         (Ticker (..))
import           Data.Aeson                   (FromJSON (..), ToJSON (..),
                                               decode, object, withObject, (.:),
                                               (.=))
import qualified Data.ByteString              as B
import qualified Data.ByteString.Lazy         as BL
import           Data.IORef
import qualified Data.Map.Strict              as M
import           Data.Maybe                   (fromMaybe)
import qualified Data.Text                    as T

data BigConfig c = BigConfig {
  confTickers  :: [Ticker],
  confStrategy :: c
}

instance (FromJSON c) => FromJSON (BigConfig c) where
  parseJSON = withObject "object" (\obj -> BigConfig <$>
    obj .: "tickers" <*>
    obj .: "params")

instance (ToJSON c) => ToJSON (BigConfig c) where
  toJSON conf = object ["tickers" .= confTickers conf,
    "params" .= confStrategy conf ]


load :: T.Text -> IO (Maybe B.ByteString)
load = undefined

junctionMain :: M.Map T.Text StrategyDescriptor -> IO ()
junctionMain descriptors = do
  parseOptions
  instanceDescriptors <- undefined
  strategies <- mkStrategies instanceDescriptors

  start strategies

  where
    parseOptions = undefined

    mkStrategies :: [StrategyInstanceDescriptor] -> IO [StrategyInstance]
    mkStrategies = mapM mkStrategy

    mkStrategy :: StrategyInstanceDescriptor -> IO StrategyInstance
    mkStrategy desc = do
      sState <- load (stateKey desc)
      sCfg <- load (configKey desc)
      case M.lookup (strategyId desc) descriptors of
        Just (StrategyDescriptor _sName sCallback sDefState) ->
          case (sCfg >>= decode . BL.fromStrict, fromMaybe sDefState (sState >>= decode . BL.fromStrict)) of
            (Just bigConfig, pState) -> do
              cfgRef <- newIORef (confStrategy bigConfig)
              stateRef <- newIORef pState
              return $ StrategyInstance
                {
                  strategyInstanceId = strategyName desc,
                  strategyEventCallback  = sCallback,
                  strategyState = stateRef,
                  strategyConfig = cfgRef
                }
            _ -> error "Can't read state and config"
        _ -> error $ "Can't find strategy: " ++ T.unpack (strategyId desc)

    start strategy = undefined




