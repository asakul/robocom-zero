
module ATrade.Driver.Junction
  (
    junctionMain
  ) where

import           ATrade.Driver.Junction.Types (StrategyDescriptor (..),
                                               StrategyInstance (..),
                                               StrategyInstanceDescriptor (..))
import           Data.Aeson                   (decode)
import qualified Data.ByteString              as B
import qualified Data.ByteString.Lazy         as BL
import           Data.IORef
import qualified Data.Map.Strict              as M
import qualified Data.Text                    as T

load :: T.Text -> IO B.ByteString
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
        Just (StrategyDescriptor _sName sCallback _sDefState) ->
          case (decode $ BL.fromStrict sCfg, decode $ BL.fromStrict sState) of
            (Just pCfg, Just pState) -> do
              cfgRef <- newIORef pCfg
              stateRef <- newIORef pState
              return $ StrategyInstance
                {
                  strategyInstanceId = strategyName desc,
                  strategyEventCallback  = sCallback,
                  strategyState = stateRef,
                  strategyConfig = cfgRef
                }
            _ -> undefined
        _ -> undefined

    start = undefined




