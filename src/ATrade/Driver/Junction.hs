{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings     #-}

module ATrade.Driver.Junction
  (
    junctionMain
  ) where

import           ATrade.Driver.Junction.Types  (StrategyDescriptor (..),
                                                StrategyInstance (..),
                                                StrategyInstanceDescriptor (..))
import           ATrade.RoboCom.Types          (Ticker (..))
import           Control.Concurrent            (forkIO)
import           Control.Concurrent.Async      (forConcurrently_)
import           Control.Concurrent.STM        (atomically)
import           Control.Concurrent.STM.TQueue (newTQueueIO)
import           Control.Concurrent.STM.TVar   (newTVarIO)
import           Data.Aeson                    (FromJSON (..), ToJSON (..),
                                                decode, object, withObject,
                                                (.:), (.=))
import qualified Data.ByteString               as B
import qualified Data.ByteString.Lazy          as BL
import           Data.IORef
import qualified Data.Map.Strict               as M
import           Data.Maybe                    (fromMaybe)
import           Data.Semigroup                ((<>))
import qualified Data.Text                     as T
import           Data.Text.IO                  (readFile)
import           Dhall                         (FromDhall, auto, input)
import           GHC.Generics                  (Generic)
import           Options.Applicative           (Parser, execParser, fullDesc,
                                                header, help, helper, info,
                                                long, metavar, progDesc, short,
                                                strOption, (<**>))
import           Prelude                       hiding (readFile)

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

data ProgramOptions =
  ProgramOptions
  {
    configPath :: FilePath
  }

data ProgramConfiguration =
  ProgramConfiguration
  {
    brokerEndpoint        :: T.Text,
    brokerServerCert      :: Maybe FilePath,
    brokerClientCert      :: Maybe FilePath,
    quotesourceEndpoint   :: T.Text,
    quotesourceServerCert :: Maybe FilePath,
    quotesourceClientCert :: Maybe FilePath,
    qhpEndpoint           :: T.Text,
    qtisEndpoint          :: T.Text,
    redisSocket           :: T.Text,
    globalLog             :: FilePath,
    instances             :: [StrategyInstanceDescriptor]
  } deriving (Generic, Show)

instance FromDhall ProgramConfiguration

load :: T.Text -> IO (Maybe B.ByteString)
load = undefined

junctionMain :: M.Map T.Text StrategyDescriptor -> IO ()
junctionMain descriptors = do
  opts <- parseOptions

  cfg <- readFile (configPath opts) >>= input auto

  bars <- newTVarIO M.empty

  strategies <- mkStrategies (instances cfg)

  start strategies bars

  where
    parseOptions = execParser options
    options = info (optionsParser <**> helper)
      (fullDesc <>
       progDesc "Robocom-zero junction mode driver" <>
       header "robocom-zero-junction")

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

    start strategies bars = undefined

    optionsParser :: Parser ProgramOptions
    optionsParser = ProgramOptions
      <$> strOption
            (long "config" <>
             short 'c' <>
             metavar "FILENAME" <>
             help "Configuration file path")

