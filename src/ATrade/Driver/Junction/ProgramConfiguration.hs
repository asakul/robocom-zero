{-# LANGUAGE DeriveGeneric #-}

module ATrade.Driver.Junction.ProgramConfiguration
  (
    ProgramOptions(..),
    ProgramConfiguration(..)
  ) where
import           ATrade.Driver.Junction.Types (StrategyInstanceDescriptor)
import qualified Data.Text                    as T
import           Dhall                        (FromDhall)
import           GHC.Generics                 (Generic)

newtype ProgramOptions =
  ProgramOptions
  {
    configPath :: FilePath
  }

data ProgramConfiguration =
  ProgramConfiguration
  {
    brokerEndpoint             :: T.Text,
    brokerNotificationEndpoint :: T.Text,
    brokerServerCert           :: Maybe FilePath,
    brokerClientCert           :: Maybe FilePath,
    quotesourceEndpoint        :: T.Text,
    quotesourceServerCert      :: Maybe FilePath,
    quotesourceClientCert      :: Maybe FilePath,
    qhpEndpoint                :: T.Text,
    qtisEndpoint               :: T.Text,
    redisSocket                :: T.Text,
    robotsConfigsPath          :: FilePath,
    logBasePath                :: FilePath,
    instances                  :: [StrategyInstanceDescriptor]
  } deriving (Generic, Show)

instance FromDhall ProgramConfiguration
