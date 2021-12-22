{-# LANGUAGE RankNTypes #-}

module ATrade.RoboCom.ConfigStorage
(
  ConfigStorage(..)
) where

import qualified Data.Text as T
import           Dhall     (FromDhall)

class (Monad m) => ConfigStorage m where
  loadConfig :: forall c. (FromDhall c) => T.Text -> m c


