{-# LANGUAGE RankNTypes #-}

module ATrade.RoboCom.Persistence
(
  MonadPersistence(..)
) where

import           Data.Aeson
import           Data.Default (Default)
import qualified Data.Text    as T

class (Monad m) => MonadPersistence m where
  saveState :: forall s. (ToJSON s) => s -> T.Text -> m ()
  loadState :: forall s. (Default s, FromJSON s) => T.Text -> m s


