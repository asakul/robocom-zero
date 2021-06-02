{-# LANGUAGE DeriveGeneric #-}

module ATrade.Exceptions (
  RoboComException(..)
) where

import           Control.Exception
import qualified Data.Text         as T
import           GHC.Generics

data RoboComException = UnableToLoadConfig T.Text
                      | UnableToLoadFeed T.Text
                      | UnableToLoadState T.Text
                      | UnableToSaveState T.Text
                      | BadParams T.Text
  deriving (Show, Generic)

instance Exception RoboComException

