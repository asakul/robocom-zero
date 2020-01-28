{-# LANGUAGE TemplateHaskell #-}

module ATrade.RoboCom
(
  robocom_version
) where

import           Data.Version
import           Paths_robocom_zero

import           Development.GitRev

robocom_version :: Version
robocom_version = version

robocom_gitrev :: String
robocom_gitrev = concat [ "robocom-zero-",
                            $(gitBranch),
                            "@",
                            $(gitHash),
                            dirty ]
  where
    dirty | $(gitDirty) = "+"
          | otherwise   = ""


