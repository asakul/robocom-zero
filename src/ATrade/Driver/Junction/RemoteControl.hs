{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}

module ATrade.Driver.Junction.RemoteControl
  (
    handleRemoteControl
  ) where

import           ATrade.Driver.Junction.JunctionMonad (JunctionEnv (peLogAction, peRemoteControlSocket, peRobots),
                                                       JunctionM)
import           ATrade.Driver.Junction.Types         (StrategyInstanceDescriptor)
import           ATrade.Logging                       (logErrorWith)
import           Control.Monad                        (unless)
import           Control.Monad.Reader                 (asks)
import           Data.Aeson                           (decode)
import qualified Data.ByteString                      as B
import qualified Data.ByteString.Lazy                 as BL
import qualified Data.Map.Strict                      as M
import qualified Data.Text                            as T
import           Data.Text.Encoding                   (decodeUtf8', encodeUtf8)
import           System.ZMQ4                          (Event (In), Poll (Sock),
                                                       poll, receive, send)
import           UnliftIO                             (MonadIO (liftIO))

data RemoteControlResponse =
    ResponseOk
  | ResponseError T.Text
  deriving (Show, Eq)

data RemoteControlRequest =
    StartRobot StrategyInstanceDescriptor
  | StopRobot T.Text
  | ReloadConfig T.Text
  | SetState T.Text B.ByteString
  | Ping
  deriving (Show)

data ParseError =
    UnknownCmd
  | UtfDecodeError
  | JsonDecodeError
  deriving (Show, Eq)

parseRemoteControlRequest :: B.ByteString -> Either ParseError RemoteControlRequest
parseRemoteControlRequest bs =
  if
    | cmd == "START" -> parseStart
    | cmd == "STOP" -> parseStop
    | cmd == "RELOAD_CONFIG" -> parseReloadConfig
    | cmd == "SET_STATE" -> parseSetState
    | cmd == "PING" -> Right Ping
    | otherwise -> Left UnknownCmd
  where
    cmd = B.takeWhile (/= 0x20) bs
    rest = B.dropWhile (== 0x20) . B.dropWhile (/= 0x20) $ bs

    parseStart = case decode . BL.fromStrict $ rest of
      Just inst -> Right (StartRobot inst)
      Nothing   -> Left JsonDecodeError

    parseStop = case decodeUtf8' rest of
      Left _  -> Left UtfDecodeError
      Right r -> Right (StopRobot (T.strip r))

    parseReloadConfig = case decodeUtf8' rest of
      Left _  -> Left UtfDecodeError
      Right r -> Right (ReloadConfig (T.strip r))

    parseSetState = case decodeUtf8' (B.takeWhile (/= 0x20) rest) of
      Left _  -> Left UtfDecodeError
      Right r -> Right (SetState r (B.dropWhile (== 0x20) . B.dropWhile (/= 0x20) $ rest))


makeRemoteControlResponse :: RemoteControlResponse -> B.ByteString
makeRemoteControlResponse ResponseOk          = "OK"
makeRemoteControlResponse (ResponseError msg) = "ERROR " <> encodeUtf8 msg

handleRemoteControl :: Int -> JunctionM ()
handleRemoteControl timeout = do
  sock <- asks peRemoteControlSocket
  logger <- asks peLogAction
  evs <- poll (fromIntegral timeout) [Sock sock [In] Nothing]
  unless (null evs) $ do
    rawRequest <- liftIO $ receive sock
    case parseRemoteControlRequest rawRequest of
      Left err      -> logErrorWith logger "RemoteControl" ("Unable to parse request: " <> (T.pack . show) err)
      Right request -> do
        response <- handleRequest request
        liftIO $ send sock [] (makeRemoteControlResponse response)
  where
    handleRequest (StartRobot inst)          = undefined
    handleRequest (StopRobot instId)         = undefined
    handleRequest (ReloadConfig instId)      = undefined
    handleRequest (SetState instId rawState) = undefined
    handleRequest Ping                       = return ResponseOk


