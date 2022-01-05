{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}

module ATrade.Driver.Junction.RemoteControl
  (
    handleRemoteControl
  ) where

import           ATrade.Driver.Junction.JunctionMonad     (JunctionEnv (peLogAction, peRemoteControlSocket, peRobots),
                                                           JunctionM, getState,
                                                           reloadConfig,
                                                           startRobot)
import           ATrade.Driver.Junction.RobotDriverThread (stopRobot)
import           ATrade.Driver.Junction.Types             (StrategyInstanceDescriptor)
import           ATrade.Logging                           (Severity (Info),
                                                           logErrorWith,
                                                           logWith)
import           Control.Monad                            (unless)
import           Control.Monad.Reader                     (asks)
import           Data.Aeson                               (decode)
import qualified Data.ByteString                          as B
import qualified Data.ByteString.Lazy                     as BL
import qualified Data.Map.Strict                          as M
import qualified Data.Text                                as T
import           Data.Text.Encoding                       (decodeUtf8',
                                                           encodeUtf8)
import           System.ZMQ4                              (Event (In),
                                                           Poll (Sock), poll,
                                                           receive, send)
import           UnliftIO                                 (MonadIO (liftIO),
                                                           atomicModifyIORef',
                                                           readIORef)

data RemoteControlResponse =
    ResponseOk
  | ResponseError T.Text
  | ResponseData B.ByteString
  deriving (Show, Eq)

data RemoteControlRequest =
    StartRobot StrategyInstanceDescriptor
  | StopRobot T.Text
  | ReloadConfig T.Text
  | GetState T.Text
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
    | cmd == "GET_STATE" -> parseGetState
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

    parseGetState = case decodeUtf8' (B.takeWhile (/= 0x20) rest) of
      Left _  -> Left UtfDecodeError
      Right r -> Right (GetState r)

    parseSetState = case decodeUtf8' (B.takeWhile (/= 0x20) rest) of
      Left _  -> Left UtfDecodeError
      Right r -> Right (SetState r (B.dropWhile (== 0x20) . B.dropWhile (/= 0x20) $ rest))


makeRemoteControlResponse :: RemoteControlResponse -> B.ByteString
makeRemoteControlResponse ResponseOk          = "OK"
makeRemoteControlResponse (ResponseError msg) = "ERROR " <> encodeUtf8 msg
makeRemoteControlResponse (ResponseData d)    = "DATA\n" <> d

handleRemoteControl :: Int -> JunctionM ()
handleRemoteControl timeout = do
  sock <- asks peRemoteControlSocket
  logger <- asks peLogAction
  evs <- poll (fromIntegral timeout) [Sock sock [In] Nothing]
  case evs of
    (x:_) -> unless (null x) $ do
      rawRequest <- liftIO $ receive sock
      case parseRemoteControlRequest rawRequest of
        Left err      -> logErrorWith logger "RemoteControl" ("Unable to parse request: " <> (T.pack . show) err)
        Right request -> do
          response <- handleRequest request
          liftIO $ send sock [] (makeRemoteControlResponse response)
    _ -> return ()
  where
    handleRequest (StartRobot inst)          = do
      startRobot inst
      return ResponseOk
    handleRequest (StopRobot instId)         = do
      robotsRef <- asks peRobots
      robots <- readIORef robotsRef
      case M.lookup instId robots of
        Just robot -> do
          logger <- asks peLogAction
          logWith logger Info "RemoteControl" $ "Stopping robot: " <> instId
          stopRobot robot
          liftIO $ atomicModifyIORef' robotsRef (\r -> (M.delete instId r, ()))
          return ResponseOk
        Nothing    -> return $ ResponseError $ "Not started: " <> instId

    handleRequest (ReloadConfig instId)      = do
      res <- reloadConfig instId
      case res of
        Left errmsg -> return $ ResponseError errmsg
        Right ()    -> return ResponseOk
    handleRequest (GetState instId) = do
      res <- getState instId
      case res of
        Left errmsg -> return $ ResponseError errmsg
        Right d     -> return $ ResponseData d
    handleRequest (SetState instId rawState) = undefined
    handleRequest Ping                       = return ResponseOk


