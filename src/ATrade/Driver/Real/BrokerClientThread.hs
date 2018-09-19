{-# LANGUAGE OverloadedStrings #-}

module ATrade.Driver.Real.BrokerClientThread (
  startBrokerClientThread,
  BrokerCommand(..)
) where

import ATrade.Broker.Client
import ATrade.Broker.Protocol
import ATrade.RoboCom.Monad hiding (submitOrder, cancelOrder)
import ATrade.RoboCom.Types
import ATrade.Types

import Control.Concurrent.BoundedChan
import Control.Concurrent hiding (writeChan, readChan, writeList2Chan, yield)
import Control.Exception
import Control.Monad.Loops
import Control.Monad

import Data.IORef
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Time.Clock
import Data.Maybe

import System.Log.Logger
import System.ZMQ4 hiding (Event)

data BrokerCommand = BrokerSubmitOrder Order | BrokerCancelOrder Integer | BrokerRequestNotifications


startBrokerClientThread :: T.Text -> Context -> T.Text -> BoundedChan BrokerCommand -> BoundedChan Event -> MVar a -> IO ThreadId
startBrokerClientThread instId ctx brEp ordersChan eventChan shutdownVar = forkIO $ whileM_ (isNothing <$> tryReadMVar shutdownVar) $
  bracket (startBrokerClient (encodeUtf8 instId) ctx brEp defaultClientSecurityParams)
    (\bro -> do
      stopBrokerClient bro
      debugM "Strategy" "Broker client: stop")
    (\bs -> handle (\e -> do
      warningM "Strategy" $ "Broker client: exception: " ++ show (e :: SomeException)
      throwIO e) $ do
        now <- getCurrentTime
        lastNotificationTime <- newIORef now
        whileM_ (andM [notTimeout lastNotificationTime, isNothing <$> tryReadMVar shutdownVar]) $ do
          brokerCommand <- readChan ordersChan
          case brokerCommand of
            BrokerSubmitOrder order -> do
              debugM "Strategy" $ "Submitting order: " ++ show order
              maybeOid <- submitOrder bs order
              debugM "Strategy" "Order submitted"
              case maybeOid of
                Right oid -> writeChan eventChan (OrderSubmitted order { orderId = oid })
                Left errmsg -> debugM "Strategy" $ T.unpack $ "Error: " `T.append` errmsg
            BrokerCancelOrder oid -> do
              debugM "Strategy" $ "Cancelling order: " ++ show oid
              _ <- cancelOrder bs oid
              debugM "Strategy" "Order cancelled"
            BrokerRequestNotifications -> do
              t <- getCurrentTime
              nt <- readIORef lastNotificationTime 
              when (t `diffUTCTime` nt > 1) $ do
                maybeNs <- getNotifications bs
                case maybeNs of
                  Left errmsg -> debugM "Strategy" $ T.unpack $ "Error: " `T.append` errmsg
                  Right ns -> do
                    mapM_ (sendNotification eventChan) ns
                    getCurrentTime >>= (writeIORef lastNotificationTime)
        nTimeout <- notTimeout lastNotificationTime
        shouldShutdown <- isNothing <$> tryReadMVar shutdownVar
        debugM "Strategy" $ "Broker loop end: " ++ show nTimeout ++ "/" ++ show shouldShutdown)

notTimeout :: IORef UTCTime -> IO Bool
notTimeout ts = do
  now <- getCurrentTime
  heartbeatTs <- readIORef ts
  return $ diffUTCTime now heartbeatTs < 30

sendNotification :: BoundedChan Event -> Notification -> IO ()
sendNotification eventChan notification =
  writeChan eventChan $ case notification of
    OrderNotification oid state -> OrderUpdate oid state
    TradeNotification trade -> NewTrade trade
