{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module ATrade.Driver.Real.BrokerClientThread (
  startBrokerClientThread,
  BrokerCommand(..)
) where

import           ATrade.Broker.Client
import           ATrade.Broker.Protocol
import           ATrade.RoboCom.Monad           hiding (cancelOrder,
                                                 submitOrder)
import           ATrade.Types

import           Control.Concurrent             hiding (readChan, writeChan,
                                                 writeList2Chan, yield)
import           Control.Concurrent.BoundedChan
import           Control.Exception
import           Control.Monad
import           Control.Monad.Loops

import           Data.IORef
import           Data.Maybe
import qualified Data.Text                      as T
import           Data.Text.Encoding
import           Data.Time.Clock

import           System.Log.Logger
import           System.ZMQ4                    hiding (Event)

data BrokerCommand = BrokerSubmitOrder Order | BrokerCancelOrder Integer | BrokerRequestNotifications | BrokerHandleNotification Notification

startBrokerClientThread :: T.Text -> Context -> T.Text -> T.Text -> BoundedChan BrokerCommand -> BoundedChan Event -> MVar a -> IO ThreadId
startBrokerClientThread instId ctx brEp notifEp ordersChan eventChan shutdownVar = do
  let callback = writeChan ordersChan . BrokerHandleNotification
  forkIO $ whileM_ (isNothing <$> tryReadMVar shutdownVar) $
    bracket (startBrokerClient (encodeUtf8 instId) ctx brEp notifEp [callback] defaultClientSecurityParams)
      (\bro -> do
        stopBrokerClient bro
        debugM "Strategy" "Broker client: stop")
      (\bs -> handle (\e -> do
        warningM "Strategy" $ "Broker client: exception: " ++ show (e :: SomeException)
        throwIO e) $ do
          now <- getCurrentTime
          lastNotificationTime <- newIORef now
          lastKnownSqnum <- newIORef 0
          whileM_ (andM [notTimeout lastNotificationTime, isNothing <$> tryReadMVar shutdownVar]) $ do
            brokerCommand <- readChan ordersChan
            case brokerCommand of
              BrokerSubmitOrder order -> do
                debugM "Strategy" $ "Submitting order: " ++ show order
                result <- submitOrder bs order
                debugM "Strategy" "Order submitted"
                case result of
                  Right _ -> debugM "Strategy" $ "Order submitted: " ++ show (orderId order)
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
                      mapM_ (\n -> do
                                prevSqnum <- atomicModifyIORef lastKnownSqnum (\s -> (getNotificationSqnum n, s))
                                when (prevSqnum + 1 < getNotificationSqnum n) $
                                  warningM "Strategy" $ "Sqnum jump: " ++ show prevSqnum ++ "->" ++ show (getNotificationSqnum n)
                                sendNotification eventChan n) ns
                      getCurrentTime >>= writeIORef lastNotificationTime
              BrokerHandleNotification notification -> do
                sendNotification eventChan n
                prevSqnum <- atomicModifyIORef lastKnownSqnum (\s -> (getNotificationSqnum n, s))

                undefined
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
    OrderNotification sqnum oid state -> OrderUpdate oid state
    TradeNotification sqnum trade     -> NewTrade trade
