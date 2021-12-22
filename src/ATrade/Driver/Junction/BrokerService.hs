{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module ATrade.Driver.Junction.BrokerService
  (
    BrokerService,
    mkBrokerService,
    submitOrder,
    cancelOrder,
    getNotifications
  ) where

import qualified ATrade.Broker.Client       as Bro
import           ATrade.Broker.Protocol     (Notification (..))
import           ATrade.Logging             (Message, logDebug)
import           ATrade.Types               (Order (..), OrderId)
import           Colog                      (WithLog)
import           Control.Monad.IO.Class     (MonadIO (liftIO))
import           Control.Monad.Reader.Class (MonadReader)
import           Data.IORef                 (IORef, atomicModifyIORef',
                                             newIORef)
import qualified Data.Map.Strict            as M
import qualified Data.Text                  as T

data BrokerService =
  BrokerService
  {
    broker         :: Bro.BrokerClientHandle,
    orderMap       :: IORef (M.Map OrderId T.Text),
    orderIdCounter :: IORef OrderId
  }

mkBrokerService :: Bro.BrokerClientHandle -> IORef (M.Map OrderId T.Text) -> IO BrokerService
mkBrokerService h om = BrokerService h om <$> newIORef 1

submitOrder :: (MonadIO m, WithLog env Message m, MonadReader env m) => BrokerService -> T.Text -> Order -> m OrderId
submitOrder service identity order = do
  oid <- nextOrderId service
  logDebug "BrokerService" $ "New order, id: " <> (T.pack . show) oid
  liftIO $ atomicModifyIORef' (orderMap service) (\s -> (M.insert oid identity s, ()))
  _ <- liftIO $ Bro.submitOrder (broker service) order { orderId = oid }
  return oid
  where
    nextOrderId srv = liftIO $ atomicModifyIORef' (orderIdCounter srv) (\s -> (s + 1, s))

cancelOrder :: BrokerService -> OrderId -> IO ()
cancelOrder service oid = do
  _ <- Bro.cancelOrder (broker service) oid
  return ()

getNotifications :: BrokerService -> IO [Notification]
getNotifications service = do
  v <- Bro.getNotifications (broker service)
  case v of
    Left _  -> return []
    Right n -> return n
