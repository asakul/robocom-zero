{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels      #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TemplateHaskell       #-}

module ATrade.Driver.Junction.OrderRouter
  (
    mkOrderRouter,
    AccountsList
  ) where

import           ATrade.Broker.Client           (BrokerClientHandle (cancelOrder, getNotifications, submitOrder),
                                                 startBrokerClient,
                                                 stopBrokerClient)
import           ATrade.Broker.Protocol         (Notification (..))
import           ATrade.RoboCom.Monad           (Event (..))
import           ATrade.Types                   (ClientSecurityParams,
                                                 Order (..), OrderId)
import           Control.Concurrent.BoundedChan (BoundedChan, newBoundedChan,
                                                 readChan, tryReadChan,
                                                 writeChan, writeList2Chan)
import           Control.Monad                  (forM_, forever)
import           Control.Monad.Logger           (MonadLogger, logDebugS,
                                                 logInfoS, logWarnS)
import qualified Data.Bimap                     as BM
import qualified Data.ByteString.Char8          as B8
import           Data.List                      (find)
import qualified Data.Text                      as T
import           GHC.OverloadedLabels           (IsLabel (..))
import           System.ZMQ4                    (Context)
import           UnliftIO                       (MonadUnliftIO, liftIO)
import           UnliftIO.Concurrent            (ThreadId, forkIO)
import           UnliftIO.IORef                 (IORef, atomicModifyIORef',
                                                 newIORef, readIORef)

data OrderRouterEvent =
  SubmitOrder Order |
  CancelOrder OrderId |
  BrokerNotification Notification

data OrderRouter =
  OrderRouter
  {
    requestChan  :: BoundedChan OrderRouterEvent,
    eventChan    :: BoundedChan Event,
    routerThread :: ThreadId,
    brokers      :: [([T.Text], BrokerClientHandle)]
  }

instance IsLabel "requestChan" (OrderRouter -> BoundedChan OrderRouterEvent) where
  fromLabel = requestChan

instance IsLabel "eventChan" (OrderRouter -> BoundedChan Event) where
  fromLabel = eventChan

instance IsLabel "brokers" (OrderRouter -> [([T.Text], BrokerClientHandle)]) where
  fromLabel = brokers

data OrderRouterEnv =
  OrderRouterEnv
  {
    requestChan               :: BoundedChan OrderRouterEvent,
    eventChan                 :: BoundedChan Event,
    brokers                   :: [([T.Text], BrokerClientHandle)],
    notificationRequestThread :: ThreadId,
    orderIdMap                :: IORef (BM.Bimap OrderId (OrderId, T.Text)),
    currentOrderId            :: IORef OrderId
  }

instance IsLabel "requestChan" (OrderRouterEnv -> BoundedChan OrderRouterEvent) where
  fromLabel = requestChan

instance IsLabel "eventChan" (OrderRouterEnv -> BoundedChan Event) where
  fromLabel = eventChan

instance IsLabel "brokers" (OrderRouterEnv -> [([T.Text], BrokerClientHandle)]) where
  fromLabel = brokers

-- | List of pairs: ([accounts], broker-endpoint, security-params)
type AccountsList = [([T.Text], T.Text, ClientSecurityParams)]


mkOrderRouter :: (MonadUnliftIO m, MonadLogger m) => Context -> AccountsList -> BoundedChan Event -> m OrderRouter
mkOrderRouter ctx accounts evtChan = do
  $(logInfoS) "OrderRouter" "Order Router started"
  rqChan <- liftIO $ newBoundedChan 1000
  bros <- makeBrokers accounts
  idmap <- newIORef BM.empty
  rqThread <- forkIO $ requestNotifications bros rqChan
  idcnt <- newIORef 1
  let env = OrderRouterEnv {
        requestChan = rqChan,
        eventChan = evtChan,
        brokers = bros,
        notificationRequestThread = rqThread,
        orderIdMap = idmap,
        currentOrderId = idcnt
  }
  tId <- forkIO (react env)
  return $ OrderRouter rqChan evtChan tId bros
  where
    makeBrokers = mapM (\(accs, ep, secParams) -> do
                           bro <- liftIO $ startBrokerClient (B8.pack "foo") ctx ep secParams
                           return (accs, bro))

    react env = do
      $(logDebugS) "OrderRouter" "Order Router react"
      let rqChan = #requestChan env
      evts <- liftIO $ readChanMax 20 rqChan
      forM_ evts (handleEvent env)

    handleEvent env evt = do
      case evt of
        (SubmitOrder order) -> doSubmitOrder env order
        (CancelOrder oid) -> doCancelOrder env oid
        (BrokerNotification notification) -> handleBrokerNotification env notification

    readChanMax n chan = do
      first <- readChan chan
      rest <- readChanN (n - 1) chan
      return $ first : rest

    readChanN n chan
      | n <= 0 = return []
      | otherwise = do
        x <- tryReadChan chan
        case x of
          Nothing -> return []
          Just v -> do
            rest <- readChanN (n - 1) chan
            return $ v : rest

    doSubmitOrder env order = do
      let bros = #brokers env
      case findBrokerForAccount (orderAccountId order) bros of
        Just bro -> do
          result <- liftIO $ submitOrder bro order
          case result of
            Left errmsg -> $(logWarnS) "OrderRouter" $ "Unable to submit order: " <> errmsg
            Right oid   -> do
              newOrderId <- atomicModifyIORef' (currentOrderId env) (\s -> (s + 1, s))
              atomicModifyIORef' (orderIdMap env) (\s -> (BM.insert newOrderId (oid, orderAccountId order) s, ()))
              pushEvent (OrderSubmitted order { orderId = newOrderId })

        Nothing -> $(logWarnS) "OrderRouter" $ "No broker found for account: " <> orderAccountId order

    doCancelOrder env oid = do
      let bros = #brokers env
      idpair <- BM.lookup oid <$> readIORef (orderIdMap env)
      case idpair of
        Just (brokerOrderId, account) ->
          case findBrokerForAccount account bros of
            Just bro -> do
              result <- liftIO $ cancelOrder bro brokerOrderId
              case result of
                Left errmsg -> $(logWarnS) "OrderRouter" $ "Unable to cancel order: " <> (T.pack . show) brokerOrderId <> ", account: " <> account <> ", " <> errmsg
                Right _ -> return ()
            Nothing -> $(logWarnS) "OrderRouter" $ "Can't find broker for order: " <> (T.pack . show) brokerOrderId <> ", account: " <> account
        Nothing -> $(logWarnS) "OrderRouter" $ "Can't find order id map: " <> (T.pack . show) oid

    handleBrokerNotification env notification = undefined
    pushEvent event = liftIO $ writeChan evtChan event

    findBrokerForAccount :: T.Text -> [([T.Text], BrokerClientHandle)] -> Maybe BrokerClientHandle
    findBrokerForAccount accId bros = snd <$> find (\x -> accId `elem` fst x) bros

    requestNotifications bros rqChan = forever $ do
      forM_ bros $ \(_, handle) -> do
        result <- liftIO $ getNotifications handle
        case result of
          Left errmsg -> $(logWarnS) "OrderRouter" $ "Can't request notifications: " <> errmsg
          Right nots -> liftIO $ writeList2Chan rqChan (BrokerNotification <$> nots)


