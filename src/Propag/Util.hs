module Propag.Util (
    timeIt
  , timeItT
  , lazyAction
  , justOrFail
  , withLockFromSet
  , forkOSFinally
) where

import Control.Concurrent.STM.TVar
import Control.Concurrent
import Control.Exception (Exception, SomeException, mask, try)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Catch (MonadThrow(throwM), MonadMask, bracket_)
import Control.Monad.STM
import Data.IORef (newIORef, readIORef, writeIORef)
import qualified Data.HashSet as S
import Data.Hashable (Hashable)
import System.CPUTime (getCPUTime)
import Text.Printf

timeItT :: MonadIO m => m a -> m (Double, a)
timeItT ioa = do
    t1 <- liftIO getCPUTime
    a <- ioa
    t2 <- liftIO getCPUTime
    let t :: Double
        t = fromIntegral (t2-t1) * 1e-12
    return (t, a)

timeIt :: MonadIO m => m a -> m a
timeIt ioa = do
    (t, a) <- timeItT ioa
    liftIO $ printf "CPU time: %6.2fs\n" t
    return a


forkOSFinally :: IO a -> (Either SomeException a -> IO ()) -> IO ThreadId
forkOSFinally action and_then =
  mask $ \restore ->
    forkOS $ try (restore action) >>= and_then

lazyAction :: MonadIO m => m a -> m (m a)
lazyAction act = do
  ref <- liftIO (newIORef Nothing)
  return $ do
    mRet <- liftIO (readIORef ref)
    case mRet of
      Nothing -> do
        ret <- act
        liftIO (writeIORef ref (Just ret))
        return ret
      Just ret -> return ret


justOrFail :: (MonadThrow m, Exception e) => e -> Maybe a -> m a
justOrFail err = maybe (throwM err) return


withLockFromSet
  :: (MonadIO m, MonadMask m, Eq k, Hashable k)
  => TVar (S.HashSet k) -> k -> m a -> m a
withLockFromSet lockVar k = bracket_ acquire release
  where
    acquire = liftIO $ atomically $ do
      locks <- readTVar lockVar
      if k `S.member` locks
        then retry
        else modifyTVar' lockVar (S.insert k)
    release = liftIO $ atomically $ modifyTVar' lockVar (S.delete k)
