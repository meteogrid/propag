{-# LANGUAGE CPP #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RecordWildCards #-}

module Propag.Engine (propagate) where

import Propag.Types (
    HasLoadExtent (..)
  , CanSerialize
  , PropagConfig
  , PropagationResult (..)
  , InitialPropagConfig
  , PropagInputs (..)
  , IgnitedElement (..)
  , InputMap (..)
  , TimeSpec
  , Template
  , Input
  , Loader
  , LoaderRaster
  , Fire (..)
  , IsUnit (..)
  , Speed
  , Azimuth
  , Time
  , inputMap
  , mapInputs
  , mapMInputs
  , toAzimuth
  , geometry
  , similarFires
  , getName
  , indexLoaderRaster
  , year

  , fOrigin

  , fuelCatalog
  , pixelSize
  , propagates
  , initialElements
  , initialTimes
  , inputs
  , uniformityConditions
  , inputsLens
  , maxTime
  , blockSize
  , mkLoader
  , runLoader
  , explodeAndInterpolate

  , d1hr
  , d10hr
  , d100hr
  , herb
  , wood
  , windSpeed
  , windBearing
  , slope
  , aspect
  , fuel

  , minute
  , (*~)
  , _0
  )
import Propag.BlockMap

import Propag.Util (justOrFail, forkOSFinally)
import Propag.Geometry (
    GeoReference(..)
  , Extent(..)
  , GeoTransform(..)
  , Raster (..)
  , PixelSize (..)
  , southUpGeoReference
  , geoRefExtent
  , dda
  )

import Behave

import Control.Applicative (liftA2)
import Control.Concurrent.STM.TQueue
import Control.Concurrent.STM.TVar
import Control.Concurrent.STM.TMVar
import Control.Concurrent (ThreadId, forkIO, forkFinally, myThreadId)
import Control.Exception (SomeException, Exception, assert, throw)
import Control.Lens hiding ((*~), contains)
import Control.Monad
import Control.Monad.Catch (
    MonadThrow, MonadCatch, MonadMask, throwM, bracket, onException
  )
import Control.Monad.STM (STM, atomically, retry, throwSTM)
import Control.Monad.Trans.Reader (ReaderT(..))

import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Reader.Class (MonadReader(..))
import Control.Monad.Trans.Maybe (MaybeT(..))
import Control.Monad.Trans (lift)

import Data.Align (align)
import Data.Default (def)
import Data.Fixed (Fixed, E0, div')
import Data.List (foldl')
import qualified Data.Map.Strict as M
--import qualified Data.HashMap.Strict as HM
import Data.Maybe (isJust, listToMaybe, catMaybes, fromMaybe, mapMaybe)
import Data.Proxy
import Data.These (These(..))
import Data.Time (UTCTime, diffUTCTime, getCurrentTime)
import Data.Typeable (Typeable)
import qualified Data.Semigroup as SG
import Linear.V2

import qualified Data.Vector as V
import qualified Data.Vector.Storable         as St
import qualified Data.Vector.Storable.Mutable as Stm
import           Data.Vector.Storable.MMap
import qualified Data.Vector.Generic         as G
import qualified Data.Vector.Generic.Mutable as GM
import qualified Numeric.Units.Dimensional.Prelude as DP
import Numeric.Units.Dimensional.Functor ()
import System.FilePath (joinPath)
import System.IO.Temp (withSystemTempDirectory)
import System.CPUTime (getCPUTime)

import Text.Printf (printf)

import qualified Data.HashPSQ as PQ


concatStrs :: [String] -> String
concatStrs = concat


data AlignAcc k a b =
  AAcc { _aThis  :: !(Maybe a)
       , _aThat  :: !(Maybe b)
       , _aMap   :: [(k,(a,b))]
       }
makeLenses ''AlignAcc



data PointWithRef = PWR
  { pwrPoint :: !(V2 Int)
  , pwrTime  :: !Time
  , pwrRef   :: !(V2 Int)
  }

data RunnerError
  = NoInputs
  | InputsNotInRange
  | NoIgnitedElements
  | InvalidFuelCode Int
  | InternalError String
  | ThreadError BlockIndex SomeException
  deriving (Show, Typeable)

instance Exception RunnerError

data BlockStatus
  = Ready           -- ready for work, no points processed yet
  | Loading         -- thread is loading data
  | Working         -- processing points
  | Done            -- Processed all points, ready for more
  | Errored SomeException -- Something bad happened
  deriving Show


isDone, isReady, isWorking :: BlockStatus -> Bool
isReady Ready = True
isReady _     = False

isWorking Working = True
isWorking Loading = True
isWorking _       = False

isDone Done = True
isDone _              = False

toThreadError :: BlockIndex -> BlockStatus -> Maybe RunnerError
toThreadError bi (Errored e) = Just (ThreadError bi e)
toThreadError _   _           = Nothing

allFinished :: [BlockStatus] -> Bool
allFinished statuses = all isDone allButReady && not (null allButReady)
  where allButReady = filter (not . isReady) statuses


data MessageLevel = Error | Warning | Info | Debug
  deriving Show

data Message = Message UTCTime MessageLevel ThreadId String

printMessage :: Message -> IO ()
printMessage (Message t level tid msg) = do
  printf "%-9s " ("["++show level++"]")
  printf "%-20s %35s: %s\n" ("("++show tid++")") ("("++show t++")") msg


newtype LoaderCache l (d :: * -> *) a =
  LoaderCache (M.Map (Loader l d a) (TMVar (LoaderRaster l d a)))
  deriving Monoid

loaderCache
  :: Lens' (LoaderCache l d a)
           (M.Map (Loader l d a) (TMVar (LoaderRaster l d a)))
loaderCache = lens (\(LoaderCache c) -> c) (const LoaderCache)


data Block l =
  Block {
    _blIndex         :: !BlockIndex
  , _blGeoReference  :: !GeoReference
  , _blFires         :: !(Stm.IOVector (Fire (V2 Int)))
  , _blIncomingFires :: !(TQueue PointWithRef)
  , _blStatus        :: !(TVar BlockStatus)
  , _blCurTime       :: !(TVar Time)
  , _blLoaderCache   :: !(TVar (PropagInputs (LoaderCache l)))
  }
makeLenses ''Block

blExtent :: Getting Extent (Block l) Extent
blExtent = blGeoReference.to geoRefExtent

instance Show (Block l)  where
  show bl = concatStrs [
      "Block { blIndex  = " , show (bl^.blIndex), ", "
            , "blExtent = " , show (bl^.blExtent), " }"]


type FireMap   l = M.Map BlockIndex (Block l)



data SimulatorEnv l =
  SimulatorEnv {
    _envFireMap      :: !(TMVar (FireMap l ))
  , _envOrigin       :: !(V2 Double)
  , _envPixelSize    :: !PixelSize
  , _envBlockSize    :: !BlockSize
  , _envMaxTime      :: !Time
  , _envMessages     :: !(TQueue Message)
  , _envSimilarFires :: !(Fire (V2 Int) -> Fire (V2 Int) -> Bool)
  , _envFuels        :: !(Catalog PreparedFuel)
  , _envInputs       :: !(M.Map Time (PropagInputs (Loader l )))
  , _envWorkDir      :: !FilePath
  }
makeLenses ''SimulatorEnv


newtype Simulator l a = Simulator (ReaderT (SimulatorEnv l ) IO a)
  deriving ( Functor, Applicative, Monad, MonadThrow, MonadCatch, MonadMask
           , MonadIO, MonadReader (SimulatorEnv l ))

runSimulator :: SimulatorEnv l -> Simulator l a -> IO a
runSimulator env (Simulator act) = runReaderT act env

propagate
  :: forall l. HasLoadExtent l
  => PropagIOConfig l
  -> InitialPropagConfig
  -> IO PropagationResult
propagate ioCfg iPc = do
  pc <- prepareAndValidateConfig ioCfg iPc
  withSystemTempDirectory "propag-work" $ \workDir -> do
    simEnvironment <- SimulatorEnv
      <$> newTMVarIO mempty
      <*> justOrFail NoIgnitedElements (calculateOrigin pc)
      <*> pure (pc^.pixelSize)
      <*> pure (pc^.blockSize)
      <*> pure (pc^.maxTime)
      <*> newTQueueIO
      <*> pure (similarFires (pc^.uniformityConditions))
      <*> pure (prepareCatalog (pc^.fuelCatalog))
      <*> pure (alignInputs (pc^.inputs))
      <*> pure workDir
    runSimulator simEnvironment $ do
      void startLoggerThread
      queueInitialElements (pc^.initialElements)
      waitThreadsAreDone
      makePropagationResult iPc
{-# INLINABLE propagate #-}

makePropagationResult
  :: forall l.  InitialPropagConfig -> Simulator l PropagationResult
makePropagationResult pc = do
  bMap     <- getFireMap
  pxSize   <- view envPixelSize
  orig     <- view envOrigin
  backward <- getBackward
  let totalGeoRef  = southUpGeoReference totalExtent pxSize
      totalExtent  = V.foldl' (SG.<>) (Extent orig orig) extents
      extents      = V.map (^.blExtent) (V.fromList (M.elems bMap))
      ixs          = M.keys bMap
      ixOrigin     = fmap minimum (V2 (ixs^..traverse._x) (ixs^..traverse._y))
      pxs2pts      = St.map (fOrigin %~ backward)
  blocks <- fmap M.fromList $ forM (M.toList bMap) $ \(idx, bl) -> do
    vec <- pxs2pts <$> liftIO (St.unsafeFreeze (bl^.blFires))
    return (idx-ixOrigin, Raster (bl^.blGeoReference) vec)
  return PropagationResult {
      _prGeoReference = totalGeoRef
    , _prConfig       = pc
    , _prBlockCount   = M.size bMap
    , _prBlocks       = blocks
    }
{-# INLINE makePropagationResult #-}

startLoggerThread :: Simulator l ThreadId
startLoggerThread = do
  chan <- view envMessages
  liftIO $ forkIO $ forever (printMessage =<< atomically (readTQueue chan))

waitThreadsAreDone :: Simulator l ()
waitThreadsAreDone = do
  formatElapsedClockCpu <- getElapsedTimeFormatter
  tMap <- view envFireMap
  liftIO $ do
    atomically $ do
      statusMap <- mapM (readTVar . (^.blStatus))  =<< readTMVar tMap
      let finished = allFinished (M.elems statusMap)
          errors = mapMaybe (uncurry toThreadError) (M.toList statusMap)
      case (listToMaybe errors, finished) of
        (Just e, _   ) -> throwSTM e
        (_     , True) -> return ()
        _              -> retry
    putStrLn "\n\nFinished simulation"
    formatElapsedClockCpu
      "Total clock time: %12.2fs\nTotal CPU time: %12.2fs\n\n"


--
-- Initialization stuff
--

prepareAndValidateConfig
  :: HasLoadExtent l
  => PropagIOConfig l
  -> PropagConfig (InputMap TimeSpec (Input Template)) UTCTime
  -> IO (PropagConfig (InputMap Time (Loader l)) Time)
prepareAndValidateConfig ioCfg
  =     justOrFail NoInputs
  <=< ( initializeLoaders ioCfg . filterOutOfRangeInputs)
  <=< ( justOrFail NoIgnitedElements . timestampsToTimes)
  <=< ( justOrFail NoIgnitedElements . explodeAndInterpolate . roundInitialElementVertices)

filterOutOfRangeInputs
  :: PropagConfig (InputMap Time s) Time
  -> PropagConfig (InputMap Time s) Time
filterOutOfRangeInputs pc = pc & inputs %~ filterInputsInRange _0 (pc^.maxTime)

roundInitialElementVertices :: PropagConfig f t -> PropagConfig f t
roundInitialElementVertices pc = pc & initialElements.traverse.geometry %~ toPixelCenter
  where
    pxSize        = unPixelSize (pc^.pixelSize)
    toPixelCenter = (*pxSize) . fmap ((+0.5) . fromIntegral . ifloor)
                  . (/pxSize)
    ifloor        = floor :: Double -> Int

calculateOrigin :: PropagConfig f t -> Maybe (V2 Double)
calculateOrigin pc = do
  x0 <- minimumOf (verticesLens._x) pc
  x1 <- maximumOf (verticesLens._x) pc
  y0 <- minimumOf (verticesLens._y) pc
  y1 <- maximumOf (verticesLens._y) pc
  return (roundToBlkCntr (V2 (x1-x0) (y1-y0)))
  where
    verticesLens = initialElements.traverse.geometry

    roundToBlkCntr
      = (*pxSize)
      . fmap fromIntegral
      . (*bSize)
      . liftA2 (flip divFixed) (fmap fromIntegral bSize)
      . fmap realToFrac
      . (/ pxSize)
    divFixed :: Fixed E0 -> Fixed E0 -> Int --asume crs en metros
    divFixed = div'
    pxSize = unPixelSize (pc^.pixelSize)
    bSize = unBlockSize (pc^.blockSize)




alignInputs
  :: Ord k => PropagInputs (InputMap k s) -> M.Map k (PropagInputs s)
alignInputs (PropagInputs a b c d e f g h i j) = M.map toInputs aligned
  where
    toInputs (((((((((a', b'), c'), d'), e'), f'), g'), h'), i'), j') =
      PropagInputs a' b' c' d' e' f' g' h' i' j'
    aligned =  unInputMap a
            .| unInputMap b
            .| unInputMap c
            .| unInputMap d
            .| unInputMap e
            .| unInputMap f
            .| unInputMap g
            .| unInputMap h
            .| unInputMap i
            .| unInputMap j
    (.|) :: Ord k => M.Map k a -> M.Map k b -> M.Map k (a,b)
    (.|) m = views aMap (M.fromDistinctAscList . reverse)
           . M.foldlWithKey' go initialAcc . align m
      where
        initialAcc = AAcc Nothing Nothing []
        go z@AAcc{_aThat=Nothing} _ (This u)    = z & aThis .~ Just u
        go z@AAcc{_aThat=Just w}  k (This u)    = z & aThis .~ Just u
                                                    & aMap  %~ ((k,(u,w)):)

        go z@AAcc{_aThis=Nothing} _ (That w)    = z & aThat .~ Just w
        go z@AAcc{_aThis=Just u}  k (That w)    = z & aThat .~ Just w
                                                    & aMap  %~ ((k,(u,w)):)

        go z                      k (These u w) = z & aThat .~ Just w
                                                    & aThis .~ Just u
                                                    & aMap  %~ ((k,(u,w)):)

timestampsToTimes
  :: PropagConfig (InputMap UTCTime s) UTCTime
  -> Maybe (PropagConfig (InputMap Time s) Time)
timestampsToTimes m = do
  initialTime <- minimumOf initialTimes m
  let toTime = (*~ DP.second) . realToFrac . (`diffUTCTime` initialTime)
  return $
    m & initialTimes %~ toTime
      & inputsLens   %~ mapInputs (inputMap %~ M.mapKeys toTime)

queueInitialElements
  :: (Foldable t, HasLoadExtent l)
  => t (IgnitedElement Time) -> Simulator l ()
queueInitialElements elems = do
  fw <- getForward
  mapM_ (\(IgnitedElement (fw->p) t) -> sendPointToTheBlockItBelongsTo (PWR p t p)) elems

filterInputsInRange
  :: Time
  -> Time
  -> PropagInputs (InputMap Time f)
  -> PropagInputs (InputMap Time f)
filterInputsInRange t0 t1 = mapInputs (inputMap %~ go)
  where
    go :: M.Map Time a -> M.Map Time a
    go m = case M.lookupLE t0 m of
             Just (t0',_) -> M.filterWithKey (\t _ -> t0'<= t && t<t1) m
             Nothing      -> M.empty



initializeLoaders
  :: forall k l.  HasLoadExtent l
  => PropagIOConfig l
  -> PropagConfig (InputMap k (Input String)) k
  -> IO (Maybe (PropagConfig (InputMap k (Loader l)) k))
initializeLoaders ioCfg pc = runMaybeT $ do
  is <- mapMInputs ioCfg go (pc^.inputs)
  return (pc & inputsLens .~ is)
  where
    go :: forall d a. CanSerialize l a
       => InputMap k (Input String) d a
       -> MaybeT IO (InputMap k (Loader l) d a)
    go = fmap InputMap . mapChk (mkLoader ioCfg) . unInputMap
    mapChk f m
      | M.null m  = MaybeT (return Nothing)
      | otherwise = lift (mapM f m)




-----------------------------------------------------------------------------
-- Automata stuff
-----------------------------------------------------------------------------

loadRaster
  :: forall l d a. HasLoadExtent l
  => (MessageLevel -> String -> IO ())
  -> Block l
  -> (Time, PropagInputs (Loader l))
  -> (forall s. Lens' (PropagInputs s) (s d a))
  -> IO (AsyncRaster l d a)
loadRaster emit bl (t, loaders) lns = join $ atomically $ do
  cache <-  readTVar (bl^.blLoaderCache)
  case loader `M.lookup` (cache^.lns.loaderCache) of
    Just mVar -> return (waitForIt mVar)
    Nothing   -> do
      mVar <- newEmptyTMVar
      modifyTVar' (bl^.blLoaderCache)
                  (lns.loaderCache %~ M.insert loader mVar)
      return $ do
        emit Debug $ printf "Loading %s for %.0fm" varName asMins
        void $ forkOSFinally (runLoader loader geoRef) $ \case
          Left e  -> atomically (putTMVar mVar (throw e))
          Right r -> atomically (putTMVar mVar r)
        waitForIt mVar
  where
    waitForIt = return . AsyncRaster . atomically . readTMVar
    loader  = loaders^.lns
    asMins  = t DP./~ minute
    varName = getName (def^.lns)
    geoRef  = bl^.blGeoReference

newtype AsyncRaster l d a =
  AsyncRaster { waitRaster :: IO (LoaderRaster l d a) }

loadInputs
  :: forall l. HasLoadExtent l
  => Block l
  -> Time
  -> Simulator l (PropagInputs (LoaderRaster l))
loadInputs bl t = do
  emit <- getEmitMessage
  loaders <- getLoadersForTime t
  let load :: forall d a. (forall s. Lens' (PropagInputs s) (s d a))
           -> IO (AsyncRaster l d a)
      load = loadRaster emit bl loaders
  liftIO $ do
    rs <- PropagInputs <$> load d1hr
                       <*> load d10hr
                       <*> load d100hr
                       <*> load herb
                       <*> load wood
                       <*> load windSpeed
                       <*> load windBearing
                       <*> load slope
                       <*> load aspect
                       <*> load fuel
    mapMInputs (Proxy :: Proxy l) waitRaster rs

getLoadersForTime
  :: forall l. Time -> Simulator l (Time, PropagInputs (Loader l))
getLoadersForTime t =
  justOrFail (InternalError "getLoadersForTime: no inputs found")
  =<< fmap (M.lookupLE t) (view envInputs)


loadFire
  :: forall l. HasLoadExtent l
  => Block l
  -> Int     -- offset
  -> V2 Int  -- ref
  -> Time
  -> Simulator l (Fire (V2 Int))
loadFire bl off ref t = withStatus bl Loading $ do
  is <- loadInputs bl t
  fuels <- view envFuels
  let load :: (IsUnit d a, CanSerialize l a)
           => Lens' (PropagInputs (LoaderRaster l))
                    (LoaderRaster l d a)
           -> Maybe (UnitQuantity d a)
      load lns = (is^.lns) `indexLoaderRaster` off
      mFire = do
        code <- fromIntegral <$> load fuel
        if code == 0 then return NonPropagating else do
          spEnv <- SpreadEnv
            <$> load d1hr
            <*> load d10hr
            <*> load d100hr
            <*> load herb
            <*> load wood
            <*> (effectiveWindSpeed <$> pure code <*> load windSpeed)
            <*> fmap toAzimuth (load windBearing)
            <*> load slope
            <*> fmap toAzimuth (load aspect)
          fuelComb@(_,comb) <- indexCatalog fuels code
          let sp    = spread2 fuelComb spEnv
              rTime = combResidenceTime comb
          return (Propagating code t ref sp spEnv rTime)
  return (fromMaybe NoValidData mFire)


canReuseReference
  :: forall l. HasLoadExtent l
  => V2 Int -> V2 Int -> Simulator l Bool
canReuseReference pxA pxB | pxA==pxB = return True
canReuseReference pxA pxB = do
  mBlockOff <- getBlockPixelAndOffset Nothing pxA
  case mBlockOff of
    Nothing -> throwM (InternalError "canReuseReference: no node for pA")
    Just (blA,_,offA) -> do
      fireA <- liftIO ((blA^.blFires) `GM.read` offA)
      isSimilar <- view envSimilarFires
      timeCalculator <- getTimeCalculator "canReuseReference" blA pxA
      let go _      []        = return True
          go blHint (px:rest) = do
            mBlockOff' <- getBlockPixelAndOffset (Just blHint) px
            case mBlockOff' of
              Just (bl, _, off) -> do
                fire <- do
                  fire <- liftIO ((bl^.blFires) `GM.read` off)
                  case fire of
                    NotAccessed -> loadFire bl off pxA (timeCalculator px)
                    _ -> return fire
                if isSimilar fireA fire then go bl rest else return False
              Nothing -> throwM $
                InternalError "canReuseReference: did not find a valid node"
      go blA (dda pxA pxB)


getIncomingPoints
  :: HasLoadExtent l => Block l -> Simulator l [PointWithRef]
getIncomingPoints bl@Block{_blIncomingFires=chan} =
  savePointsOnBlock bl =<< liftIO (atomically (readAllAvailableElements chan))


saveIgnitedPoint
  :: HasLoadExtent l => Block l -> PointWithRef -> Simulator l Bool
saveIgnitedPoint bl pwr@PWR{pwrPoint=p} = do
  forward <- getToBlockIx
  let bix = forward p
  if bl^.blIndex == bix
     then isJust <$> savePointOnBlock bl pwr
     else sendPointToTheBlockItBelongsTo pwr >> return False



savePointsOnBlock
  :: HasLoadExtent l
  => Block l
  -> [PointWithRef]
  -> Simulator l [PointWithRef]
savePointsOnBlock bl = fmap catMaybes . mapM (savePointOnBlock bl)


getOrCreateBlock
  :: HasLoadExtent l
  => BlockIndex -> Simulator l (Block l)
getOrCreateBlock blockIx = do
  fireMap <- view envFireMap
  eBl <- liftIO $ atomically $ do
    bMap <- readTMVar fireMap
    case blockIx `M.lookup` bMap of
      Just bl -> return (Right bl)
      Nothing -> do
        bMap' <- takeTMVar fireMap
        return (Left bMap')
  case eBl of
    Right bl -> return bl
    Left bMap -> (do
      bl <- newEmptyBlock blockIx
      _ <- automataThread bl
      let bMap' = M.insert blockIx bl bMap
      liftIO (atomically (putTMVar fireMap bMap'))
      return bl
      ) `onException` liftIO (atomically (putTMVar fireMap bMap))

sendPointToTheBlockItBelongsTo
  :: HasLoadExtent l => PointWithRef -> Simulator l ()
sendPointToTheBlockItBelongsTo !(pwr@PWR{pwrPoint=p}) = do
  forward <- getToBlockIx
  let blockIx = forward p
  bl <- getOrCreateBlock blockIx
  liftIO (atomically (writeTQueue (bl^.blIncomingFires) pwr))


newEmptyBlock :: BlockIndex -> Simulator l (Block l)
newEmptyBlock blockIx = do
  geoRef <- blockGeoReference blockIx
  env <- ask
  liftIO $ Block
    <$> pure blockIx
    <*> pure geoRef
    <*> newFireVector env blockIx
    <*> newTQueueIO
    <*> newTVarIO Ready
    <*> newTVarIO _0
    <*> newTVarIO mempty

newFireVector
  :: SimulatorEnv l -> BlockIndex -> IO (Stm.IOVector (Fire (V2 Int)))
newFireVector env blockIx = do
  let size = env^.envBlockSize.to unBlockSize.to product
      path = joinPath [ env^.envWorkDir
                      , printf "%d_%d.bin" (blockIx^._x) (blockIx^._y)]

  vec <- unsafeMMapMVector path ReadWriteEx (Just (0, size))
  GM.set vec NotAccessed
  return vec


-- Guarda un PointWithRef en el raster del nodo y actualiza su spreadinfo.
-- Lo devuelve si lo ha guardado o Nothing si decide no hacerlo.
-- (eg: por ser incombustible)
savePointOnBlock
  :: HasLoadExtent l
  => Block l -> PointWithRef
  -> Simulator l (Maybe PointWithRef)
savePointOnBlock bl pwr@PWR{pwrPoint=p, pwrTime=t,pwrRef=ref} = do
  forward <- getToBlockPixelAndOffset
  let (bix,_,off) = forward p
      loadAndSave = do
        newFire <- loadFire bl off ref t
        liftIO (GM.write (bl^.blFires) off newFire)
        return (if propagates newFire then Just pwr else Nothing)
  assert ((bl^.blIndex) == bix) (return ())
  curFire <- liftIO ((bl^.blFires) `GM.read` off)
  tMax <- view envMaxTime
  case curFire of
    NotAccessed            | t < tMax -> loadAndSave
    Propagating{_fTime=t'} | t < t'   -> loadAndSave
    _                                 -> return Nothing




automataThread
  :: forall l. HasLoadExtent l
  => Block l -> Simulator l ThreadId
automataThread bl = do
  env <- ask
  liftIO $ forkFinally (runSimulator env (simulate PQ.empty)) $ \case
    Left e -> atomically (writeTVar (bl^.blStatus) (Errored e))
    Right () -> return ()
  where
    simulate !(PQ.minView -> Just (pA, timeA, pRcand, queue)) = do
      liftIO (atomically (writeTVar (bl^.blCurTime) timeA))
      --Debug (timeA
      canReuse <- canReuseReference pRcand pA
      let pR = if canReuse then pRcand else pA
          ngs = neighbors pA
      timeCalculator <- getTimeCalculator "simulate" bl pR
      queue' <- foldM (stepNeighbors timeCalculator pR) queue ngs
      simulate . insertElems queue' =<< getIncomingPoints bl

    simulate !queue = do
      liftIO $ do
        atomically $ do
          curStatus <- readTVar (bl^.blStatus)
          noWork <- isEmptyTQueue (bl^.blIncomingFires)
          when (isWorking curStatus && noWork) (writeTVar (bl^.blStatus) Done)
        atomically $ do
          void (peekTQueue (bl^.blIncomingFires)) -- block until work arrives
          writeTVar (bl^.blStatus) Working
      simulate . insertElems queue =<< getIncomingPoints bl

    stepNeighbors timeCalculator !pR !queue !pB
      | pB == pR  = return queue
      | otherwise = do
          let timeB = timeCalculator pB
          queueIt <- saveIgnitedPoint bl (PWR pB timeB pR)
          if queueIt
             then return (PQ.insert pB timeB pR queue)
             else return queue

    neighbors (V2 x y) =
      [V2 (x + i) (y + j) | i <- [-1,0,1], j <- [-1,0,1], not (i==0 && j==0)]

    insertElems = foldl' (\q (PWR p t r) -> PQ.insert p t r q)



getTimeCalculator
  :: String -> Block l -> V2 Int -> Simulator l (V2 Int -> Time)
getTimeCalculator requester blHint pA = do
  mBlockOff <- getBlockPixelAndOffset (Just blHint) pA
  case mBlockOff of
    Just i@(bl, _, off) -> do
      fire <- liftIO ((bl^.blFires) `GM.read` off)
      case fire of
        Propagating{_fTime=t, _fSpread=sp}  -> do
          pxSize <- view envPixelSize
          return (calculator (unPixelSize pxSize) t sp)
        f -> gtcError ("expected a Propagating fire, got: "++show f) (Just i)
    Nothing -> gtcError "expected to find a node" Nothing

  where
    calculator pxSize timeA sp pB
      | speed > _0 = timeA DP.+ (dist DP./ speed)
      | otherwise  = timeA DP.+ (999 DP.*~ year)
      where
        speed = getSpreadAtAzimuth sp pA pB ^. sSpeed
        dist  = distanceMeters (toReal pA * pxSize)
                               (toReal pB * pxSize)
        toReal = fmap fromIntegral :: V2 Int -> V2 Double

    gtcError msg (Just (bl,px,off)) = do
      emit <- getEmitMessage
      emit Error (printf (concatStrs [
          "getTimeCalculator via %s: %s\n"
        , "\tblock: %s\n"
        , "\tpx: %s\n"
        , "\toff: %s\n"
        , "\tpoint: %s"
        ]) requester msg (show bl) (show px) (show off) (show pA))
      throwM (InternalError ("getTimeCalculator: "++msg))

    gtcError msg Nothing = do
      emit <- getEmitMessage
      emit Error msg
      throwM (InternalError ("getTimeCalculator: "++msg))

getSpreadAtAzimuth :: Spread -> V2 Int -> V2 Int -> SpreadAtAzimuth
getSpreadAtAzimuth sp pA pB
  | pA == pB  = SpreadAtAzimuth  { _sSpeed  = sp^.sSpeedMax
                                 , _sByrams = sp^.sByramsMax
                                 , _sFlame  = sp^.sFlameMax}
  | otherwise = spreadAtAzimuth sp az
  where
    az     = azimuth (toReal pA) (toReal pB)
    toReal = fmap fromIntegral :: V2 Int -> V2 Double



effectiveWindSpeed :: Int -> Speed -> Speed
effectiveWindSpeed fCode = fmap ((/ 1.15) . (*factor))
  where
    factor = factors G.! fCode
    factors :: St.Vector Double
    factors = [
      0,    -- dummy
      0.69, -- 1
      0.75,
      0.44,
      0.55,
      0.42,
      0.44,
      0.44,
      0.28,
      0.28,
      0.36,
      0.36,
      0.40,
      0.50
      ]




-----------------------------------------------------------------------------
-- Simulator utils
-----------------------------------------------------------------------------

getFireMap :: Simulator l (FireMap l)
getFireMap = liftIO . atomically . readTMVar =<< view envFireMap


getBackward2 :: Simulator l (BlockIndex -> V2 Int -> V2 Double)
getBackward2 =
  bmBackward2 <$> view envBlockSize <*> view envPixelSize <*> view envOrigin


getForward :: Simulator l (V2 Double -> V2 Int)
getForward = bmForward <$> view envPixelSize <*> view envOrigin

getBackward :: Simulator l (V2 Int -> V2 Double)
getBackward = bmBackward <$> view envPixelSize <*> view envOrigin

getToBlockPixelAndOffset
  :: Simulator l (V2 Int -> (BlockIndex, V2 Int, Int))
getToBlockPixelAndOffset = bmToBlockPixelAndOffset <$> view envBlockSize



getToBlockIx :: Simulator l (V2 Int -> BlockIndex)
getToBlockIx = do
  f <- getToBlockPixelAndOffset
  return ((\(idx,_,_) -> idx) . f)


getBlockPixelAndOffset
  :: Maybe (Block l)
  -> V2 Int
  -> Simulator l (Maybe (Block l, V2 Int, Int))
getBlockPixelAndOffset mHint pA = do
  forward <- getToBlockPixelAndOffset
  let (bi, px, off) = forward pA
  case mHint of
    Just hint
      | (hint^.blIndex) == bi ->
          return (Just (hint, px, off))
    _ -> (fmap (\n -> (n,px,off)) . M.lookup bi) <$> getFireMap


blockGeoReference :: BlockIndex -> Simulator l GeoReference
blockGeoReference blockIx = do
  env <- ask
  backward <- getBackward2
  let origin     = backward blockIx 0
      matrix     = V2 (V2 dx 0) (V2 0 dy)
      bSize      = unBlockSize (env^.envBlockSize)
      (V2 dx dy) = unPixelSize (env^.envPixelSize)
  return (GeoReference (GeoTransform matrix origin) bSize)

getEmitMessage
  :: MonadIO m => Simulator l (MessageLevel -> String -> m ())
getEmitMessage = do
  chan <- view envMessages
  return $ \level msg -> liftIO $ do
    (t,myTid) <- (,) <$> getCurrentTime <*> myThreadId
    atomically (writeTQueue chan (Message t level myTid msg))


withStatus
  :: (MonadMask m, MonadIO m) => Block l -> BlockStatus -> m a -> m a
withStatus bl status = bracket enter exit . const
  where
    enter = liftIO $ atomically $ do
      curStatus <- readTVar (bl^.blStatus)
      writeTVar (bl^.blStatus) status
      return curStatus
    exit = liftIO . atomically . writeTVar (bl^.blStatus)





-----------------------------------------------------------------------------
-- Utils which could live elsewhere
-----------------------------------------------------------------------------

readAllAvailableElements :: TQueue a -> STM [a]
readAllAvailableElements chan = go
  where
    go = maybe (return []) (\e -> fmap (e:) go) =<< tryReadTQueue chan

distanceMeters :: V2 Double -> V2 Double -> DP.Length Double
distanceMeters a b = sqrt (dx*dx+dy*dy) *~ DP.meter
  where V2 dx dy = b - a

azimuth :: V2 Double -> V2 Double -> Azimuth
azimuth (V2 x0 y0) (V2 x1 y1)
  = atan2 (x1-x0) (y1-y0) *~ DP.radian
{-# INLINE azimuth #-}


getElapsedTimeFormatter
  :: (MonadIO m, MonadIO m') => m (String -> m' ())
getElapsedTimeFormatter = liftIO $ do
  (startTime, startCPU) <- (,) <$> getCurrentTime <*> getCPUTime
  return $ \fmt -> liftIO $ do
    curTime <- getCurrentTime
    curCPU <- getCPUTime
    let elapsedClock, elapsedCpu :: Double
        elapsedClock = realToFrac (curTime `diffUTCTime` startTime)
        elapsedCpu   = fromIntegral (curCPU-startCPU) * 1e-12
    printf fmt elapsedClock elapsedCpu
