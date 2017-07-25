{-# LANGUAGE CPP #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}

module Propag.Types (
    Time
  , DTime
  , DistanceUnit
  , Template
  , BlockIndex
  , PropagationResult (..)
  , BlockSize (..)
  , HasDistance (..)
  , HasLoadExtent (..)
  , CanSerialize
  , UniformityCondition (..)
  , PropagConfig (..)
  , InitialPropagConfig
  , InputContainerSatisfies
  , PropagInputs (..)
  , TimeSpec (..)
  , InputMap (..)
  , InputName (..)
  , PropagIOConstraint
  , PropagIONullable
  , FireGetter
  , Fire (..)
  , FireRaster
  , IsUnit (..)
  , PixelSize (..)
  , Loader
  , FuelCode (..)
  , IgnitedElement(..)
  , IgnitedElements
  , Input (..)
  , LoaderRaster (..)
  , PropagRaster
  , indexLoaderRaster
  , inputMap
  , mapInputs
  , mapMInputs
  , tsTime
  , tsStart
  , tsEnd
  , tsStep
  , tsOffset
  , ignitedElementTime
  , ignitedElementGeometry
  , similarFires
  , propagates
  , geometry
  , time
  , mkLoader
  , runLoader

  , HasFuelCatalog (..)
  , HasPixelSize (..)
  , HasCrs (..)
  , HasInitialElements (..)
  , initialTimes
  , HasInputs (..)
  , HasUniformityConditions (..)
  , inputsLens
  , HasMaxTime (..)
  , HasBlockSize (..)
  , HasUnits (..)

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

  , value
  , band
  , uri
  , cache
  , noData
  , layerName
  , attribute

  , explodeInputMap
  , interpolateTemplates
  , explodeAndInterpolate
  , allEqual
  , distanceSmallerThan

  , fTime
  , fOrigin
  , fSpread
  , fSpreadEnv
  , fFuelCode
  , fResidenceTime

  , prGeoReference
  , prBlocks
  , prBlockSize
  , prPixelSize
  , prExtent
  , prConfig
  , prBlockCount

  , per
  , kmh
  , def
  , weaken
  , module Behave.Units
  , module Behave
  , module Numeric.Units.Dimensional.SIUnits
) where

import Propag.BlockMap
import Propag.Template (Template, renderWithTime)
import Propag.Geometry
import Behave
import Behave.Units hiding ((*~), (/~), Time)
import qualified Behave.Units as Behave

import Control.Lens hiding ((.=), (*~))
import Control.Monad
import Control.Monad.IO.Class (MonadIO(liftIO))

import Data.Default (Default(def))
import Data.Maybe (fromMaybe)
import Data.Int (Int16)
import Data.Function (on)
import Data.Hashable (Hashable(..))
import qualified Data.Map.Strict as M
import Data.Text (Text)
import Data.Time (UTCTime, addUTCTime)
import Data.Typeable (Typeable)
import Data.String (IsString)
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Storable as St
import Data.Vector.Missing (Nullable(..))
import Foreign.Storable
import Foreign.Ptr (plusPtr)
import GHC.Exts (Constraint)

import qualified Numeric.Units.Dimensional.Prelude as DP

import Numeric.Units.Dimensional.Variants (Variant(DUnit))
import Numeric.Units.Dimensional (
    Unit
  , Quantity
  , DLength
  , DTime
  , Metricality(..)
  , weaken
  )
import Numeric.Units.Dimensional.SIUnits
import System.Mem.StableName (
    StableName
  , eqStableName
  , hashStableName
  , makeStableName
  )

type family PropagIOConstraint (l :: *) (a :: *) :: Constraint
type family PropagIONullable   (l :: *) (a :: *) :: *

type Time = Quantity DTime Double
type DistanceUnit = Unit 'NonMetric DLength Double

class HasDistance a where
  distance    :: a -> a -> a

  default distance :: Num a => a -> a -> a
  distance a b = abs (a - b)
  {-# INLINE distance #-}

class IsUnit (d :: * -> *) a where
  type UnitQuantity d a :: *

  infixl 7 *~
  (*~) :: a                -> d a -> UnitQuantity d a

  infixl 7 /~
  (/~) :: UnitQuantity d a -> d a -> a

instance Fractional a => IsUnit (Unit k d) a where
  type UnitQuantity (Unit k d) a = Quantity d a
  (*~) = (DP.*~)
  (/~) = (DP./~)
  {-# INLINE (*~) #-}
  {-# INLINE (/~) #-}

instance Num a => HasDistance (Quantity d a) where
  distance a b = DP.abs (a DP.- b)
  {-# INLINE distance #-}

data FuelCode a = FuelCode
  deriving (Eq, Show)

instance IsUnit FuelCode Int16 where
  type UnitQuantity FuelCode Int16 = Int16
  (*~) = const
  (/~) = const
  {-# INLINE (*~) #-}
  {-# INLINE (/~) #-}

instance HasDistance Int16


newtype InputName (d :: * -> *) a = InputName { getName :: String }
  deriving (Eq, Ord, Hashable, IsString)

instance Show (InputName d a) where show = show . getName


newtype InputMap k s (d :: * -> *) a =
  InputMap { unInputMap :: M.Map k (s d a) } deriving Monoid

inputMap :: Lens (InputMap k s d a)
                 (InputMap k' s' d' a')
                 (M.Map k (s d a))
                 (M.Map k' (s' d' a'))
inputMap = lens unInputMap (const InputMap)


deriving instance (Eq k, Eq (s d a)) => Eq (InputMap k s d a)
deriving instance ( Show (s d a)
                  , Show k
                  ) => Show (InputMap k s d a)


instance Ord k => Default (InputMap k s d a) where def = mempty

data TimeSpec
  = TimeStamp {
      _tsTime   :: !UTCTime
    }
  | Interval {
      _tsStart  :: !UTCTime
    , _tsEnd    :: !UTCTime
    , _tsStep   :: !Time
    }
  | Offset {
      _tsOffset :: !Time
  } deriving (Show, Eq, Ord, Typeable)
makeLenses ''TimeSpec






data PropagInputs f =
  PropagInputs {
  -- | Moisture of 1hr lag dead particles
    _d1hr            :: !(f (Unit 'NonMetric DRatio) Double)
  -- | Moisture of 10hr lag dead particles
  , _d10hr           :: !(f (Unit 'NonMetric DRatio) Double)
  -- | Moisture of 100hr lag dead particles
  , _d100hr          :: !(f (Unit 'NonMetric DRatio) Double)
  -- | Moisture of live herbaceous particles
  , _herb            :: !(f (Unit 'NonMetric DRatio) Double)
  -- | Moisture of live woody particles
  , _wood            :: !(f (Unit 'NonMetric DRatio) Double)
  -- | Wind speed
  , _windSpeed       :: !(f (Unit 'NonMetric DVelocity) Double)
  -- | Wind direction azimuth (compass bearing)
  , _windBearing     :: !(f (Unit 'NonMetric DPlaneAngle) Double)
  -- | Terrain slope (rise/reach ratio)
  , _slope           :: !(f (Unit 'NonMetric DRatio) Double)
  -- | Terrain aspect (downslope compass bearing)
  , _aspect          :: !(f (Unit 'NonMetric DPlaneAngle) Double)
  -- | Fuel catalog indexes
  , _fuel            :: !(f FuelCode Int16)
  }
makeLenses ''PropagInputs


mapInputs
  :: (forall d a. f d a -> g d a)
  -> PropagInputs f
  -> PropagInputs g
mapInputs nat (PropagInputs a b c d e f g h i j) =
  PropagInputs (nat a) (nat b) (nat c) (nat d) (nat e)
               (nat f) (nat g) (nat h) (nat i) (nat j)


kmh :: Unit 'NonMetric DVelocity Double
kmh = kilo meter `per` hour

per :: Fractional a
    => Unit m1 d1 a
    -> Unit m2 d2 a
    -> DP.Dimensional ('DUnit 'NonMetric) (d1 DP./ d2) a
per a b = weaken a DP./ weaken b
{-# INLINE per #-}


instance Default (FuelCode Int16) where
  def = FuelCode


type InputContainerSatisfies f (c :: * -> Constraint) =
  ( c (f (Unit 'NonMetric DVelocity) Double)
  , c (f (Unit 'NonMetric DRatio) Double)
  , c (f (Unit 'NonMetric DPlaneAngle) Double)
  , c (f FuelCode Int16)
  )

deriving instance InputContainerSatisfies f Show => Show (PropagInputs f)
deriving instance InputContainerSatisfies f Eq => Eq (PropagInputs f)

instance InputContainerSatisfies f Monoid => Monoid (PropagInputs f) where
  mempty = PropagInputs mempty mempty mempty mempty mempty
                        mempty mempty mempty mempty mempty

  mappend (PropagInputs a b c d e f g h i j)
          (PropagInputs a' b' c' d' e' f' g' h' i' j') =
    PropagInputs (a `mappend` a')
                 (b `mappend` b')
                 (c `mappend` c')
                 (d `mappend` d')
                 (e `mappend` e')
                 (f `mappend` f')
                 (g `mappend` g')
                 (h `mappend` h')
                 (i `mappend` i')
                 (j `mappend` j')


instance {-# OVERLAPPABLE #-} InputContainerSatisfies f Default
  => Default (PropagInputs f) where
  def = PropagInputs def def def def def def def def def def


data IgnitedElement t = IgnitedElement
  { _ignitedElementGeometry :: !(V2 Double)
  , _ignitedElementTime     :: !t
  } deriving (Show, Functor)
makeFields ''IgnitedElement
makeLenses ''IgnitedElement

type IgnitedElements t = [ IgnitedElement t]


data Input s (d :: * -> *) a =
    RasterDataset {
      _inputUri        :: !s
    , _inputBand       :: !Int
    , _inputCache      :: !Bool
    , _inputUnits      :: !(d a)
    }
  | VectorDataset {
      _inputUri       :: !s
    , _inputLayerName :: !(Maybe Text)
    , _inputAttribute :: !Text
    , _inputNoData    :: !a
    , _inputUnits     :: !(d a)
    }
  | Constant {
      _inputValue     :: !a
    , _inputUnits     :: !(d a)
    } deriving Show
makeFields ''Input
makeLensesFor [("_inputUri", "uriLens")] ''Input





data UniformityCondition d a
  = AllEqual
  | DistanceSmallerThan !(UnitQuantity d a)

deriving instance Show (UnitQuantity d a) => Show (UniformityCondition d a)
deriving instance Eq (UnitQuantity d a) => Eq (UniformityCondition d a)



similarQuantities
  :: (HasDistance (UnitQuantity d a), Ord (UnitQuantity d a))
  => UniformityCondition d a
  -> UnitQuantity d a -> UnitQuantity d a
  -> Bool
similarQuantities AllEqual a b = a == b
similarQuantities (DistanceSmallerThan v) a b = a `distance` b < v
{-# INLINE similarQuantities #-}




allEqual :: UniformityCondition d a
allEqual = AllEqual

distanceSmallerThan :: UnitQuantity d a -> UniformityCondition d a
distanceSmallerThan = DistanceSmallerThan


instance Default (PropagInputs UniformityCondition) where
  def = PropagInputs {
    _d1hr        = distanceSmallerThan (10 *~ perCent)
  , _d10hr       = distanceSmallerThan (10 *~ perCent)
  , _d100hr      = distanceSmallerThan (10 *~ perCent)
  , _herb        = distanceSmallerThan (10 *~ perCent)
  , _wood        = distanceSmallerThan (10 *~ perCent)
  , _windSpeed   = distanceSmallerThan (1 *~ (meter DP./ second))
  , _windBearing = distanceSmallerThan (5 *~ degree)
  , _slope       = distanceSmallerThan (0.087488663525923993 *~ perOne)
  , _aspect      = distanceSmallerThan (22.5 *~ degree)
  , _fuel        = allEqual
  }

instance Default (PropagInputs InputName) where
  def = PropagInputs {
    _d1hr        = "1 hour lag dead fuel moisture"
  , _d10hr       = "10 hour lag dead fuel moisture"
  , _d100hr      = "100 hour lag dead fuel moisture"
  , _herb        = "Live herbaceous fuel moisture"
  , _wood        = "Live woody fuel moisture"
  , _windSpeed   = "Wind speed"
  , _windBearing = "Wind bearing"
  , _slope       = "Slope"
  , _aspect      = "Aspect"
  , _fuel        = "Fuel"
  }


data PropagConfig f t =
  PropagConfig {
    _propagConfigInputs               :: !(PropagInputs f)
  , _propagConfigCrs                  :: !Crs
  , _propagConfigUniformityConditions :: !(PropagInputs UniformityCondition)
  , _propagConfigInitialElements      :: !(IgnitedElements t)
  , _propagConfigPixelSize            :: !PixelSize
  , _propagConfigBlockSize            :: !BlockSize
  , _propagConfigMaxTime              :: !Time
  , _propagConfigFuelCatalog          :: !(Catalog Fuel)
  }

makeFields ''PropagConfig

deriving instance ( Show t
         , InputContainerSatisfies f Show
         ) => Show (PropagConfig f t )

instance ( Default (PropagInputs f)
         , Default (PropagInputs UniformityCondition)
         , Monoid  (IgnitedElements t)
         ) => Default (PropagConfig f t ) where
  def = PropagConfig def (EPSG 25830) def mempty (PixelSize 25) (BlockSize 256) (5 *~ hour) def

makeLensesFor [
    ("_propagConfigInputs", "inputsLens")
  , ("_propagConfigInitialElements", "initialElementsLens")
  ] ''PropagConfig



type InitialPropagConfig =
  PropagConfig (InputMap TimeSpec (Input Template)) UTCTime

initialTimes
  :: Traversal (PropagConfig f t1) (PropagConfig f t2) t1 t2
initialTimes = initialElementsLens.traverse.ignitedElementTime


data Fire ref
  = Propagating {
      _fFuelCode        :: !Int
    , _fTime            :: !Time
    , _fOrigin          :: !ref
    , _fSpread          :: !Spread
    , _fSpreadEnv       :: !SpreadEnv
    , _fResidenceTime   :: !Time
    }
  | NonPropagating
  | NotAccessed
  | NoValidData
  deriving Show
makeLenses ''Fire

type FireGetter ref a =
  forall f. (Contravariant f, Functor f, Applicative f)
  => (a -> f a) -> Fire ref -> f (Fire ref)



instance Storable ref => Storable (Fire ref) where
  {-# INLINE sizeOf #-}
  sizeOf    _ = sizeOf (undefined :: Int)
              + sizeOf (undefined :: Time)
              + sizeOf (undefined :: ref)
              + sizeOf (undefined :: Spread)
              + sizeOf (undefined :: SpreadEnv)
              + sizeOf (undefined :: Time)

  {-# INLINE alignment #-}
  alignment _ = alignment (undefined :: Double)

  {-# INLINE peekElemOff #-}
  peekElemOff p idx = do
    f <- peek p0
    case f of
      0    -> return NonPropagating
      (-1) -> return NotAccessed
      (-2) -> return NoValidData
      _    -> Propagating <$> pure f  <*> peek p1 <*> peek p2
                          <*> peek p3 <*> peek p4 <*> peek p5
    where
      !p0 = p  `plusPtr` (idx * sizeOf (undefined :: Fire ref))
      !p1 = p0 `plusPtr` (sizeOf (undefined :: Int))
      !p2 = p1 `plusPtr` (sizeOf (undefined :: Time))
      !p3 = p2 `plusPtr` (sizeOf (undefined :: ref))
      !p4 = p3 `plusPtr` (sizeOf (undefined :: Spread))
      !p5 = p4 `plusPtr` (sizeOf (undefined :: SpreadEnv))

  {-# INLINE pokeElemOff #-}
  pokeElemOff p idx = \case
    NonPropagating          -> poke p0 0
    NotAccessed             -> poke p0 (-1)
    NoValidData             -> poke p0 (-2)
    Propagating a b c d e f -> poke p0 a
                            >> poke p1 b
                            >> poke p2 c
                            >> poke p3 d
                            >> poke p4 e
                            >> poke p5 f
    where
      !p0 = p  `plusPtr` (idx * sizeOf (undefined :: Fire ref))
      !p1 = p0 `plusPtr` (sizeOf (undefined :: Int))
      !p2 = p1 `plusPtr` (sizeOf (undefined :: Time))
      !p3 = p2 `plusPtr` (sizeOf (undefined :: ref))
      !p4 = p3 `plusPtr` (sizeOf (undefined :: Spread))
      !p5 = p4 `plusPtr` (sizeOf (undefined :: SpreadEnv))

propagates :: Fire ref -> Bool
propagates Propagating{} = True
propagates _             = False
{-# INLINE propagates #-}



similarFires
  :: forall ref. PropagInputs UniformityCondition
  -> Fire ref -> Fire ref -> Bool
similarFires pis a b =
     match fuel        (fFuelCode. to fromIntegral)
  && match d1hr        (fSpreadEnv.seD1hr)
  && match d10hr       (fSpreadEnv.seD10hr)
  && match d100hr      (fSpreadEnv.seD100hr)
  && match herb        (fSpreadEnv.seHerb)
  && match wood        (fSpreadEnv.seWood)
  && match windSpeed   (fSpreadEnv.seWindSpeed)
  && match windBearing (fSpreadEnv.seWindAzimuth)
  && match slope       (fSpreadEnv.seSlope)
  && match aspect      (fSpreadEnv.seAspect)
  where
    match
      :: forall d a. (Ord (UnitQuantity d a), HasDistance (UnitQuantity d a))
      => Getter (PropagInputs UniformityCondition) (UniformityCondition d a)
      -> FireGetter ref (UnitQuantity d a)
      -> Bool
    match l1 l2 = fromMaybe False $
      similarQuantities <$> pure (pis^.l1) <*> (a^?l2) <*> (b^?l2)
    {-# INLINE match #-}
{-# INLINE similarFires #-}



type FireRaster = Raster St.Vector (Fire (V2 Double))

data PropagationResult =
  PropagationResult {
    _prGeoReference :: GeoReference
  , _prConfig       :: InitialPropagConfig
  , _prBlockCount   :: Int
  , _prBlocks       :: M.Map BlockIndex FireRaster
  }
makeLenses ''PropagationResult

prBlockSize :: Getter PropagationResult BlockSize
prBlockSize = prConfig . blockSize

prPixelSize :: Getter PropagationResult PixelSize
prPixelSize
  = prGeoReference
  . to (\gr -> let V2 (V2 dx _) (V2 _ dy) = gtMatrix (grTransform gr)
              in PixelSize (V2 dx dy))

prExtent :: Getter PropagationResult Extent
prExtent = prGeoReference . to geoRefExtent


type CanSerialize l a =
  (( PropagIOConstraint l a
  , Elem (PropagIONullable l a) ~ a
  , G.Vector (PropagIOVector l) (PropagIONullable l a)
  , Nullable (PropagIONullable l a)
  ) :: Constraint)

type CanSerializePropagTypes l =
   ( CanSerialize l Double
   , CanSerialize l Int16
   )


class CanSerializePropagTypes l => HasLoadExtent l where

  type PropagIOVector  l :: * -> *
  data PropagIOConfig  l :: *

  loadExtent
    :: PropagIOConstraint l a
    => PropagIOConfig l
    -> Input String d a
    -> GeoReference
    -> IO (PropagRaster l a)


data Loader l d a =
  Loader { loaderSN     :: !(StableName (Input String d a))
         , loaderSNHash :: !Int
         , runLoader    :: GeoReference -> IO (LoaderRaster l d a)
         }

instance Eq (Loader l d a) where
  (==) = eqStableName `on` loaderSN
  {-# INLINE (==) #-}

instance Ord (Loader l d a) where
  compare a b =
    if a == b
    then EQ
    else (compare `on` loaderSNHash) a b
  {-# INLINE compare #-}

instance Hashable (Loader l d a) where
  hashWithSalt i = hashWithSalt i . loaderSNHash
  {-# INLINE hashWithSalt #-}

mkLoader
  :: forall l m d a.
  ( HasLoadExtent l
  , CanSerialize l a
  , MonadIO m
  )
  => PropagIOConfig l
  -> Input String d a
  -> m (Loader l d a)
mkLoader cfg !i = do
  sn <- liftIO (makeStableName i)
  return (
    Loader sn (hashStableName sn) (liftM mkLoaderRaster . loadExtent cfg i)
    )
  where mkLoaderRaster r = LoaderRaster (i^.units, r)





type PropagRaster l a = Raster (PropagIOVector l) (PropagIONullable l a)

newtype LoaderRaster l (d :: * -> *) a =
  LoaderRaster (d a, PropagRaster l a)

indexLoaderRaster
  :: (IsUnit d a, CanSerialize l a)
  => LoaderRaster l d a -> Int -> Maybe (UnitQuantity d a)
indexLoaderRaster (LoaderRaster (u,Raster _ v)) =
  nullable Nothing (Just . (*~ u)) . (G.!) v
{-# INLINE indexLoaderRaster #-}





mapMInputs
  :: forall proxy l f g m. (Applicative m, CanSerializePropagTypes l)
  => proxy l
  -> (forall d a. CanSerialize l a => f d a -> m (g d a))
  -> PropagInputs f
  -> m (PropagInputs g)
mapMInputs _ nat (PropagInputs a b c d e f g h i j) =
  PropagInputs <$> nat a <*> nat b <*> nat c <*> nat d <*> nat e
               <*> nat f <*> nat g <*> nat h <*> nat i <*> nat j



interpolateTemplates
  :: PropagConfig (InputMap UTCTime (Input Template)) UTCTime
  -> PropagConfig (InputMap UTCTime (Input String  )) UTCTime
interpolateTemplates = inputsLens %~ mapInputs (inputMap %~ M.mapWithKey go)
  where
    go t i@RasterDataset{}      = i & uriLens %~ renderWithTime t
    go t i@VectorDataset{}      = i & uriLens %~ renderWithTime t
    go _ (Constant a b)         = Constant a b

explodeInputMap
  :: UTCTime -> InputMap TimeSpec s d a -> InputMap UTCTime s d a
explodeInputMap baseTime =
  InputMap . M.fromList . concat . map go . M.toList . unInputMap
  where
    go (TimeStamp k, v) = [(k,v)]
    go (Offset k, v)    = [(addOff k baseTime, v)]
    go (Interval _ _ s, _) | s <= _0 = [] -- cowardly refuse to loop forever
    go (Interval f t s, v) =
      flip zip (repeat v) $ takeWhile (<=t) $ iterate (addOff s) f
    addOff k = addUTCTime (realToFrac (k /~ second))

explodeAndInterpolate
  :: PropagConfig (InputMap TimeSpec (Input Template)) UTCTime
  -> Maybe (PropagConfig (InputMap UTCTime (Input String)) UTCTime )
explodeAndInterpolate pc = do
  baseTime <- minimumOf initialTimes pc
  return $ interpolateTemplates
         $ inputsLens %~ mapInputs (explodeInputMap baseTime)
         $ pc
