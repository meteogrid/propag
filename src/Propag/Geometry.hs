{-# LANGUAGE BangPatterns #-}
module Propag.Geometry (
  module Propag.Geometry
, module Linear.V2
) where

import Control.Exception (assert)
import qualified Data.Semigroup as SG
import Linear.V2 (V2(..))
import Linear.Epsilon (nearZero)

data Extent = Extent
  { eMin :: !(V2 Double)
  , eMax :: !(V2 Double)
  } deriving (Eq, Ord, Show)

data Crs = EPSG  !Int
         | Proj4 !String
  deriving (Eq, Ord, Show)

instance SG.Semigroup Extent where
  Extent a0 a1 <> Extent b0 b1 = Extent (min <$> a0 <*> b0) (max <$> a1 <*> b1)

eSize :: Extent -> V2 Double
eSize (Extent lo hi) = hi - lo

data GeoTransform = GeoTransform
  { gtMatrix :: !(V2 (V2 Double))
  , gtOrigin :: !(V2 Double)
  } deriving (Eq, Ord, Show)

data GeoReference = GeoReference
  { grTransform :: !GeoTransform
  , grSize      :: !(V2 Int)
  , grCrs       :: !Crs
  } deriving (Eq, Ord, Show)

data Raster v a = Raster
  { rGeoReference :: !GeoReference
  , rData         :: !(v a)
  } deriving (Eq, Show)

newtype PixelSize = PixelSize {unPixelSize :: V2 Double}
  deriving (Eq, Ord, Show)

geoRefExtent :: GeoReference -> Extent 
geoRefExtent (GeoReference (GeoTransform (V2 (V2 dx xr) (V2 yr dy)) o) sz _)
  = Extent o (assert (dx>0 && dy>0 && xr==0 && yr==0)
                     (o + (fmap fromIntegral sz * V2 dx dy)))

buffer :: V2 Double -> Extent -> Extent
buffer p (Extent lo hi) = Extent (lo - p) (hi + p)


-- | based on http://www.cse.chalmers.se/edu/year/2010/course/TDA361/grid.pdf
dda :: V2 Int -> V2 Int -> [V2 Int]
dda pA pB@(V2 x1 y1)
  | step == 0 = [pA]
  | otherwise = go pA (V2 tMaxX0 tMaxY0)
  where
    go p@(V2 x y) (V2 tMaxX tMaxY)
    {-
      | traceShow ( "go"
                  , ("ab", pA, pB)
                  , ("p" , x, y)
                  , ("xs", tMaxX, deltaX)
                  , ("ys", tMaxY, deltaY)
                  ) False = undefined
    -}
      | not (validX x && validY y) = []
      | nearZero (tMaxX-tMaxY)     = p : go (V2 (x+stepX)      (y+stepY))
                                            (V2 (tMaxX+deltaX) (tMaxY+deltaY))
      | tMaxX < tMaxY              = p : go (V2 (x+stepX)      y             )
                                            (V2 (tMaxX+deltaX) tMaxY         )
      | otherwise                  = p : go (V2 x              (y+stepY)     )
                                            (V2 tMaxX          (tMaxY+deltaY))

    !step@(V2 stepX stepY) = signum (pB - pA)

    !(V2 dx dy) = fmap fromIntegral (pB - pA) :: V2 Double
    !deltaY     = abs (1/dy)
    !deltaX     = abs (1/dx)
    !tMaxX0     = abs (1/dx)
    !tMaxY0     = abs (1/dy)

    validX = if stepX > 0 then (<=x1) else (>=x1)
    validY = if stepY > 0 then (<=y1) else (>=y1)

southUpGeoReference :: Extent -> PixelSize -> Crs -> GeoReference
southUpGeoReference ext (PixelSize pz@(V2 dx dy)) crs = GeoReference
  { grSize      = fmap round (eSize ext / pz)
  , grTransform = GeoTransform { gtOrigin = eMin ext
                               , gtMatrix = V2 (V2 dx 0) (V2 0 dy)
                               }
  , grCrs = crs
  }
