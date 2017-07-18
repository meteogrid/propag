{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Propag.BlockMap where

import Propag.Geometry (PixelSize(..))
import Control.Applicative (liftA2)
import Control.Arrow ((&&&))
import Linear.V2



type BlockIndex = V2 Int


newtype BlockSize = BlockSize {unBlockSize :: V2 Int}
  deriving (Eq, Show)


bmBackward
  :: PixelSize -> V2 Double -> V2 Int -> V2 Double
bmBackward (PixelSize d) o = (+ o) . (* d) . fmap fromIntegral
{-# INLINE bmBackward #-}

bmBackward2
  :: BlockSize -> PixelSize -> V2 Double
  -> BlockIndex -> V2 Int -> V2 Double
bmBackward2 (BlockSize bd) d o bix = bmBackward d o . (+(bix*bd))
{-# INLINE bmBackward2 #-}



bmForward
  :: PixelSize -> V2 Double
  -> V2 Double -> V2 Int
bmForward (PixelSize d) o = fmap round . (/ d) . subtract o
{-# INLINE bmForward #-}

bmForward2
  :: BlockSize -> PixelSize -> V2 Double
  -> V2 Double -> (BlockIndex, V2 Int)
bmForward2 bs d o = bmToBlockPixel bs . bmForward d o
{-# INLINE bmForward2 #-}

bmToBlockPixel :: BlockSize -> V2 Int -> (BlockIndex, V2 Int)
bmToBlockPixel (BlockSize bs) =
  liftA2 (flip div) bs &&& liftA2 (flip mod) bs
{-# INLINE bmToBlockPixel #-}


bmToBlockPixelAndOffset :: BlockSize -> V2 Int -> (BlockIndex, V2 Int, Int)
bmToBlockPixelAndOffset bs@(BlockSize (V2 nx _)) =
  (\(bix, px@(V2 i j)) -> (bix, px, j*nx+i)) . bmToBlockPixel bs
{-# INLINE bmToBlockPixelAndOffset #-}
