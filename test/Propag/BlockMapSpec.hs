module Propag.BlockMapSpec (main, spec) where

import Propag.Geometry
import Propag.BlockMap

import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck hiding (Result, Success)

main :: IO ()
main = hspec spec



spec :: Spec
spec = do
  describe "backward and forward" $ do
    prop "forward (backward a) == a" $
      forAll ((,) <$> arbitraryPixelsizeAndOrigin <*> arbitraryPixel) $
        \((ps,o),px) -> let bw = bmBackward ps o
                            fw = bmForward ps o
                        in fw (bw px) == px


arbitraryPixelsizeAndOrigin :: Gen (PixelSize, V2 Double)
arbitraryPixelsizeAndOrigin = (,) <$> arbitraryPixelSize <*> arbitraryPoint


arbitraryPoint :: Arbitrary a => Gen (V2 a)
arbitraryPoint = V2 <$> arbitrary <*> arbitrary

arbitraryPixel :: Gen (V2 Int)
arbitraryPixel = arbitraryPoint

arbitraryBixAndPixel :: Gen (V2 Int, V2 Int)
arbitraryBixAndPixel = (,) <$> arbitraryPixel <*> arbitraryPixel

arbitraryBlockSize :: Gen (BlockSize)
arbitraryBlockSize
  = BlockSize <$> (V2 <$> (getPositive <$> arbitrary)
                      <*> (getPositive <$> arbitrary))

arbitraryPixelSize :: Gen PixelSize
arbitraryPixelSize
  = PixelSize <$> (V2 <$> (getNonZero <$> arbitrary)
                      <*> (getNonZero <$> arbitrary))
