module Propag.GeometrySpec (main, spec) where

import Propag.Geometry

import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck hiding (Result, Success)

import Debug.Trace

main :: IO ()
main = hspec spec



spec :: Spec
spec = do
  describe "dda" $ do

    prop "does not repeat pixels" $ do
      forAll arbitraryPoints $ \(a,b) ->
        let ps = dda a b
            notSame x y
              | x == y    = traceShow ("repeated", x, y, a, b) False
              | otherwise = True
        in length (take 2 ps) > 1 ==>
            all id (zipWith notSame ps (tail ps))

    prop "always returns at least one pixel" $ do
      forAll arbitraryPoints $ \(a,b) ->
        case dda a b of
          [] -> False
          _  -> True

    prop "reaches destination" $ do
      forAll arbitraryPoints $ \(a,b) ->
        let [dest] = dda b b
        in case dda a b of
             els | last els == dest           -> True
             els | traceShow (("a,b,dest", a,b,dest),("els",els)) True -> False

arbitraryPoint :: Arbitrary a => Gen (V2 a)
arbitraryPoint = V2 <$> arbitrary <*> arbitrary

arbitraryPoints :: Gen (V2 Int, V2 Int)
arbitraryPoints = (,) <$> arbitraryPoint <*> arbitraryPoint
