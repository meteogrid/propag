{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
module Main (main) where

import Propag
import Propag.Geometry (Raster(..), V2(..), grSize)
import Propag.Engine (propagate)

import Control.Concurrent (threadDelay)
import Control.Lens
import Control.Monad (void)
import Data.Time (UTCTime(..))
import Data.Time.Calendar (fromGregorian)
import Data.Time.LocalTime (TimeOfDay(..), timeOfDayToTime)
import qualified Data.Vector as V
import qualified Numeric.Units.Dimensional.Prelude as DP
import Numeric.Units.Dimensional.SIUnits (hour)
import System.Random (randomRIO)

main :: IO ()
main = void $ propagate DemoIOConfig cfg
  where
    time = timeOfDayToTime (TimeOfDay 11 0 0)
    startTime = UTCTime (fromGregorian 2012 8 27) time
    endTime = UTCTime (fromGregorian 2012 8 28) time
    timeSpec = Interval startTime endTime (1 DP.*~ hour)
    cfg :: InitialPropagConfig
    cfg = def
        & blockSize .~ BlockSize 64
        & pixelSize .~ PixelSize 5
        & maxTime   .~ (24 DP.*~ hour)
        & initialElements .~
          [ IgnitedElement (V2 392210.2231 4485371.234) startTime
          , IgnitedElement (V2 392210.2231 4491371.234) startTime
          ]
        & inputs .~ PropagInputs {
              _d1hr        = InputMap [(timeSpec, Constant 10 perCent)]
            , _d10hr       = InputMap [(timeSpec, Constant 10 perCent)]
            , _d100hr      = InputMap [(timeSpec, Constant 10 perCent)]
            , _herb        = InputMap [(timeSpec, Constant 80 perCent)]
            , _wood        = InputMap [(timeSpec, Constant 26.6 perCent)]
            , _windSpeed   = InputMap [(timeSpec, Constant 5 kmh)]
            , _windBearing = InputMap [(timeSpec, Constant 10 bearingDegree)]
            , _slope       = InputMap [(timeSpec, Constant 20 perCent)]
            , _aspect      = InputMap [(timeSpec, Constant 30 bearingDegree)]
            , _fuel        = InputMap [(timeSpec, Constant 3  def)]
            }

data DemoIO

type HasDemoIO a = (Show a , Eq a)

type instance PropagIOConstraint DemoIO a = HasDemoIO a
type instance PropagIONullable   DemoIO a = Maybe a

instance Missing V.Vector (Maybe a) where
  type BaseVector V.Vector (Maybe a) = V.Vector

instance HasLoadExtent DemoIO where
  type PropagIOVector  DemoIO = V.Vector
  data PropagIOConfig  DemoIO = DemoIOConfig deriving Show

  loadExtent _ Constant{_inputValue=v} geoRef = do
    let vec = V.replicate (product (grSize geoRef)) (Just v)
    threadDelay =<< randomRIO (10,1000)
    return (Raster geoRef vec)

  loadExtent _ _ _ = error "not implemented"
