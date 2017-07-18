module Propag.Template (
    Template
  , mkTemplate
  , placeHolders
  , renderWithTime
) where

import Control.Arrow (second)
import Data.Time.Clock (UTCTime)
import Data.Time.Format (formatTime, defaultTimeLocale, iso8601DateFormat)

import Text.StringTemplate

newtype Template = Template { unTemplate :: StringTemplate String}

renderWithTime :: UTCTime -> Template -> String
renderWithTime t (Template s) = toString (setManyAttrib attrs s)
  where
    attrs = map (second (flip (formatTime defaultTimeLocale) t))
                placeHolderFormats

mkTemplate :: String -> Either String Template
mkTemplate = validate . newSTMP
  where validate t = case checkTemplate t of
                       (Just err,_,_) -> Left err
                       (Nothing,_,_)  -> Right (Template t)


placeHolders :: [String]
placeHolders = map fst placeHolderFormats

placeHolderFormats :: [(String, String)]
placeHolderFormats = [
    ("YYYY", "%Y")
  , ("MM",   "%m")
  , ("DD",   "%d")
  , ("hh",   "%h")
  , ("mm",   "%M")
  , ("ss",   "%S")
  , ("iso8601", iso8601DateFormat (Just "%H:%M:%SZ"))
  ]

instance Show Template where
  show = toString . setManyAttrib (map mkPH [
    ]) . unTemplate
    where mkPH s = (s, concat ["$",s,"$"])

instance Eq Template where
  a == b = show a == show b
