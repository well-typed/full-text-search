{-# LANGUAGE BangPatterns, GeneralizedNewtypeDeriving #-}
module Data.SearchEngine.DocFeatVals (
    DocFeatVals,
    featureValue,
    create,
  ) where

import Data.SearchEngine.DocTermIds (vecIndexIx, vecCreateIx)
import Data.Vector (Vector)
import Data.Ix (Ix)


-- | Storage for the non-term feature values i a document.
--
newtype DocFeatVals feature = DocFeatVals (Vector Float)
  deriving (Show)

featureValue :: (Ix feature, Bounded feature) => DocFeatVals feature -> feature -> Float
featureValue (DocFeatVals featVec) = vecIndexIx featVec

create :: (Ix feature, Bounded feature) =>
          (feature -> Float) -> DocFeatVals feature
create docFeatVals =
    DocFeatVals (vecCreateIx docFeatVals)

