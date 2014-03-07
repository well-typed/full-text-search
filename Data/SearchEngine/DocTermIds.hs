{-# LANGUAGE BangPatterns, GeneralizedNewtypeDeriving #-}
module Data.SearchEngine.DocTermIds (
    DocTermIds,
    TermId,
    fieldLength,
    fieldTermCount,
    fieldElems,
    create,
    denseTable,
    vecIndexIx,
    vecCreateIx,
  ) where

import Data.SearchEngine.TermBag (TermBag, TermId)
import qualified Data.SearchEngine.TermBag as TermBag

import Data.Vector (Vector, (!))
import qualified Data.Vector as Vec
import qualified Data.Vector.Unboxed as UVec
import Data.Ix (Ix)
import qualified Data.Ix as Ix


-- | The 'TermId's for the 'Term's that occur in a document. Documents may have
-- multiple fields and the 'DocTerms' type holds them separately for each field.
--
newtype DocTermIds field = DocTermIds (Vector TermBag)
  deriving (Show)

getField :: (Ix field, Bounded field) => DocTermIds field -> field -> TermBag
getField (DocTermIds fieldVec) = vecIndexIx fieldVec

create :: (Ix field, Bounded field) =>
          (field -> [TermId]) -> DocTermIds field
create docTermIds =
    DocTermIds (vecCreateIx (TermBag.fromList . docTermIds))

-- | The number of terms in a field within the document.
fieldLength :: (Ix field, Bounded field) => DocTermIds field -> field -> Int
fieldLength docterms field =
    TermBag.size (getField docterms field)

-- | The frequency of a particular term in a field within the document.
fieldTermCount :: (Ix field, Bounded field) => DocTermIds field -> field -> TermId -> Int
fieldTermCount docterms field termid =
    fromIntegral (TermBag.termCount (getField docterms field) termid)

fieldElems :: (Ix field, Bounded field) => DocTermIds field -> field -> [TermId]
fieldElems docterms field =
    TermBag.elems (getField docterms field)

-- | The 'DocTermIds' is really a sparse 2d array, and doing lookups with
-- 'fieldTermCount' has a O(log n) cost. This function converts to a dense
-- tabular representation which then enables linear scans.
--
denseTable :: (Ix field, Bounded field) => DocTermIds field ->
              (Int, Int -> TermId, Int -> field -> Int)
denseTable (DocTermIds fieldVec) =
    let (!termids, !termcounts) = TermBag.denseTable (Vec.toList fieldVec)
        !numTerms = UVec.length termids
     in ( numTerms
        , \i    -> termids UVec.! i
        , \i ix -> let j = Ix.index (minBound, maxBound) ix
                    in fromIntegral (termcounts UVec.! (j * numTerms + i))
        )

---------------------------------
-- Vector indexed by Ix Bounded
--

vecIndexIx  :: (Ix ix, Bounded ix) => Vector a -> ix -> a
vecIndexIx vec ix = vec ! Ix.index (minBound, maxBound) ix

vecCreateIx :: (Ix ix, Bounded ix) => (ix -> a) -> Vector a
vecCreateIx f = Vec.fromListN (Ix.rangeSize bounds)
                  [ y | ix <- Ix.range bounds, let !y = f ix ]
  where
    bounds = (minBound, maxBound)

