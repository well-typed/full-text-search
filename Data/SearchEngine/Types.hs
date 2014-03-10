{-# LANGUAGE NamedFieldPuns, RecordWildCards #-}

module Data.SearchEngine.Types (
    -- * Search engine types and helper functions
    SearchEngine(..),
    SearchConfig(..),
    SearchRankParameters(..),
    BM25F.FeatureFunction(..),
    initSearchEngine,
    cacheBM25Context,

    -- ** Helper type for non-term features
    NoFeatures,
    noFeatures,

    -- * Re-export SearchIndex and other types
    SearchIndex, Term, TermId,
    DocIdSet, DocId,
    DocTermIds, DocFeatVals,

    -- * Internal sanity check
    invariant,
  ) where

import Data.SearchEngine.SearchIndex (SearchIndex, Term, TermId)
import qualified Data.SearchEngine.SearchIndex as SI
import Data.SearchEngine.DocIdSet (DocIdSet, DocId)
import qualified Data.SearchEngine.DocIdSet as DocIdSet
import Data.SearchEngine.DocFeatVals (DocFeatVals)
import Data.SearchEngine.DocTermIds  (DocTermIds)
import qualified Data.SearchEngine.BM25F as BM25F

import Data.Ix
import Data.Array.Unboxed



data SearchConfig doc key field feature = SearchConfig {
       documentKey          :: doc -> key,
       extractDocumentTerms :: doc -> field -> [Term],
       transformQueryTerm   :: Term -> field -> Term,
       documentFeatureValue :: doc -> feature -> Float
     }

data SearchRankParameters field feature = SearchRankParameters {
       paramK1                 :: !Float,
       paramB                  :: field -> Float,
       paramFieldWeights       :: field -> Float,
       paramFeatureWeights     :: feature -> Float,
       paramFeatureFunctions   :: feature -> BM25F.FeatureFunction,

       paramResultsetSoftLimit   :: !Int,
       paramResultsetHardLimit   :: !Int,
       paramAutosuggestPrefilterLimit  :: !Int,
       paramAutosuggestPostfilterLimit :: !Int
     }

data SearchEngine doc key field feature = SearchEngine {
       searchIndex      :: !(SearchIndex      key field feature),
       searchConfig     :: !(SearchConfig doc key field feature),
       searchRankParams :: !(SearchRankParameters field feature),

       -- cached info
       sumFieldLengths :: !(UArray field Int),
       bm25Context     :: BM25F.Context TermId field feature
     }

invariant :: (Ord key, Ix field, Bounded field) =>
             SearchEngine doc key field feature -> Bool
invariant SearchEngine{searchIndex} =
    SI.invariant searchIndex
-- && check caches

initSearchEngine :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
                    SearchConfig doc key field feature ->
                    SearchRankParameters field feature ->
                    SearchEngine doc key field feature
initSearchEngine config params =
    cacheBM25Context
      SearchEngine {
        searchIndex      = SI.emptySearchIndex,
        searchConfig     = config,
        searchRankParams = params,
        sumFieldLengths  = listArray (minBound, maxBound) (repeat 0),
        bm25Context      = undefined
      }

cacheBM25Context :: Ix field =>
                    SearchEngine doc key field feature ->
                    SearchEngine doc key field feature
cacheBM25Context
    se@SearchEngine {
      searchRankParams = SearchRankParameters{..},
      searchIndex,
      sumFieldLengths
    }
  = se { bm25Context = bm25Context' }
  where
    bm25Context' = BM25F.Context {
      BM25F.numDocsTotal    = SI.docCount searchIndex,
      BM25F.avgFieldLength  = \f -> fromIntegral (sumFieldLengths ! f)
                                  / fromIntegral (SI.docCount searchIndex),
      BM25F.numDocsWithTerm = DocIdSet.size . SI.lookupTermId searchIndex,
      BM25F.paramK1         = paramK1,
      BM25F.paramB          = paramB,
      BM25F.fieldWeight     = paramFieldWeights,
      BM25F.featureWeight   = paramFeatureWeights,
      BM25F.featureFunction = paramFeatureFunctions
    }


-----------------------------

data NoFeatures = NoFeatures
  deriving (Eq, Ord, Bounded)

instance Ix NoFeatures where
  range   _   = []
  inRange _ _ = False
  index   _ _ = -1

noFeatures :: NoFeatures -> a
noFeatures _ = error "noFeatures"

