{-# LANGUAGE BangPatterns, NamedFieldPuns, RecordWildCards #-}

module Data.SearchEngine.Query (

    -- * Querying
    query,
    ResultsFilter(..),

    -- * Explain mode for query result rankings
    queryExplain,
    BM25F.Explanation(..),
    setRankParams,

    -- ** Utils used by autosuggest
    relevanceScore,
    indexDocToBM25Doc,
    expandTransformedQueryTerm,
  ) where

import Data.SearchEngine.Types
import qualified Data.SearchEngine.SearchIndex as SI
import qualified Data.SearchEngine.DocIdSet    as DocIdSet
import qualified Data.SearchEngine.DocTermIds  as DocTermIds
import qualified Data.SearchEngine.DocFeatVals as DocFeatVals
import qualified Data.SearchEngine.BM25F       as BM25F

import Data.Ix
import Data.List
import Data.Function
import Data.Maybe


-- | Execute a normal query. Find the documents in which one or more of
-- the search terms appear and return them in ranked order.
--
-- The number of documents returned is limited by the 'paramResultsetSoftLimit'
-- and 'paramResultsetHardLimit' paramaters. This also limits the cost of the
-- query (which is primarily the cost of scoring each document).
--
-- The given terms are all assumed to be complete (as opposed to prefixes
-- like with 'queryAutosuggest').
--
query :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
         SearchEngine doc key field feature ->
         [Term] -> [key]
query se@SearchEngine{ searchIndex,
                       searchRankParams = SearchRankParameters{..} }
      terms =

  let -- Start by transforming/normalising all the query terms.
      -- This can be done differently for each field we search by.
      lookupTerms :: [Term]
      lookupTerms = concatMap (expandTransformedQueryTerm se) terms

      -- Then we look up all the normalised terms in the index.
      rawresults :: [Maybe (TermId, DocIdSet)]
      rawresults = map (SI.lookupTerm searchIndex) lookupTerms

      -- For the terms that occur in the index, this gives us the term's id
      -- and the set of documents that the term occurs in.
      termids   :: [TermId]
      docidsets :: [DocIdSet]
      (termids, docidsets) = unzip (catMaybes rawresults)

      -- We looked up the documents that *any* of the term occur in (not all)
      -- so this could be rather a lot of docs if the user uses a few common
      -- terms. Scoring these result docs is a non-trivial cost so we want to
      -- limit the number that we have to score. The standard trick is to
      -- consider the doc sets in the order of size, smallest to biggest. Once
      -- we have gone over a certain threshold of docs then don't bother with
      -- the doc sets for the remaining terms. This tends to work because the
      -- scoring gives lower weight to terms that occur in many documents.
      unrankedResults :: DocIdSet
      unrankedResults = pruneRelevantResults
                          paramResultsetSoftLimit
                          paramResultsetHardLimit
                          docidsets

      --TODO: technically this isn't quite correct. Because each field can
      -- be normalised differently, we can end up with different termids for
      -- the same original search term, and then we score those as if they
      -- were different terms, which makes a difference when the term appears
      -- in multiple fields (exactly the case BM25F is supposed to deal with).
      -- What we ought to have instead is an Array (Int, field) TermId, and
      -- make the scoring use the appropriate termid for each field, but to
      -- consider them the "same" term.
   in rankResults se termids (DocIdSet.toList unrankedResults)

-- | Before looking up a term in the main index we need to normalise it
-- using the 'transformQueryTerm'. Of course the transform can be different
-- for different fields, so we have to collect all the forms (eliminating
-- duplicates).
--
expandTransformedQueryTerm :: (Ix field, Bounded field) =>
                              SearchEngine doc key field feature ->
                              Term -> [Term]
expandTransformedQueryTerm SearchEngine{searchConfig} term =
    nub [ transformForField field
        | let transformForField = transformQueryTerm searchConfig term
        , field <- range (minBound, maxBound) ]


rankResults :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
               SearchEngine doc key field feature ->
               [TermId] -> [DocId] -> [key]
rankResults se@SearchEngine{searchIndex} queryTerms docids =
    map snd
  $ sortBy (flip compare `on` fst)
      [ (relevanceScore se queryTerms doctermids docfeatvals, dockey)
      | docid <- docids
      , let (dockey, doctermids, docfeatvals) = SI.lookupDocId searchIndex docid ]

relevanceScore :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
                  SearchEngine doc key field feature ->
                  [TermId] -> DocTermIds field -> DocFeatVals feature -> Float
relevanceScore SearchEngine{bm25Context} queryTerms doctermids docfeatvals =
    BM25F.score bm25Context doc queryTerms
  where
    doc = indexDocToBM25Doc doctermids docfeatvals

indexDocToBM25Doc :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
                     DocTermIds field ->
                     DocFeatVals feature ->
                     BM25F.Doc TermId field feature
indexDocToBM25Doc doctermids docfeatvals =
    BM25F.Doc {
      BM25F.docFieldLength        = DocTermIds.fieldLength    doctermids,
      BM25F.docFieldTermFrequency = DocTermIds.fieldTermCount doctermids,
      BM25F.docFeatureValue       = DocFeatVals.featureValue docfeatvals
    }

pruneRelevantResults :: Int -> Int -> [DocIdSet] -> DocIdSet
pruneRelevantResults softLimit hardLimit =
    -- Look at the docsets starting with the smallest ones. Smaller docsets
    -- correspond to the rarer terms, which are the ones that score most highly.
    go DocIdSet.empty . sortBy (compare `on` DocIdSet.size)
  where
    go !acc [] = acc
    go !acc (d:ds)
        -- If this is the first one, we add it anyway, otherwise we're in
        -- danger of returning no results at all.
      | DocIdSet.null acc = go d ds
        -- We consider the size our docset would be if we add this extra one...
        -- If it puts us over the hard limit then stop.
      | size > hardLimit  = acc
        -- If it puts us over soft limit then we add it and stop
      | size > softLimit  = DocIdSet.union acc d
        -- Otherwise we can add it and carry on to consider the remainder
      | otherwise         = go (DocIdSet.union acc d) ds
      where
        size = DocIdSet.size acc + DocIdSet.size d


--------------------------------
-- Normal query with explanation
--

queryExplain :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
                SearchEngine doc key field feature ->
                [Term] -> [(BM25F.Explanation field feature Term, key)]
queryExplain se@SearchEngine{ searchIndex,
                              searchConfig     = SearchConfig{transformQueryTerm},
                              searchRankParams = SearchRankParameters{..} }
      terms =

  -- See 'query' above for explanation. Really we ought to combine them.
  let lookupTerms :: [Term]
      lookupTerms = [ term'
                    | term  <- terms
                    , let transformForField = transformQueryTerm term
                    , term' <- nub [ transformForField field
                                   | field <- range (minBound, maxBound) ]
                    ]

      rawresults :: [Maybe (TermId, DocIdSet)]
      rawresults = map (SI.lookupTerm searchIndex) lookupTerms

      termids   :: [TermId]
      docidsets :: [DocIdSet]
      (termids, docidsets) = unzip (catMaybes rawresults)

      unrankedResults :: DocIdSet
      unrankedResults = pruneRelevantResults
                          paramResultsetSoftLimit
                          paramResultsetHardLimit
                          docidsets

   in rankExplainResults se termids (DocIdSet.toList unrankedResults)

rankExplainResults :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
                      SearchEngine doc key field feature ->
                      [TermId] ->
                      [DocId] ->
                      [(BM25F.Explanation field feature Term, key)]
rankExplainResults se@SearchEngine{searchIndex} queryTerms docids =
    sortBy (flip compare `on` (BM25F.overallScore . fst))
      [ (explainRelevanceScore se queryTerms doctermids docfeatvals, dockey)
      | docid <- docids
      , let (dockey, doctermids, docfeatvals) = SI.lookupDocId searchIndex docid ]


explainRelevanceScore :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
                         SearchEngine doc key field feature ->
                         [TermId] ->
                         DocTermIds field ->
                         DocFeatVals feature ->
                         BM25F.Explanation field feature Term
explainRelevanceScore SearchEngine{bm25Context, searchIndex}
                      queryTerms doctermids docfeatvals =
    fmap (SI.getTerm searchIndex) (BM25F.explain bm25Context doc queryTerms)
  where
    doc = indexDocToBM25Doc doctermids docfeatvals


setRankParams :: SearchRankParameters field feature ->
                 SearchEngine doc key field feature ->
                 SearchEngine doc key field feature
setRankParams params@SearchRankParameters{..} se =
    se {
      searchRankParams = params,
      bm25Context      = (bm25Context se) {
        BM25F.paramK1         = paramK1,
        BM25F.paramB          = paramB,
        BM25F.fieldWeight     = paramFieldWeights,
        BM25F.featureWeight   = paramFeatureWeights,
        BM25F.featureFunction = paramFeatureFunctions
      }
    }


--------------------------------
-- Results filter
--

-- | In some applications it is necessary to enforce some security or
-- visibility rule about the query results (e.g. in a typical DB-based
-- application different users can see different data items). Typically
-- it would be too expensive to build different search indexes for the
-- different contexts and so the strategy is to use one index containing
-- everything and filter for visibility in the results. This means the
-- filter condition is different for different queries (e.g. performed
-- on behalf of different users).
--
-- Filtering the results after a query is possible but not the most efficient
-- thing to do because we've had to score all the not-visible documents.
-- The better thing to do is to filter as part of the query, this way we can
-- filter before the expensive scoring.
--
-- We provide one further optimisation: bulk predicates. In some applications
-- it can be quicker to check the security\/visibility of a whole bunch of
-- results all in one go.
--
data ResultsFilter key = NoFilter
                       | FilterPredicate     (key -> Bool)
                       | FilterBulkPredicate ([key] -> [Bool])
--TODO: allow filtering & non-feature score lookup in one bulk op

