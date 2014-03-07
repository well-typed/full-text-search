{-# LANGUAGE BangPatterns, NamedFieldPuns, RecordWildCards, ScopedTypeVariables #-}

module Data.SearchEngine (

    -- * Basic interface

    -- ** Querying
    Term,
    query,

    -- *** Query auto-completion \/ auto-suggestion
    queryAutosuggest,
    ResultsFilter(..),

    -- ** Making a search engine instance
    initSearchEngine,
    SearchEngine,
    SearchConfig(..),
    SearchRankParameters(..),
    BM25F.FeatureFunction(..),

    -- ** Helper type for non-term features
    NoFeatures,
    noFeatures,

    -- ** Managing documents to be searched
    insertDoc,
    insertDocs,
    deleteDoc,

    -- * Explain mode for query result rankings
    queryExplain,
    BM25F.Explanation(..),
    setRankParams,

    -- * Internal sanity check
    invariant,
  ) where

import Data.SearchEngine.SearchIndex (SearchIndex, Term, TermId)
import qualified Data.SearchEngine.SearchIndex as SI
import Data.SearchEngine.DocIdSet (DocIdSet, DocId)
import qualified Data.SearchEngine.DocIdSet as DocIdSet
import Data.SearchEngine.DocTermIds (DocTermIds)
import qualified Data.SearchEngine.DocTermIds as DocTermIds
import Data.SearchEngine.DocFeatVals (DocFeatVals)
import qualified Data.SearchEngine.DocFeatVals as DocFeatVals
import qualified Data.SearchEngine.BM25F as BM25F

import Data.Ix
import Data.Ord
import Data.Array.Unboxed
import Data.List
import Data.Function
import Data.Maybe
import qualified Data.Map as Map
import qualified Data.IntSet as IntSet
import qualified Data.Vector.Unboxed as Vec


-------------------
-- Doc layer
--
-- That is, at the layer of documents, so covering the issues of:
--  - inserting/removing whole documents
--  - documents having multiple fields
--  - documents having multiple terms
--  - transformations (case-fold/normalisation/stemming) on the doc terms
--  - transformations on the search terms
--

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
       paramResultsetSoftLimit :: !Int,
       paramResultsetHardLimit :: !Int
     }

data SearchEngine doc key field feature = SearchEngine {
       searchIndex      :: !(SearchIndex      key field feature),
       searchConfig     :: !(SearchConfig doc key field feature),
       searchRankParams :: !(SearchRankParameters field feature),

       -- cached info
       sumFieldLengths :: !(UArray field Int),
       bm25Context     :: BM25F.Context TermId field feature
     }

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

invariant :: (Ord key, Ix field, Bounded field) =>
             SearchEngine doc key field feature -> Bool
invariant SearchEngine{searchIndex} =
    SI.invariant searchIndex
-- && check caches

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

updateCachedFieldLengths :: (Ix field, Bounded field) =>
                            Maybe (DocTermIds field) -> Maybe (DocTermIds field) ->
                            SearchEngine doc key field feature ->
                            SearchEngine doc key field feature
updateCachedFieldLengths Nothing (Just newDoc) se@SearchEngine{sumFieldLengths} =
    se {
      sumFieldLengths =
        array (bounds sumFieldLengths)
              [ (i, n + DocTermIds.fieldLength newDoc i)
              | (i, n) <- assocs sumFieldLengths ]
    }
updateCachedFieldLengths (Just oldDoc) (Just newDoc) se@SearchEngine{sumFieldLengths} =
    se {
      sumFieldLengths =
        array (bounds sumFieldLengths)
              [ (i, n - DocTermIds.fieldLength oldDoc i
                      + DocTermIds.fieldLength newDoc i)
              | (i, n) <- assocs sumFieldLengths ]
    }
updateCachedFieldLengths (Just oldDoc) Nothing se@SearchEngine{sumFieldLengths} =
    se {
      sumFieldLengths =
        array (bounds sumFieldLengths)
              [ (i, n - DocTermIds.fieldLength oldDoc i)
              | (i, n) <- assocs sumFieldLengths ]
    }
updateCachedFieldLengths Nothing Nothing se = se

insertDocs :: (Ord key, Ix field, Bounded field, Ix feature, Bounded feature) =>
              [doc] ->
              SearchEngine doc key field feature ->
              SearchEngine doc key field feature
insertDocs docs se = foldl' (\se' doc -> insertDoc doc se') se docs

insertDoc :: (Ord key, Ix field, Bounded field, Ix feature, Bounded feature) =>
             doc ->
             SearchEngine doc key field feature ->
             SearchEngine doc key field feature
insertDoc doc se@SearchEngine{ searchConfig = SearchConfig {
                                 documentKey, 
                                 extractDocumentTerms,
                                 documentFeatureValue
                               }
                             , searchIndex } =
    let key = documentKey doc
        searchIndex' = SI.insertDoc key (extractDocumentTerms doc)
                                        (documentFeatureValue doc)
                                        searchIndex
        oldDoc       = SI.lookupDocKey searchIndex  key
        newDoc       = SI.lookupDocKey searchIndex' key

     in cacheBM25Context $
        updateCachedFieldLengths oldDoc newDoc $
          se { searchIndex = searchIndex' }

deleteDoc :: (Ord key, Ix field, Bounded field) =>
             key ->
             SearchEngine doc key field feature ->
             SearchEngine doc key field feature
deleteDoc key se@SearchEngine{searchIndex} =
    let searchIndex' = SI.deleteDoc key searchIndex
        oldDoc       = SI.lookupDocKey searchIndex key

     in cacheBM25Context $
        updateCachedFieldLengths oldDoc Nothing $
          se { searchIndex = searchIndex' }

-----------------------------

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
                       searchConfig     = SearchConfig{transformQueryTerm},
                       searchRankParams = SearchRankParameters{..} }
      terms =

  let -- Start by transforming/normalising all the query terms.
      -- This can be done differently for each field we search by.
      lookupTerms :: [Term]
      lookupTerms = [ term'
                    | term  <- terms
                    , let transformForField = transformQueryTerm term
                    , term' <- nub [ transformForField field
                                   | field <- range (minBound, maxBound) ]
                    ]

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

-----------------------------

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


-- For an intro to this auto-complete/suggest, see:
--
-- /Type Less, Find More: Fast Autocompletion Search with a Succinct Index/
-- by Holger Bast and Ingmar Weber
-- <http://www.mpi-inf.mpg.de/~bast/papers/autocompletion-sigir.pdf>
--
-- We also include snippets from this paper inline below for reference.
--
-- We follow this paper only in part however. We do not use their clever
-- index. We do not follow their scoring suggestion (indeed there's good
-- reasons to think it does not work at all for bm25 or for any other scoring
-- function based only on term frequency).


-- | Execute an \"auto-suggest\" query. This is where one of the search terms
-- is an incomplete prefix and we are looking for possible completions of that
-- search term, and result documents to go with the possible completions.
--
-- An auto-suggest query only gives useful results when the 'SearchEngine' is
-- configured to use a non-term feature score. That is, when we can give
-- documents an importance score independent of what terms we are looking for.
-- This is because an auto-suggest query is backwards from a normal query: we
-- are asking for relevant terms occurring in important or popular documents
-- so we need some notion of important or popular. Without this we would just
-- be ranking based on term frequency which while it makes sense for normal
-- \"forward\" queries is pretty meaningless for auto-suggest \"reverse\"
-- queries. Indeed for single-term auto-suggest queries the ranking function
-- we use will assign 0 for all documents and completions if there is no 
-- non-term feature scores.
--
queryAutosuggest :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
                    SearchEngine doc key field feature ->
                    ResultsFilter key ->
                    [Term] -> Term -> ([(Term, Float)], [(key, Float)])
queryAutosuggest se resultsFilter precedingTerms partialTerm =

     step8_external
   . step7_rank
   . step6_scoreDs
   . step5_scoreTs
   . step4_cache
   . step3_filter
   . step2_process
   $ step1_prep
       precedingTerms partialTerm

  where
    -- Construct the auto-suggest query from the query terms
    step1_prep pre_ts t = mkAutosuggestQuery se pre_ts t

    -- Find the appropriate subset of ts and ds
    -- and an intermediate result that will be useful later:
    -- { (t, ds ∩ ds_t) | t ∈ ts, ds ∩ ds_t ≠ ∅ }
    step2_process (ts, ds, pre_ts) = (ts', ds', tdss', pre_ts)
      where
        (tdss', ts', ds') = processAutosuggestQuery se (ts, ds, pre_ts)

    -- Filter ds to those that are visible for this query
    -- and at the same time, do the docid -> docinfo lookup
    -- (needed at this step anyway to do the filter)
    step3_filter (ts, ds, tdss, pre_ts) = (ts, ds_info, tdss, pre_ts)
      where
        ds_info = filterAutosuggestQuery se resultsFilter ds

    -- For all ds, calculate and cache a couple bits of info needed
    -- later for scoring completion terms and doc results
    step4_cache (ts, ds_info, tdss, pre_ts) = (ds_info', tdss)
      where
        ds_info' = cacheDocScoringInfo se ts ds_info pre_ts

    -- Score the completion terms
    step5_scoreTs (ds_info, tdss) = (ds_info, tdss, ts_scored)
      where
        ts_scored = scoreAutosuggestQueryCompletions tdss ds_info

    -- Score the doc results (making use of the completion scores)
    step6_scoreDs (ds_info, tdss, ts_scored) = (ts_scored, ds_scored)
      where
        ds_scored = scoreAutosuggestQueryResults tdss ds_info ts_scored

    -- Rank the completions and results based on their scores
    step7_rank = sortResults

    -- Convert from internal Ids into external forms: Term and doc key
    step8_external = convertIdsToExternal se


sortResults :: (Ord av, Ord bv) => ([(a,av)], [(b,bv)]) -> ([(a,av)], [(b,bv)])
sortResults (xs, ys) =
    ( sortBySndDescending xs
    , sortBySndDescending ys )
  where
    sortBySndDescending :: Ord v => [(x,v)] -> [(x,v)]
    sortBySndDescending = sortBy (flip (comparing snd))

convertIdsToExternal :: SearchEngine doc key field feature ->
                        ([(TermId, v)], [(DocId, v)]) -> ([(Term, v)], [(key, v)])
convertIdsToExternal SearchEngine{searchIndex} (termids, docids) =
    ( [ (SI.getTerm   searchIndex termid, s) | (termid, s) <- termids ]
    , [ (SI.getDocKey searchIndex docid,  s) | (docid,  s) <- docids  ]
    )


-- From Bast and Weber:
--
--   An autocompletion query is a pair (T, D), where T is a range of terms
--   (all possible completions of the last term which the user has started
--   typing) and D is a set of documents (the hits for the preceding part of
--   the query).
--
-- We augment this with the preceding terms because we will need these to
-- score the set of documents D.
--
-- Note that the set D will be the entire collection in the case that the
-- preceding part of the query is empty. For efficiency we represent that
-- case specially with Maybe.

type AutosuggestQuery = ([TermId], Maybe DocIdSet, [TermId])

mkAutosuggestQuery :: (Ix field, Bounded field) =>
                      SearchEngine doc key field feature ->
                      [Term] -> Term -> AutosuggestQuery
mkAutosuggestQuery SearchEngine{ searchIndex, searchConfig }
                   precedingTerms partialTerm =
    (completionTerms, precedingDocHits, precedingTerms')
  where
    completionTerms = map fst $ SI.lookupTermsByPrefix searchIndex partialTerm
                      --TODO: need to take transformQueryTerm into account

    (precedingTerms', precedingDocHits)
      | null precedingTerms = ([], Nothing)
      | otherwise           = fmap (Just . DocIdSet.unions)
                                   (lookupRawResults precedingTerms)

    lookupRawResults :: [Term] -> ([TermId], [DocIdSet])
    lookupRawResults ts =
      unzip $ catMaybes
        [ SI.lookupTerm searchIndex t'
        | t  <- ts
        , let transformForField = transformQueryTerm searchConfig t
        , t' <- nub [ transformForField field
                    | field <- range (minBound, maxBound) ]
        ]


-- From Bast and Weber:
--
--   To process the query means to compute the subset T' ⊆ T of terms that
--   occur in at least one document from D, as well as the subset D' ⊆ D of
--   documents that contain at least one of these words.
--
--   The obvious way to use an inverted index to process an autocompletion
--   query (T, D) is to compute, for each t ∈ T, the intersections D ∩ Dt.
--   Then, T' is simply the set of all t for which the intersection was
--   non-empty, and D' is the union of all (non-empty) intersections.
--
-- We will do this but additionally we will return all the non-empty
-- intersections because they will be useful when scoring.

processAutosuggestQuery :: SearchEngine doc key field feature ->
                           AutosuggestQuery ->
                           ([(TermId, DocIdSet)], [TermId], DocIdSet)
processAutosuggestQuery SearchEngine{ searchIndex }
                         (completionTerms, precedingDocHits, _) =
    ( completionTermAndDocSets
    , completionTerms'
    , allTermDocSet
    )
  where
    -- We look up each candidate completion to find the set of documents
    -- it appears in, and filtering (intersecting) down to just those
    -- appearing in the existing partial query results (if any).
    -- Candidate completions not appearing at all within the existing
    -- partial query results are excluded at this stage.
    --
    -- We have to keep these doc sets for the whole process, so we keep
    -- them as the compact DocIdSet type.
    --
    completionTermAndDocSets :: [(TermId, DocIdSet)]
    completionTermAndDocSets =
      [ (t, ds_t')
      | t <- completionTerms
      , let ds_t  = SI.lookupTermId searchIndex t
            ds_t' = case precedingDocHits of
                      Just ds -> ds `DocIdSet.intersection` ds_t
                      Nothing -> ds_t
      , not (DocIdSet.null ds_t')
      ]

    -- The remaining candidate completions
    completionTerms' = [ w | (w, _ds_w) <- completionTermAndDocSets ]

    -- The union of all these is this set of documents that form the results.
    allTermDocSet :: DocIdSet
    allTermDocSet =
      DocIdSet.unions [ ds_t | (_t, ds_t) <- completionTermAndDocSets ]


filterAutosuggestQuery :: SearchEngine doc key field feature ->
                          ResultsFilter key ->
                          DocIdSet ->
                          [(DocId, (key, DocTermIds field, DocFeatVals feature))]
filterAutosuggestQuery SearchEngine{ searchIndex } resultsFilter ds =
    case resultsFilter of
      NoFilter ->
        [ (docid, doc)
        | docid <- DocIdSet.toList ds
        , let doc = SI.lookupDocId searchIndex docid ]

      FilterPredicate predicate ->
        [ (docid, doc)
        | docid <- DocIdSet.toList ds
        , let doc@(k,_,_) = SI.lookupDocId searchIndex docid
        , predicate k ]

      FilterBulkPredicate bulkPredicate ->
        [ (docid, doc)
        | let docids = DocIdSet.toList ds
              docinf = map (SI.lookupDocId searchIndex) docids
              keep   = bulkPredicate [ k | (k,_,_) <- docinf ]
        , (docid, doc, True) <- zip3 docids docinf keep ]


-- Scoring
-------------
--
-- From Bast and Weber:
--   In practice, only a selection of items from these lists can and will be
--   presented to the user, and it is of course crucial that the most relevant
--   completions and hits are selected.
--
--   A standard approach for this task in ad-hoc retrieval is to have a
--   precomputed score for each term-in-document pair, and when a query is
--   being processed, to aggregate these scores for each candidate document,
--   and return documents with the highest such aggregated scores.
--
--   Both INV and HYB can be easily adapted to implement any such scoring and
--   aggregation scheme: store by each term-in-document pair its precomputed
--   score, and when intersecting, aggregate the scores. A decision has to be
--   made on how to reconcile scores from different completions within the
--   same document. We suggest the following: when merging the intersections
--   (which gives the set D' according to Definition 1), compute for each
--   document in D' the maximal score achieved for some completion in T'
--   contained in that document, and compute for each completion in T' the
--   maximal score achieved for a hit from D' achieved for this completion.
--
-- So firstly let us explore what this means and then discuss why it does not
-- work for BM25.
--
-- The "precomputed score for each term-in-document pair" refers to the bm25
-- score for this term in this document (and obviously doesn't have to be
-- precomputed, though that'd be faster).
--
-- So the score for a document d ∈ D' is:
--   maximum of score for d ∈ D ∩ Dt, for any t ∈ T'
--
-- While the score for a completion t ∈ T' is:
--   maximum of score for d ∈ D ∩ Dt
--
-- So for documents we say their score is their best score for any of the
-- completion terms they contain. While for completions we say their score
-- is their best score for any of the documents they appear in.
--
-- For a scoring function like BM25 this appears to be not a good method, both
-- in principle and in practice. Consider what terms get high BM25 scores:
-- very rare ones. So this means we're going to score highly documents that
-- contain the least frequent terms, and completions that are themselves very
-- rare. This is silly.
--
-- Another important thing to note is that if we use this scoring method then
-- we are using the BM25 score in a way that makes no sense. The BM25 score
-- for different documents for the /same/ set of terms are comparable. The
-- score for the same for different document with different terms are simply
-- not comparable.
--
-- This also makes sense if you consider what question the BM25 score is
-- answering: "what is the likelihood that this document is relevant given that
-- I judge these terms to be relevant". However an auto-suggest query is
-- different: "what is the likelihood that this term is relevant given the
-- importance/popularity of the documents (and any preceding terms I've judged
-- to be relevant)". They are both conditional likelihood questions but with
-- different premises.
--
-- More generally, term frequency information simply isn't useful for
-- auto-suggest queries. We don't want results that have the most obscure terms
-- nor the most common terms, not even something in-between. Term frequency
-- just doesn't tell us anything unless we've already judged terms to be
-- relevant, and in an auto-suggest query we've not done that yet.
--
-- What we really need is information on the importance/popularity of the
-- documents. We can actually do something with that.
--
-- So, instead we follow a different strategy. We require that we have
-- importance/popularity info for the documents.
--
-- A first approximation would be to rank result documents by their importance
-- and completion terms by the sum of the importance of the documents each
-- term appears in.
--
-- Score for a document d ∈ D'
--   importance score for d
--
-- Score for a completion t ∈ T'
--   sum of importance score for d ∈ D ∩ Dt
--
-- The only problem with this is that just because a term appears in an
-- important document, doesn't mean that term is /about/ that document, or to
-- put it another way, that term may not be relevant for that document. For
-- example common words like "the" likely appear in all important documents
-- but this doesn't really tell us anything because "the" isn't an important
-- keyword.
--
-- So what we want to do is to weight the document importance by the relevance
-- of the keyword to the document. So now if we have an important document and
-- a relevant keyword for that document then we get a high score, but an
-- irrelevant term like "the" would get a very low weighting and so would not
-- contribute much to the score, even for very important documents.
--
-- The intuition is that we will score term completions by taking the
-- document importance weighted by the relevance of that term to that document
-- and summing over all the documents where the term occurs.
--
-- We define document importance (for the set D') to be the BM25F score for
-- the documents with any preceding terms. So this includes the non-term
-- feature score for the importance/popularity, and also takes account of
-- preceding terms if there were any.
--
-- We define term relevance (for terms in documents) to be the BM25F score for
-- that term in that document as a fraction of the total BM25F score for all
-- terms in the document. Thus the relevance of all terms in a document sums
-- to 1.
--
-- Now we can re-weight the document importance by the term relevance:
--
-- Score for a completion t ∈ T'
--   sum (for d ∈ D ∩ Dt) of ( importance for d * relevance for t in d )
--
-- And now for document result scores. We don't want to just stick with the
-- innate document importance. We want to re-weight by the completion term
-- scores:
--
-- Score for a document d ∈ D'
--   sum (for t ∈ T' ∩ d) (importance score for d * score for completion t)
--
-- Clear as mud?

type DocImportance = Float
type TermRelevanceBreakdown = Map.Map TermId Float

-- | Precompute the document importance and the term relevance breakdown for
-- all the documents. This will be used in scoring the term completions
-- and the result documents. They will all be used and some used many
-- times so it's better to compute up-front and share.
--
-- This is actually the expensive bit (which is why we've filtered already).
--
cacheDocScoringInfo :: (Ix field, Bounded field, Ix feature, Bounded feature) =>
                       SearchEngine doc key field feature ->
                       [TermId] ->
                       [(DocId, (key, DocTermIds field, DocFeatVals feature))] ->
                       [TermId] ->
                       Map.Map DocId (DocImportance, TermRelevanceBreakdown)
cacheDocScoringInfo se completionTerms allTermDocInfo precedingTerms =
    Map.fromList
      [ (docid, (docImportance, termRelevances))
      | (docid, (_dockey, doctermids, docfeatvals)) <- allTermDocInfo
      , let docImportance  = relevanceScore se precedingTerms
                                            doctermids docfeatvals
            termRelevances = relevanceBreakdown se doctermids docfeatvals
                                                completionTerms
      ]

-- | Calculate the relevance of each of a given set of terms to the given
-- document.
--
-- We define the \"relevance\" of each term in a document to be its
-- term-in-document score as a fraction of the total of the scores for all
-- terms in the document. Thus the sum of all the relevance values in the
-- document is 1.
--
-- Note: we have to calculate the relevance for all terms in the document
-- but we only keep the relevance value for the terms of interest.
--
relevanceBreakdown :: forall doc key field feature.
                      (Ix field, Bounded field, Ix feature, Bounded feature) =>
                      SearchEngine doc key field feature ->
                      DocTermIds field -> DocFeatVals feature ->
                      [TermId] -> TermRelevanceBreakdown
relevanceBreakdown SearchEngine{ bm25Context } doctermids docfeatvals ts =
    let -- We'll calculate the bm25 score for each term in this document
        bm25Doc     = indexDocToBM25Doc doctermids docfeatvals

        -- Cache the info that depends only on this doc, not the terms
        termScore   :: (TermId -> (field -> Int) -> Float)
        termScore   = BM25F.scoreTermsBulk bm25Context bm25Doc

        -- The DocTermIds has the info we need to do bulk scoring, but it's
        -- a sparse representation, so we first convert it to a dense table
        term        :: Int -> TermId
        count       :: Int -> field -> Int
        (!numTerms, term, count) = DocTermIds.denseTable doctermids

        -- We generate the vector of scores for all terms, based on looking up
        -- the termid and the per-field counts in the dense table
        termScores  :: Vec.Vector Float
        !termScores = Vec.generate numTerms $ \i ->
                       termScore (term i) (\f -> count i f)

        -- We keep only the values for the terms we're interested in
        -- and normalise so we get the relevence fraction
        !scoreSum   = Vec.sum termScores
        !tset       = IntSet.fromList (map fromEnum ts)
     in Map.fromList
      . Vec.toList
      . Vec.map    (\(t,s) -> (t, s/scoreSum))
      . Vec.filter (\(t,_) -> fromEnum t `IntSet.member` tset)
      . Vec.imap   (\i s   -> (term i, s))
      $ termScores


scoreAutosuggestQueryCompletions :: [(TermId, DocIdSet)]
                                 -> Map.Map DocId (Float, Map.Map TermId Float)
                                 -> [(TermId, Float)]
scoreAutosuggestQueryCompletions completionTermAndDocSets allTermDocInfo =
    [ (t, candidateScore t ds_t)
    | (t, ds_t) <- completionTermAndDocSets ]
  where
    -- The score for a completion is the sum of the importance of the
    -- documents in which that completion occurs, weighted by the relevance
    -- of the term to each document. For example we can have a very
    -- important document and our completion term is highly relevant to it
    -- or we could have a large number of moderately important documents
    -- that our term is quite relevant to. In either example the completion
    -- term would score highly.
    candidateScore :: TermId -> DocIdSet -> Float
    candidateScore t ds_t =
      sum [ docImportance * termRelevence
          | Just (docImportance, termRelevances) <-
               map (`Map.lookup` allTermDocInfo) (DocIdSet.toList ds_t)
          , let Just termRelevence = Map.lookup t termRelevances
          ]


scoreAutosuggestQueryResults :: [(TermId, DocIdSet)] ->
                                Map.Map DocId (Float, Map.Map TermId Float) ->
                                [(TermId, Float)] ->
                                [(DocId, Float)]
scoreAutosuggestQueryResults completionTermAndDocSets allTermDocInfo
                             scoredCandidates =
  Map.toList $ Map.fromListWith (+)
    [ (docid, docImportance * score_t)
    | ((_, ds_t), (_, score_t)) <- zip completionTermAndDocSets scoredCandidates
    , let docids  = DocIdSet.toList ds_t
          docinfo = map (`Map.lookup` allTermDocInfo) docids
    , (docid, Just (docImportance, _)) <- zip docids docinfo
    ]


-----------------------------

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

-----------------------------

data NoFeatures = NoFeatures
  deriving (Eq, Ord, Bounded)

instance Ix NoFeatures where
  range   _   = []
  inRange _ _ = False
  index   _ _ = -1

noFeatures :: NoFeatures -> a
noFeatures _ = error "noFeatures"

