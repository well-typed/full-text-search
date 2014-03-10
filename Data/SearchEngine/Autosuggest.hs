{-# LANGUAGE BangPatterns, NamedFieldPuns, RecordWildCards,
             ScopedTypeVariables #-}

module Data.SearchEngine.Autosuggest (

    -- * Query auto-completion \/ auto-suggestion
    queryAutosuggest,
    ResultsFilter(..),

  ) where

import Data.SearchEngine.Types
import Data.SearchEngine.Query (ResultsFilter(..))
import qualified Data.SearchEngine.Query       as Query
import qualified Data.SearchEngine.SearchIndex as SI
import qualified Data.SearchEngine.DocIdSet    as DocIdSet
import qualified Data.SearchEngine.DocTermIds  as DocTermIds
import qualified Data.SearchEngine.BM25F       as BM25F

import Data.Ix
import Data.Ord
import Data.List
import Data.Maybe
import qualified Data.Map as Map
import qualified Data.IntSet as IntSet
import qualified Data.Vector.Unboxed as Vec


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
      , let docImportance  = Query.relevanceScore se precedingTerms
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
        bm25Doc     = Query.indexDocToBM25Doc doctermids docfeatvals

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

