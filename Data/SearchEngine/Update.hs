{-# LANGUAGE BangPatterns, NamedFieldPuns, RecordWildCards #-}

module Data.SearchEngine.Update (

    -- * Managing documents to be searched
    insertDoc,
    insertDocs,
    deleteDoc,

  ) where

import Data.SearchEngine.Types
import qualified Data.SearchEngine.SearchIndex as SI
import qualified Data.SearchEngine.DocTermIds as DocTermIds

import Data.Ix
import Data.Array.Unboxed
import Data.List


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

