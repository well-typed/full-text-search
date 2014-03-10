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
    FeatureFunction(..),

    -- ** Helper type for non-term features
    NoFeatures,
    noFeatures,

    -- ** Managing documents to be searched
    insertDoc,
    insertDocs,
    deleteDoc,

    -- * Explain mode for query result rankings
    queryExplain,
    Explanation(..),
    setRankParams,

    -- * Internal sanity check
    invariant,
  ) where

import Data.SearchEngine.Types
import Data.SearchEngine.Update
import Data.SearchEngine.Query
import Data.SearchEngine.Autosuggest

