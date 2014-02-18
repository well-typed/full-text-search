{-# LANGUAGE OverloadedStrings, NamedFieldPuns #-}
module PackageSearch (
    PkgSearchEngine,
    initialPkgSearchEngine,
    defaultSearchRankParameters,
    PkgDocField(..),
  ) where

import Data.SearchEngine

import ExtractNameTerms
import ExtractDescriptionTerms

import Data.Ix
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as T
import NLP.Snowball

import Distribution.Package
import Distribution.PackageDescription
import Distribution.Text (display)


type PkgSearchEngine = SearchEngine
                         PackageDescription
                         PackageName
                         PkgDocField
                         NoFeatures

data PkgDocField = NameField
                 | SynopsisField
                 | DescriptionField
  deriving (Eq, Ord, Enum, Bounded, Ix, Show)

initialPkgSearchEngine :: PkgSearchEngine
initialPkgSearchEngine =
    initSearchEngine pkgSearchConfig defaultSearchRankParameters

pkgSearchConfig :: SearchConfig PackageDescription
                                PackageName PkgDocField NoFeatures
pkgSearchConfig =
    SearchConfig {
      documentKey           = packageName,
      extractDocumentTerms  = extractTokens,
      transformQueryTerm    = normaliseQueryToken,
      documentFeatureValue  = const noFeatures
  }
  where
    extractTokens :: PackageDescription -> PkgDocField -> [Text]
    extractTokens pkg NameField        = extractPackageNameTerms
                                           (display $ packageName pkg)
    extractTokens pkg SynopsisField    = extractSynopsisTerms
                                           stopWords (synopsis    pkg)
    extractTokens pkg DescriptionField = extractDescriptionTerms
                                           stopWords (description pkg)

    normaliseQueryToken :: Text -> PkgDocField -> Text
    normaliseQueryToken tok =
      let tokFold = T.toCaseFold tok
          tokStem = stem English tokFold
       in \field -> case field of
                      NameField        -> tokFold
                      SynopsisField    -> tokStem
                      DescriptionField -> tokStem

defaultSearchRankParameters :: SearchRankParameters PkgDocField NoFeatures
defaultSearchRankParameters =
    SearchRankParameters {
      paramK1,
      paramB,
      paramFieldWeights,
      paramFeatureWeights     = noFeatures,
      paramFeatureFunctions   = noFeatures,
      paramResultsetSoftLimit = 200,
      paramResultsetHardLimit = 400
    }
  where
    paramK1 :: Float
    paramK1 = 1.5

    paramB :: PkgDocField -> Float
    paramB NameField        = 0.9
    paramB SynopsisField    = 0.5
    paramB DescriptionField = 0.5

    paramFieldWeights :: PkgDocField -> Float
    paramFieldWeights NameField        = 20
    paramFieldWeights SynopsisField    = 5
    paramFieldWeights DescriptionField = 1


stopWords :: Set Term
stopWords =
  Set.fromList
    ["haskell","library","simple","using","interface","functions",
     "implementation","package","support","'s","based","for","a","and","the",
     "to","of","with","in","an","on","from","that","as","into","by","is",
     "some","which","or","like","your","other","can","at","over","be","it",
     "within","their","this","but","are","get","one","all","you","so","only",
     "now","how","where","when","up","has","been","about","them","then","see",
     "no","do","than","should","out","off","much","if","i","have","also"]

