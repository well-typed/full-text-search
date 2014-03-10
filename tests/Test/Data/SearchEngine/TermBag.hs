{-# OPTIONS_GHC -fno-warn-orphans #-}
module Test.Data.SearchEngine.TermBag where

import Data.SearchEngine.TermBag

import qualified Data.Vector.Unboxed as Vec
import qualified Data.List as List
import Test.QuickCheck


instance Arbitrary TermBag where
  arbitrary = fromList `fmap` (listOf arbitrary)

instance Arbitrary TermId where
  arbitrary = TermId `fmap` choose (0,5)

prop_invariant :: TermBag -> Bool
prop_invariant = invariant

prop_elems :: [TermId] -> Bool
prop_elems tids =
    (map head . List.group . List.sort) tids
 == (elems . fromList) tids

prop_fromList :: [TermId] -> Bool
prop_fromList tids =
    (map (\g -> (head g, fromIntegral (length g `min` 255)))
     . List.group . List.sort) tids
 == (toList . fromList) tids

prop_size :: [TermId] -> Bool
prop_size tids =
    (size . fromList) tids == length tids

prop_termCount :: [TermId] -> Bool
prop_termCount tids =
    and [ termCount bag tid == count 
        | let bag = fromList tids 
        , (tid, count) <- toList bag
        ]

prop_denseTable1 :: [TermBag] -> Bool
prop_denseTable1 bags =
    Vec.toList terms == (List.sort . foldr List.union [] . map elems) bags
  where
    (terms, _) = denseTable bags

prop_denseTable2 :: [TermBag] -> Bool
prop_denseTable2 bags =
    and [ termCount bag (terms Vec.! t) == counts Vec.! (b * numTerms + t)
        | let (terms, counts) = denseTable bags
              numTerms        = Vec.length terms
        , (b, bag) <- zip [0..] bags
        ,  t       <- [0..Vec.length terms - 1]
        ] 
