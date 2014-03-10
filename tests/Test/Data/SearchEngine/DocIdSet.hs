{-# OPTIONS_GHC -fno-warn-orphans #-}
module Test.Data.SearchEngine.DocIdSet where

import Data.SearchEngine.DocIdSet

import qualified Data.Vector.Unboxed as Vec
import qualified Data.List as List
import Test.QuickCheck


instance Arbitrary DocIdSet where
  arbitrary = fromList `fmap` (listOf arbitrary)

instance Arbitrary DocId where
  arbitrary = DocId `fmap` choose (0,15)


prop_insert :: DocIdSet -> DocId -> Bool
prop_insert dset x =
    let dset' = insert x dset
     in invariant dset && invariant dset'
     && all (`member` dset') (x : toList dset)

prop_delete :: DocIdSet -> DocId -> Bool
prop_delete dset x =
    let dset' = delete x dset
     in invariant dset && invariant dset'
     && all (`member` dset') (List.delete x (toList dset))
     && not (x `member` dset')

prop_delete' :: DocIdSet -> Bool
prop_delete' dset =
    all (prop_delete dset) (toList dset)

prop_union :: DocIdSet -> DocIdSet -> Bool
prop_union dset1 dset2 =
    let dset  = union dset1 dset2
        dset' = fromList (List.union (toList dset1) (toList dset2))

     in invariant dset && invariant dset'
     && dset == dset'

prop_union' :: DocIdSet -> DocIdSet -> Bool
prop_union' dset1 dset2 =
    let dset   = union dset1 dset2
        dset'  = List.foldl' (\s i -> insert i s) dset1 (toList dset2)
        dset'' = List.foldl' (\s i -> insert i s) dset2 (toList dset1)
     in invariant dset && invariant dset' && invariant dset''
     && dset == dset'
     && dset' == dset''

member :: DocId -> DocIdSet -> Bool
member x (DocIdSet vec) =
   x `List.elem` Vec.toList vec

