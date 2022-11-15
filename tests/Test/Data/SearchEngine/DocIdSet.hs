{-# OPTIONS_GHC -fno-warn-orphans #-}
module Test.Data.SearchEngine.DocIdSet where

import Data.SearchEngine.DocIdSet (DocIdSet(DocIdSet), DocId(DocId))
import qualified Data.SearchEngine.DocIdSet as DocIdSet

import qualified Data.Vector.Unboxed as Vec
import qualified Data.List as List
import Test.QuickCheck


instance Arbitrary DocIdSet where
  arbitrary = DocIdSet.fromList `fmap` (listOf arbitrary)

instance Arbitrary DocId where
  arbitrary = DocId `fmap` choose (0,15)


prop_insert :: DocIdSet -> DocId -> Bool
prop_insert dset x =
    let dset' = DocIdSet.insert x dset
     in DocIdSet.invariant dset && DocIdSet.invariant dset'
     && all (`member` dset') (x : DocIdSet.toList dset)

prop_delete :: DocIdSet -> DocId -> Bool
prop_delete dset x =
    let dset' = DocIdSet.delete x dset
     in DocIdSet.invariant dset && DocIdSet.invariant dset'
     && all (`member` dset') (List.delete x (DocIdSet.toList dset))
     && not (x `member` dset')

prop_delete' :: DocIdSet -> Bool
prop_delete' dset =
    all (prop_delete dset) (DocIdSet.toList dset)

prop_union :: DocIdSet -> DocIdSet -> Bool
prop_union dset1 dset2 =
    let dset  = DocIdSet.union dset1 dset2
        dset' = DocIdSet.fromList (List.union (DocIdSet.toList dset1) (DocIdSet.toList dset2))

     in DocIdSet.invariant dset && DocIdSet.invariant dset'
     && dset == dset'

prop_union' :: DocIdSet -> DocIdSet -> Bool
prop_union' dset1 dset2 =
    let dset   = DocIdSet.union dset1 dset2
        dset'  = List.foldl' (\s i -> DocIdSet.insert i s) dset1 (DocIdSet.toList dset2)
        dset'' = List.foldl' (\s i -> DocIdSet.insert i s) dset2 (DocIdSet.toList dset1)
     in DocIdSet.invariant dset && DocIdSet.invariant dset' && DocIdSet.invariant dset''
     && dset == dset'
     && dset' == dset''

member :: DocId -> DocIdSet -> Bool
member x (DocIdSet vec) =
   x `List.elem` Vec.toList vec

