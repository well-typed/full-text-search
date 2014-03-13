{-# LANGUAGE BangPatterns, GeneralizedNewtypeDeriving #-}
module Data.SearchEngine.DocIdSet (
    DocId(..),
    DocIdSet(..),
    null,
    size,
    empty,
    singleton,
    fromList,
    toList,
    insert,
    delete,
    union,
    unions,
    intersection,
    invariant,
  ) where

import Data.Word
import qualified Data.Vector.Unboxed         as Vec
import qualified Data.Vector.Unboxed.Mutable as MVec
import qualified Data.Vector.Generic.Base    as VecGen
import qualified Data.Vector.Unboxed.Base    as VecBase
import qualified Data.Vector.Generic.Mutable as VecMut
import Control.Monad.ST
import qualified Data.Set as Set
import Data.List (foldl', sortBy)
import Data.Function (on)

import Prelude hiding (null)


newtype DocId = DocId Word32
  deriving (Eq, Ord, Show, Enum, Bounded, Vec.Unbox,
            VecGen.Vector VecBase.Vector,
            VecMut.MVector VecBase.MVector)

newtype DocIdSet = DocIdSet (Vec.Vector DocId)
  deriving (Eq, Show)

-- represented as a sorted sequence of ids
invariant :: DocIdSet -> Bool
invariant (DocIdSet vec) =
    strictlyAscending (Vec.toList vec)
  where
    strictlyAscending (a:xs@(b:_)) = a < b && strictlyAscending xs
    strictlyAscending _  = True


size :: DocIdSet -> Int
size (DocIdSet vec) = Vec.length vec

null :: DocIdSet -> Bool
null (DocIdSet vec) = Vec.null vec

empty :: DocIdSet
empty = DocIdSet Vec.empty

singleton :: DocId -> DocIdSet
singleton = DocIdSet . Vec.singleton

fromList :: [DocId] -> DocIdSet
fromList = DocIdSet . Vec.fromList . Set.toAscList . Set.fromList

toList ::  DocIdSet -> [DocId]
toList (DocIdSet vec) = Vec.toList vec

insert :: DocId -> DocIdSet -> DocIdSet
insert x (DocIdSet vec) =
    case binarySearch vec 0 (Vec.length vec - 1) x of
      (_, True)  -> DocIdSet vec
      (i, False) -> case Vec.splitAt i vec of
                      (before, after) ->
                        DocIdSet (Vec.concat [before, Vec.singleton x, after])

delete :: DocId -> DocIdSet -> DocIdSet
delete x (DocIdSet vec) =
    case binarySearch vec 0 (Vec.length vec - 1) x of
      (_, False) -> DocIdSet vec
      (i, True)  -> case Vec.splitAt i vec of
                      (before, after) ->
                        DocIdSet (before Vec.++ Vec.tail after)

binarySearch :: Vec.Vector DocId -> Int -> Int -> DocId -> (Int, Bool)
binarySearch vec !a !b !key
  | a > b     = (a, False)
  | otherwise =
    let mid = (a + b) `div` 2
     in case compare key (vec Vec.! mid) of
          LT -> binarySearch vec a (mid-1) key
          EQ -> (mid, True)
          GT -> binarySearch vec (mid+1) b key

unions :: [DocIdSet] -> DocIdSet
unions = foldl' union empty
         -- a bit more effecient if we merge small ones first
       . sortBy (compare `on` size)

union :: DocIdSet -> DocIdSet -> DocIdSet
union x y | null x = y
          | null y = x
union (DocIdSet xs) (DocIdSet ys) =
    DocIdSet (Vec.create (MVec.new sizeBound >>= writeMergedUnion xs ys))
  where
    sizeBound = Vec.length xs + Vec.length ys

writeMergedUnion :: Vec.Vector DocId -> Vec.Vector DocId ->
                    MVec.MVector s DocId -> ST s (MVec.MVector s DocId)
writeMergedUnion xs0 ys0 !out = do
    i <- go xs0 ys0 0
    return $! MVec.take i out
  where
    go !xs !ys !i
      | Vec.null xs = do Vec.copy (MVec.slice i (Vec.length ys) out) ys
                         return (i + Vec.length ys)
      | Vec.null ys = do Vec.copy (MVec.slice i (Vec.length xs) out) xs
                         return (i + Vec.length xs)
      | otherwise   = let x = Vec.head xs; y = Vec.head ys
                      in case compare x y of
                          GT -> do MVec.write out i y
                                   go           xs  (Vec.tail ys) (i+1)
                          EQ -> do MVec.write out i x
                                   go (Vec.tail xs) (Vec.tail ys) (i+1)
                          LT -> do MVec.write out i x
                                   go (Vec.tail xs)           ys  (i+1)

intersection :: DocIdSet -> DocIdSet -> DocIdSet
intersection x y | null x = empty
                 | null y = empty
intersection (DocIdSet xs) (DocIdSet ys) =
    DocIdSet (Vec.create (MVec.new sizeBound >>= writeMergedIntersection xs ys))
  where
    sizeBound = max (Vec.length xs) (Vec.length ys)

writeMergedIntersection :: Vec.Vector DocId -> Vec.Vector DocId ->
                           MVec.MVector s DocId -> ST s (MVec.MVector s DocId)
writeMergedIntersection xs0 ys0 !out = do
    i <- go xs0 ys0 0
    return $! MVec.take i out
  where
    go !xs !ys !i
      | Vec.null xs = return i
      | Vec.null ys = return i
      | otherwise   = let x = Vec.head xs; y = Vec.head ys
                      in case compare x y of
                          GT ->    go           xs  (Vec.tail ys)  i
                          EQ -> do MVec.write out i x
                                   go (Vec.tail xs) (Vec.tail ys) (i+1)
                          LT ->    go (Vec.tail xs)           ys   i

