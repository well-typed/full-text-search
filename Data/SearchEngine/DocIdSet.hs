{-# LANGUAGE BangPatterns, GeneralizedNewtypeDeriving, MultiParamTypeClasses,
             TypeFamilies #-}
module Data.SearchEngine.DocIdSet (
    DocId(DocId),
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
import qualified Data.Vector.Generic         as GVec
import qualified Data.Vector.Generic.Mutable as GMVec
import Control.Monad.ST
import Control.Monad (liftM)
import qualified Data.Set as Set
import Data.List (foldl', sortBy)
import Data.Function (on)

import Prelude hiding (null)


newtype DocId = DocId { unDocId :: Word32 }
  deriving (Eq, Ord, Show, Enum, Bounded)

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

------------------------------------------------------------------------------
-- verbose Unbox instances
--

instance MVec.Unbox DocId

newtype instance MVec.MVector s DocId = MV_DocId (MVec.MVector s Word32)

instance GMVec.MVector MVec.MVector DocId where
    basicLength          (MV_DocId v) = GMVec.basicLength v
    basicUnsafeSlice i l (MV_DocId v) = MV_DocId (GMVec.basicUnsafeSlice i l v)
    basicUnsafeNew     l              = MV_DocId `liftM` GMVec.basicUnsafeNew l
    basicUnsafeReplicate l x          = MV_DocId `liftM` GMVec.basicUnsafeReplicate l (unDocId x)
    basicUnsafeRead  (MV_DocId v) i   = DocId `liftM`    GMVec.basicUnsafeRead v i
    basicUnsafeWrite (MV_DocId v) i x = GMVec.basicUnsafeWrite v i (unDocId x)
    basicClear       (MV_DocId v)     = GMVec.basicClear v
    basicSet         (MV_DocId v) x   = GMVec.basicSet v (unDocId x)
    basicUnsafeGrow  (MV_DocId v) l   = MV_DocId `liftM` GMVec.basicUnsafeGrow v l
    basicUnsafeCopy  (MV_DocId v) (MV_DocId v') = GMVec.basicUnsafeCopy v v'
    basicUnsafeMove  (MV_DocId v) (MV_DocId v') = GMVec.basicUnsafeMove v v'
    basicOverlaps    (MV_DocId v) (MV_DocId v') = GMVec.basicOverlaps   v v'
    {-# INLINE basicLength #-}
    {-# INLINE basicUnsafeSlice #-}
    {-# INLINE basicOverlaps #-}
    {-# INLINE basicUnsafeNew #-}
    {-# INLINE basicUnsafeReplicate #-}
    {-# INLINE basicUnsafeRead #-}
    {-# INLINE basicUnsafeWrite #-}
    {-# INLINE basicClear #-}
    {-# INLINE basicSet #-}
    {-# INLINE basicUnsafeCopy #-}
    {-# INLINE basicUnsafeMove #-}
    {-# INLINE basicUnsafeGrow #-}

newtype instance Vec.Vector DocId = V_DocId (Vec.Vector Word32)

instance GVec.Vector Vec.Vector DocId where
    basicUnsafeFreeze (MV_DocId mv)  = V_DocId  `liftM` GVec.basicUnsafeFreeze mv
    basicUnsafeThaw   (V_DocId  v)   = MV_DocId `liftM` GVec.basicUnsafeThaw v
    basicLength       (V_DocId  v)   = GVec.basicLength v
    basicUnsafeSlice i l (V_DocId v) = V_DocId (GVec.basicUnsafeSlice i l v)
    basicUnsafeIndexM (V_DocId  v) i = DocId `liftM` GVec.basicUnsafeIndexM v i
    basicUnsafeCopy   (MV_DocId mv)
                      (V_DocId  v)   = GVec.basicUnsafeCopy mv v
    elemseq           (V_DocId  v) x = GVec.elemseq v (unDocId x)
    {-# INLINE basicUnsafeFreeze #-}
    {-# INLINE basicUnsafeThaw #-}
    {-# INLINE basicLength #-}
    {-# INLINE basicUnsafeSlice #-}
    {-# INLINE basicUnsafeIndexM #-}
    {-# INLINE basicUnsafeCopy #-}
    {-# INLINE elemseq #-}

