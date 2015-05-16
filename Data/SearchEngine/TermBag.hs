{-# LANGUAGE BangPatterns, GeneralizedNewtypeDeriving, MultiParamTypeClasses,
             TypeFamilies #-}
module Data.SearchEngine.TermBag (
    TermId(TermId), TermCount,
    TermBag,
    size,
    fromList,
    toList,
    elems,
    termCount,
    denseTable,
    invariant
  ) where

import qualified Data.Vector.Unboxed         as Vec
import qualified Data.Vector.Unboxed.Mutable as MVec
import qualified Data.Vector.Generic         as GVec
import qualified Data.Vector.Generic.Mutable as GMVec
import Control.Monad.ST
import Control.Monad (liftM)
import qualified Data.Map as Map
import Data.Word (Word32, Word8)
import Data.Bits
import Data.List (sortBy, foldl')
import Data.Function (on)

newtype TermId = TermId { unTermId :: Word32 }
  deriving (Eq, Ord, Show, Enum)

instance Bounded TermId where
  minBound = TermId 0
  maxBound = TermId 0x00FFFFFF

data TermBag = TermBag !Int !(Vec.Vector TermIdAndCount)
  deriving Show

-- We sneakily stuff both the TermId and the bag count into one 32bit word
type TermIdAndCount = Word32
type TermCount      = Word8

-- Bottom 24 bits is the TermId, top 8 bits is the bag count
termIdAndCount :: TermId -> Int -> TermIdAndCount
termIdAndCount (TermId termid) freq =
      (min (fromIntegral freq) 255 `shiftL` 24)
  .|. (termid .&. 0x00FFFFFF)

getTermId :: TermIdAndCount -> TermId
getTermId word = TermId (word .&. 0x00FFFFFF)

getTermCount :: TermIdAndCount -> TermCount
getTermCount word = fromIntegral (word `shiftR` 24)

invariant :: TermBag -> Bool
invariant (TermBag _ vec) =
    strictlyAscending (Vec.toList vec)
  where
    strictlyAscending (a:xs@(b:_)) = getTermId a < getTermId b
                                  && strictlyAscending xs
    strictlyAscending _  = True

size :: TermBag -> Int
size (TermBag sz _) = sz

elems :: TermBag -> [TermId]
elems (TermBag _ vec) = map getTermId (Vec.toList vec)

toList :: TermBag -> [(TermId, TermCount)]
toList (TermBag _ vec) = [ (getTermId x, getTermCount x)
                         | x <- Vec.toList vec ]

termCount :: TermBag -> TermId -> TermCount
termCount (TermBag _ vec) =
    binarySearch 0 (Vec.length vec - 1)
  where
    binarySearch :: Int -> Int -> TermId -> TermCount
    binarySearch !a !b !key
      | a > b     = 0
      | otherwise =
        let mid         = (a + b) `div` 2
            tidAndCount = vec Vec.! mid
         in case compare key (getTermId tidAndCount) of
              LT -> binarySearch a (mid-1) key
              EQ -> getTermCount tidAndCount
              GT -> binarySearch (mid+1) b key

fromList :: [TermId] -> TermBag
fromList termids =
    let bag = Map.fromListWith (+) [ (t, 1) | t <- termids ]
        sz  = Map.foldl' (+) 0 bag
        vec = Vec.fromListN (Map.size bag)
                            [ termIdAndCount termid freq
                            | (termid, freq) <- Map.toAscList bag ]
     in TermBag sz vec

-- | Given a bunch of term bags, merge them into a table for easier subsequent
-- processing. This is bascially a sparse to dense conversion. Missing entries
-- are filled in with 0. We represent the table as one vector for the
-- term ids and a 2d array for the counts.
--
-- Unfortunately vector does not directly support 2d arrays and array does
-- not make it easy to trim arrays.
--
denseTable :: [TermBag] -> (Vec.Vector TermId, Vec.Vector TermCount)
denseTable termbags = 
    (tids, tcts)
  where
    -- First merge the TermIds into one array
    -- then make a linear pass to create the counts array
    -- filling in 0s or the counts as we find them
    !numBags   = length termbags
    !tids      = unionsTermId termbags
    !numTerms  = Vec.length tids
    !numCounts = numTerms * numBags
    !tcts      = Vec.create (do
                   out <- MVec.new numCounts
                   sequence_
                     [ writeMergedTermCounts tids bag out i
                     | (n, TermBag _ bag) <- zip [0..] termbags
                     , let i = n * numTerms ]
                   return out
                 )

writeMergedTermCounts :: Vec.Vector TermId -> Vec.Vector TermIdAndCount ->
                         MVec.MVector s TermCount -> Int -> ST s ()
writeMergedTermCounts xs0 ys0 !out i0 =
    -- assume xs & ys are sorted, and ys contains a subset of xs
    go xs0 ys0 i0
  where
    go !xs !ys !i
      | Vec.null ys = MVec.set (MVec.slice i (Vec.length xs) out) 0
      | Vec.null xs = return ()
      | otherwise   = let x   = Vec.head xs
                          ytc = Vec.head ys
                          y   = getTermId ytc
                          c   = getTermCount ytc
                      in case x == y of
                           True  -> do MVec.write out i c
                                       go (Vec.tail xs) (Vec.tail ys) (i+1)
                           False -> do MVec.write out i 0
                                       go (Vec.tail xs)           ys  (i+1)

-- | Given a set of term bags, form the set of TermIds
--
unionsTermId :: [TermBag] -> Vec.Vector TermId
unionsTermId tbs =
    case sortBy (compare `on` bagVecLength) tbs of
      []             -> Vec.empty
      [TermBag _ xs] -> (Vec.map getTermId xs)
      (x0:x1:xs)     -> foldl' union3 (union2 x0 x1) xs
  where
    bagVecLength (TermBag _ vec) = Vec.length vec

union2 :: TermBag -> TermBag -> Vec.Vector TermId
union2 (TermBag _ xs) (TermBag _ ys) =
    Vec.create (MVec.new sizeBound >>= writeMergedUnion2 xs ys)
  where
    sizeBound = Vec.length xs + Vec.length ys

writeMergedUnion2 :: Vec.Vector TermIdAndCount -> Vec.Vector TermIdAndCount ->
                     MVec.MVector s TermId -> ST s (MVec.MVector s TermId)
writeMergedUnion2 xs0 ys0 !out = do
    i <- go xs0 ys0 0
    return $! MVec.take i out
  where
    go !xs !ys !i
      | Vec.null xs = do Vec.copy (MVec.slice i (Vec.length ys) out)
                                  (Vec.map getTermId ys)
                         return (i + Vec.length ys)
      | Vec.null ys = do Vec.copy (MVec.slice i (Vec.length xs) out)
                                  (Vec.map getTermId xs)
                         return (i + Vec.length xs)
      | otherwise   = let x = getTermId (Vec.head xs)
                          y = getTermId (Vec.head ys)
                      in case compare x y of
                          GT -> do MVec.write out i y
                                   go           xs  (Vec.tail ys) (i+1)
                          EQ -> do MVec.write out i x
                                   go (Vec.tail xs) (Vec.tail ys) (i+1)
                          LT -> do MVec.write out i x
                                   go (Vec.tail xs)           ys  (i+1)

union3 :: Vec.Vector TermId -> TermBag -> Vec.Vector TermId
union3 xs (TermBag _ ys) =
    Vec.create (MVec.new sizeBound >>= writeMergedUnion3 xs ys)
  where
    sizeBound = Vec.length xs + Vec.length ys

writeMergedUnion3 :: Vec.Vector TermId -> Vec.Vector TermIdAndCount ->
                     MVec.MVector s TermId -> ST s (MVec.MVector s TermId)
writeMergedUnion3 xs0 ys0 !out = do
    i <- go xs0 ys0 0
    return $! MVec.take i out
  where
    go !xs !ys !i
      | Vec.null xs = do Vec.copy (MVec.slice i (Vec.length ys) out)
                                  (Vec.map getTermId ys)
                         return (i + Vec.length ys)
      | Vec.null ys = do Vec.copy (MVec.slice i (Vec.length xs) out) xs
                         return (i + Vec.length xs)
      | otherwise   = let x =            Vec.head xs
                          y = getTermId (Vec.head ys)
                      in case compare x y of
                          GT -> do MVec.write out i y
                                   go           xs  (Vec.tail ys) (i+1)
                          EQ -> do MVec.write out i x
                                   go (Vec.tail xs) (Vec.tail ys) (i+1)
                          LT -> do MVec.write out i x
                                   go (Vec.tail xs)           ys  (i+1)

------------------------------------------------------------------------------
-- verbose Unbox instances
--

instance MVec.Unbox TermId

newtype instance MVec.MVector s TermId = MV_TermId (MVec.MVector s Word32)

instance GMVec.MVector MVec.MVector TermId where
    basicLength          (MV_TermId v) = GMVec.basicLength v
    basicUnsafeSlice i l (MV_TermId v) = MV_TermId (GMVec.basicUnsafeSlice i l v)
    basicUnsafeNew     l              = MV_TermId `liftM` GMVec.basicUnsafeNew l
    basicUnsafeReplicate l x          = MV_TermId `liftM` GMVec.basicUnsafeReplicate l (unTermId x)
    basicUnsafeRead  (MV_TermId v) i   = TermId `liftM`    GMVec.basicUnsafeRead v i
    basicUnsafeWrite (MV_TermId v) i x = GMVec.basicUnsafeWrite v i (unTermId x)
    basicClear       (MV_TermId v)     = GMVec.basicClear v
    basicSet         (MV_TermId v) x   = GMVec.basicSet v (unTermId x)
    basicUnsafeGrow  (MV_TermId v) l   = MV_TermId `liftM` GMVec.basicUnsafeGrow v l
    basicUnsafeCopy  (MV_TermId v) (MV_TermId v') = GMVec.basicUnsafeCopy v v'
    basicUnsafeMove  (MV_TermId v) (MV_TermId v') = GMVec.basicUnsafeMove v v'
    basicOverlaps    (MV_TermId v) (MV_TermId v') = GMVec.basicOverlaps   v v'
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

newtype instance Vec.Vector TermId = V_TermId (Vec.Vector Word32)

instance GVec.Vector Vec.Vector TermId where
    basicUnsafeFreeze (MV_TermId mv)  = V_TermId  `liftM` GVec.basicUnsafeFreeze mv
    basicUnsafeThaw   (V_TermId  v)   = MV_TermId `liftM` GVec.basicUnsafeThaw v
    basicLength       (V_TermId  v)   = GVec.basicLength v
    basicUnsafeSlice i l (V_TermId v) = V_TermId (GVec.basicUnsafeSlice i l v)
    basicUnsafeIndexM (V_TermId  v) i = TermId `liftM` GVec.basicUnsafeIndexM v i
    basicUnsafeCopy   (MV_TermId mv)
                      (V_TermId  v)   = GVec.basicUnsafeCopy mv v
    elemseq           (V_TermId  v) x = GVec.elemseq v (unTermId x)
    {-# INLINE basicUnsafeFreeze #-}
    {-# INLINE basicUnsafeThaw #-}
    {-# INLINE basicLength #-}
    {-# INLINE basicUnsafeSlice #-}
    {-# INLINE basicUnsafeIndexM #-}
    {-# INLINE basicUnsafeCopy #-}
    {-# INLINE elemseq #-}

