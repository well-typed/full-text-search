-----------------------------------------------------------------------------
-- |
-- Module      :  Distribution.Client.IndexUtils
-- Copyright   :  (c) Duncan Coutts 2008
-- License     :  BSD-like
--
-- Maintainer  :  duncan@community.haskell.org
-- Stability   :  provisional
-- Portability :  portable
--
-- Extra utils related to the package indexes.
-----------------------------------------------------------------------------
module PackageIndexUtils (
    readPackageIndexFile
  ) where

import qualified Codec.Archive.Tar as Tar

import Distribution.Package
         ( PackageId, PackageIdentifier(..), PackageName(..) )
import Distribution.PackageDescription
         ( GenericPackageDescription )
import Distribution.PackageDescription.Parse
         ( parsePackageDescription )
import Distribution.ParseUtils
         ( ParseResult(..) )
import Distribution.Text
         ( simpleParse )
import Distribution.Simple.Utils
         ( fromUTF8 )

import Data.Maybe (fromMaybe)
import qualified Data.ByteString.Lazy as BS
import qualified Data.ByteString.Lazy.Char8 as BS.Char8
import Data.ByteString.Lazy (ByteString)
import System.FilePath (takeExtension, splitDirectories, normalise)


readPackageIndexFile :: FilePath -> IO [(PackageId, GenericPackageDescription)]
readPackageIndexFile indexFile =
      either fail return
    . parsePackageIndex
  =<< BS.readFile indexFile

-- | Parse an uncompressed \"00-index.tar\" repository index file represented
-- as a 'ByteString'.
--
parsePackageIndex :: ByteString
                  -> Either String [(PackageId, GenericPackageDescription)]
parsePackageIndex = accum [] . Tar.read
  where
    accum pkgs es = case es of
      Tar.Fail err   -> Left  (show err)
      Tar.Done       -> Right (reverse pkgs)
      Tar.Next e es' -> accum pkgs' es'
        where
          pkgs' = extract pkgs e

    extract pkgs entry =
       fromMaybe pkgs $ tryExtractPkg
      where
        tryExtractPkg = do
          (pkgid, pkg) <- extractPkg entry
          return ((pkgid, pkg):pkgs)


extractPkg :: Tar.Entry -> Maybe (PackageId, GenericPackageDescription)
extractPkg entry = case Tar.entryContent entry of
  Tar.NormalFile content _
     | takeExtension fileName == ".cabal"
    -> case splitDirectories (normalise fileName) of
        [pkgname,vers,_] -> case simpleParse vers of
          Just ver -> Just (pkgid, descr)
            where
              pkgid  = PackageIdentifier (PackageName pkgname) ver
              parsed = parsePackageDescription . fromUTF8 . BS.Char8.unpack
                                               $ content
              descr  = case parsed of
                ParseOk _ d -> d
                _           -> error $ "Couldn't read cabal file "
                                    ++ show fileName
          _ -> Nothing
        _ -> Nothing
  _ -> Nothing
  where
    fileName = Tar.entryPath entry
