module Main where

import Data.SearchEngine
import PackageSearch

import PackageIndexUtils

import Data.List
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Time

import Control.Monad
import Control.Exception
import System.IO
import System.Directory
import System.Exit

import Distribution.PackageDescription (PackageDescription)
import Distribution.PackageDescription.Configuration (flattenPackageDescription)
import Distribution.Package (packageVersion, packageName)
import Distribution.Text (display)


-------------------
-- Main experiment
--

main :: IO ()
main = do
    putStrLn "reading 00-index.tar..."
    pkgs <- readPackages

    putStrLn "forcing pkgs..."
    evaluate (foldl' (\a p -> seq p a) () pkgs)

    let searchengine = insertDocs pkgs initialPkgSearchEngine

    putStrLn "constructing index..."
    printTiming "done" $
      evaluate searchengine >> return ()
    putStrLn $ "search engine invariant: " ++ show (invariant searchengine)

--    print [ avgFieldLength ctx s | s <- [minBound..maxBound] ]

--    print $ take 100 $ sortBy (flip compare) $ map Set.size $ Map.elems (termMap searchindex)
--    T.putStr $ T.unlines $ Map.keys (termMap searchindex)
--    let SearchEngine{searchIndex=SearchIndex{termMap, termIdMap, docKeyMap, docIdMap}} = searchengine
--    print (Map.size termMap, IntMap.size termIdMap, Map.size docKeyMap, IntMap.size docIdMap)

    let loop = do
          putStr "search term> "
          hFlush stdout 
          t <- T.getLine
          unless (T.null t) $ do
            putStrLn "Ranked results:"
            let rankedResults = query searchengine (T.words t)

            putStr $ unlines
              [ {-show rank ++ ": " ++ -}display pkgname
              | ({-rank, -}pkgname) <- take 10 rankedResults ]

            loop
    return ()
    loop

printTiming :: String -> IO () -> IO ()
printTiming msg action = do
    t   <- getCurrentTime
    action
    t'  <- getCurrentTime
    putStrLn (msg ++ ". time: " ++ show (diffUTCTime t' t))

readPackages :: IO [PackageDescription]
readPackages = do
  exists <- doesFileExist "00-index.tar"
  when (not exists) $ do
    putStrLn "This program needs a 00-index.tar package index."
    putStrLn "Please grab 00-index.tar.gz from hackage and gunzip it."
    exitFailure

  pkgs <- PackageIndexUtils.readPackageIndexFile "00-index.tar"
  let latestPkgs = Map.fromListWith
                     (\a b -> if packageVersion (fst a) > packageVersion (fst b)
                                then a else b)
                     [ (packageName pkgid, (pkgid, pkg))
                     | (pkgid, pkg) <- pkgs ]

  return . map (flattenPackageDescription . snd)
         . Map.elems
         $ latestPkgs

