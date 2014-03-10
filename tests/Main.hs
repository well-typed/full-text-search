
import qualified Test.Data.SearchEngine.DocIdSet as DocIdSet
import qualified Test.Data.SearchEngine.TermBag  as TermBag

import Test.Tasty
import Test.Tasty.QuickCheck


main :: IO ()
main = defaultMain $
         testGroup ""
           [ docIdSetTests
           , termBagTests
           ]

docIdSetTests :: TestTree
docIdSetTests =
    testGroup "TermIdSet"
      [ testProperty "prop_insert"  DocIdSet.prop_insert
      , testProperty "prop_delete"  DocIdSet.prop_delete
      , testProperty "prop_delete'" DocIdSet.prop_delete'
      , testProperty "prop_union"   DocIdSet.prop_union
      , testProperty "prop_union'"  DocIdSet.prop_union'
      ]

termBagTests :: TestTree
termBagTests =
    testGroup "TermBag"
      [ testProperty "prop_invariant"   TermBag.prop_invariant
      , testProperty "prop_elems"       TermBag.prop_elems
      , testProperty "prop_fromList"    TermBag.prop_fromList
      , testProperty "prop_size"        TermBag.prop_size
      , testProperty "prop_termCount"   TermBag.prop_termCount
      , testProperty "prop_denseTable1" TermBag.prop_denseTable1
      , testProperty "prop_denseTable2" TermBag.prop_denseTable2
      ]


