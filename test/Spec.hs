import qualified Test.BarAggregator
import qualified Test.RoboCom.Indicators
import qualified Test.RoboCom.Positions
import qualified Test.RoboCom.Utils

import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "Tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit Tests"
  [Test.RoboCom.Indicators.unitTests,
   Test.RoboCom.Positions.unitTests,
   Test.RoboCom.Utils.unitTests,
   Test.BarAggregator.unitTests ]
