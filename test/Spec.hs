import qualified Test.BarAggregator
import qualified Test.Driver.Junction.QuoteThread
import qualified Test.RoboCom.Indicators
import qualified Test.RoboCom.Utils

import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "Tests" [unitTests, properties]

unitTests :: TestTree
unitTests = testGroup "Unit Tests"
  [Test.RoboCom.Indicators.unitTests,
   Test.RoboCom.Utils.unitTests,
   Test.BarAggregator.unitTests,
   Test.Driver.Junction.QuoteThread.unitTests]

properties :: TestTree
properties = testGroup "Properties"
  [Test.BarAggregator.properties ]

