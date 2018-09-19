import qualified Test.RoboCom.Indicators
import qualified Test.RoboCom.Utils

import Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "Tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Properties" [Test.RoboCom.Indicators.unitTests, Test.RoboCom.Utils.unitTests]
