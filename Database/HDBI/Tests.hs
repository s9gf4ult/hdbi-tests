{-# LANGUAGE
  OverloadedStrings
, ScopedTypeVariables
, FlexibleInstances
, FlexibleContexts
  #-}

module Database.HDBI.Tests
       (
         TestFieldTypes (..)
       , allTests
       ) where

import Control.Applicative
import Data.AEq
import Data.Decimal
import Data.Int
import Data.Monoid
import Data.List (intercalate)
import Data.Fixed
import Data.Time
import Data.UUID
import Data.Word
import Database.HDBI
import Test.Framework
import Test.Framework.Providers.HUnit
import Test.Framework.Providers.QuickCheck2
import Test.HUnit ((@?=), Assertion)
import Test.QuickCheck
import Test.QuickCheck.Assertions
import Test.QuickCheck.Instances ()
import qualified Data.ByteString as B
import qualified Data.Set as S
import qualified Data.Text.Lazy as TL
import qualified Test.QuickCheck.Monadic as QM

-- | Auxiliary class to simplify tests writing
class SqlRow a where
  toRow :: a -> [SqlValue]
  fromRow :: [SqlValue] -> a

instance (ToSql a, FromSql a, ToSql b, FromSql b) => SqlRow (a, b) where
  toRow (a, b) = [toSql a, toSql b]
  fromRow [a, b] = (fromSql a, fromSql b) -- quick and dirty - just for tests

instance (ToSql a, FromSql a, ToSql b, FromSql b, ToSql c, FromSql c) => SqlRow (a, b, c) where
  toRow (a, b, c) = [toSql a, toSql b, toSql c]
  fromRow [a, b, c] = (fromSql a, fromSql b, fromSql c)

  
instance Arbitrary (DecimalRaw Integer) where
  arbitrary = Decimal <$> arbitrary <*> arbitrary

instance Arbitrary UUID where
  arbitrary = fromWords
              <$> arbitrary
              <*> arbitrary
              <*> arbitrary
              <*> arbitrary

-- | Database specific type names for each SqlValue type
data TestFieldTypes = TestFieldTypes
                      { tfDecimal :: Query
                      , tfInteger :: Query
                      , tfDouble :: Query
                      , tfText :: Query
                      , tfBlob :: Query
                      , tfBool :: Query
                      , tfBitField :: Query
                      , tfUUID :: Query
                      , tfUTCTime :: Query
                      , tfLocalDate :: Query
                      , tfLocalTimeOfDay :: Query
                      , tfLocalTime :: Query
                      }

allTests :: (Connection con) => TestFieldTypes -> con -> Test
allTests tf con = buildTest $ do
  createTables tf con
  return $ allTestsGroup con

createTables :: (Connection con) => TestFieldTypes -> con -> IO ()
createTables tf con = do
  mapM_ shortRC
    [("decimals", tfDecimal)
    ,("integers", tfInteger)
    ,("doubles", tfDouble)
    ,("texts", tfText)
    ,("blobs", tfBlob)
    ,("bools", tfBool)
    ,("bitfields", tfBitField)
    ,("uuids", tfUUID)
    ,("utctimes", tfUTCTime)
    ,("localdates", tfLocalDate)
    ,("localtimeofdays", tfLocalTimeOfDay)
    ,("localtimes", tfLocalTime)
    ]
  recreateTable "intdec" [tfInteger tf, tfDecimal tf]
  recreateTable "intublobs" [tfInteger tf, tfUUID tf, tfBlob tf]
  where
    recreateTable tname fnames = do
      runRaw con $ "DROP TABLE IF EXISTS " <> tname
      runRaw con $ "CREATE TABLE " <> tname <> " (" <> vals <> ")"
      where
        vals = Query $ TL.pack $ intercalate ", "
               $ map (\(col, fname) -> "val" ++ show col ++ " " ++ (TL.unpack $ unQuery fname))
               $ zip [1..] fnames
    shortRC (tname, func) = recreateTable tname [func tf]

allTestsGroup :: (Connection con) => con -> Test
allTestsGroup con = testGroup "tests from package"
                    [ insertSelectTests con
                    ]

insertSelectTests :: (Connection con) => con -> Test
insertSelectTests c = testGroup "Can insert and select"
           [ testProperty "Decimal" $ \(d :: Decimal) -> preciseEqual c "decimals" d
           , testProperty "Int32" $ \(i :: Int32) -> preciseEqual c "integers" i
           , testProperty "Int64" $ \(i :: Int64) -> preciseEqual c "integers" i
           , testProperty "Integer" $ \(i :: Integer) -> preciseEqual c "decimals" i
           , testProperty "Double" $ \(d :: Double) -> approxEqual c "doubles" d
           , testProperty "Text" $ forAll genText $ \(t :: TL.Text) -> preciseEqual c "texts" t
           , testProperty "ByteString" $ \(b :: B.ByteString) -> preciseEqual c "blobs" b
           , testProperty "Bool" $ \(b :: Bool) -> preciseEqual c "bools" b
           , testProperty "UUID" $ \(u :: UUID) -> preciseEqual c "uuids" u
           , testProperty "BitField" $ \(w :: Word64) -> preciseEqual c "bitfields" (BitField w)
           , testProperty "UTCTime" $ forAll genUTC $ \(u :: UTCTime) -> preciseEqual c "utctimes" u
           , testProperty "Day" $ \(d :: Day) -> preciseEqual c "localdates" d
           , testProperty "TimeOfDay" $ forAll genTOD $ \(tod :: TimeOfDay) -> preciseEqual c "localtimeofdays" tod
           , testProperty "LocalTime" $ forAll genLT $ \(lt :: LocalTime) -> preciseEqual c "localtimes" lt
           , testProperty "Null" $ preciseEqual c "integers" SqlNull
           , testProperty "Maybe Integer" $ \(val :: Maybe Integer) -> preciseEqual c "integers" val
           , testProperty "Maybe ByteString" $ \(val :: Maybe B.ByteString) -> preciseEqual c "blobs" val
           , testProperty "Insert many numbers"
             $ \(x :: [(Integer, Decimal)]) -> setsEqual c "intdecs" x
           , testProperty "Insert many text"
             $ \(x :: [(Maybe Integer, UUID, Maybe B.ByteString)]) -> setsEqual c "intublobs" x
           ]



runInsertSelect :: (FromSql a, ToSql a, Connection con) => con -> Query -> a -> IO a
runInsertSelect conn tname val = withTransaction conn $ do
  runRaw conn $ "delete from " <> tname
  run conn ("insert into " <> tname <> "(val1) values (?)") [toSql val]
  withStatement conn ("select val1 from " <> tname) $ \st -> do
    executeRaw st
    [[ret]] <- fetchAllRows st
    return $ fromSql ret

setsEqual :: (Connection con, SqlRow row, Eq row, Ord row, Show row) => con -> Query -> [row] -> Property
setsEqual conn tname values = QM.monadicIO $ do
  ret <- QM.run $ withTransaction conn $ do
    runRaw conn $ "delete from " <> tname
    runMany conn ("insert into " <> tname <> "(" <> valnames <> ") values (" <> qmarks <> ")")
      $ map toRow values
    withStatement conn ("select " <> valnames <> " from " <> tname) $ \st -> do
      executeRaw st
      r <- fetchAllRows st
      return $ map fromRow r
  
  QM.stop $ (S.fromList values) ==? (S.fromList ret)
  where
    valnames = Query $ TL.pack $ intercalate ", "
               $ map (\c -> "val" ++ show c) [1..length values]
    qmarks = Query $ TL.pack $ intercalate ", "
             $ replicate (length values) "?"

preciseEqual :: (Eq a, Show a, FromSql a, ToSql a, Connection con) => con -> Query -> a -> Property
preciseEqual conn tname val = QM.monadicIO $ do
  res <- QM.run $ runInsertSelect conn tname val
  QM.stop $ res ?== val


approxEqual :: (Show a, AEq a, FromSql a, ToSql a, Connection con) => con -> Query -> a -> Property
approxEqual conn tname val = QM.monadicIO $ do
  res <- QM.run $ runInsertSelect conn tname val
  QM.stop $ res ?~== val

-- | Generate Text without 'NUL' symbols
genText :: Gen TL.Text
genText = TL.filter fltr <$> arbitrary
  where
    fltr '\NUL' = False         -- NULL truncates C string when pass to libpq binding.
    fltr _ = True

genTOD :: Gen TimeOfDay
genTOD = roundTod <$> arbitrary

genLT :: Gen LocalTime
genLT = rnd <$> arbitrary
  where
    rnd x@(LocalTime {localTimeOfDay = t}) = x {localTimeOfDay = roundTod t}

-- | Strip TimeOfDay to microsecond precision
roundTod :: TimeOfDay -> TimeOfDay
roundTod x@(TimeOfDay {todSec = s}) = x {todSec = anyToMicro s}

genUTC :: Gen UTCTime
genUTC = rnd <$> arbitrary
  where
    rnd x@(UTCTime {utctDayTime = d}) = x {utctDayTime = anyToMicro d}

anyToMicro :: (Fractional b, Real a) => a -> b
anyToMicro a = fromRational $ toRational ((fromRational $ toRational a) :: Micro)



-- stmtStatus :: PostgreConnection -> Assertion
-- stmtStatus c = do
--   runRaw c "drop table table1"
--   runRaw c "create table table1 (val bigint)" -- Just for postgre 9
--   s <- prepare c "select * from table1"
--   statementStatus s >>= (@?= StatementNew)
--   executeRaw s
--   statementStatus s >>= (@?= StatementExecuted)
--   _ <- fetchRow s
--   statementStatus s >>= (@?= StatementFetched)
--   finish s
--   statementStatus s >>= (@?= StatementFinished)
--   reset s
--   statementStatus s >>= (@?= StatementNew)

-- inTransactionStatus :: PostgreConnection -> Assertion
-- inTransactionStatus c = do
--   inTransaction c >>= (@?= False)
--   withTransaction c $ do
--     inTransaction c >>= (@?= True)

-- connStatusGood :: PostgreConnection -> Assertion
-- connStatusGood c = connStatus c >>= (@?= ConnOK)

-- connClone :: PostgreConnection -> Assertion
-- connClone c = do
--   newc <- clone c
--   connStatus newc >>= (@?= ConnOK)
--   withTransaction newc $ inTransaction c >>= (@?= False)
--   withTransaction c $ inTransaction newc >>= (@?= False)
--   disconnect newc
--   connStatus newc >>= (@?= ConnDisconnected)

-- checkColumnNames :: PostgreConnection -> Assertion
-- checkColumnNames c = do
--   withTransaction c $ do
--     runRaw c "drop table if exists table1"
--     runRaw c "create table table1 (val1 bigint, val2 bigint, val3 bigint)"
--     s <- prepare c "select val1, val2, val3 from table1"
--     executeRaw s
--     getColumnNames s >>= (@?= ["val1", "val2", "val3"])
--     getColumnsCount s >>= (@?= 3)
--     finish s

-- testG3 :: PostgreConnection -> Test
-- testG3 c = testGroup "Fixed tests"
--            [ testCase "Statement status" $ stmtStatus c
--            , testCase "inTransaction return right value" $ inTransactionStatus c
--            , testCase "Connection status is good" $ connStatusGood c
--            , testCase "Connection clone works" $ connClone c
--            , testCase "Check driver name" $ hdbiDriverName c @?= "postgresql"
--            , testCase "Check transaction support" $ dbTransactionSupport c @?= True
--            , testCase "Check right column names" $ checkColumnNames c
--            ]

-- main :: IO ()
-- main = do
--   a <- getArgs
--   case a of
--     (conn:args) -> do
--       c <- connectPostgreSQL $ TL.pack conn
--       (flip defaultMainWithArgs) args [ testG1 c
--                                       , testG3 c
--                                       ]
--       disconnect c

--     _ -> do
--       mapM_ putStrLn [ "Need at least one argument as connection string"
--                      , "the rest will be passed as arguments to test-framework"]
--       exitWith $ ExitFailure 1
