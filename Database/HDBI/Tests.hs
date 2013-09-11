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
import Control.Concurrent
import Control.Concurrent.STM.TVar
import Control.Monad
import Control.Monad.STM
import Data.AEq
import Data.Decimal
import Data.Fixed
import Data.Int
import Data.List (intercalate, sort)
import Data.Monoid
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
  fromRow _ = error "FromRow: too few elemtns in the list must be 2"

instance (ToSql a, FromSql a, ToSql b, FromSql b, ToSql c, FromSql c) => SqlRow (a, b, c) where
  toRow (a, b, c) = [toSql a, toSql b, toSql c]
  fromRow [a, b, c] = (fromSql a, fromSql b, fromSql c)
  fromRow _ = error "FromRow: too few elemtns in the list must be 3"

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
  recreateTable "intdecs" [tfInteger tf, tfDecimal tf]
  recreateTable "intublobs" [tfInteger tf, tfUUID tf, tfBlob tf]
  recreateTable "table1" [tfInteger tf, tfInteger tf, tfInteger tf]
  where
    recreateTable tname fnames = do
      runRaw con $ "DROP TABLE IF EXISTS " <> tname
      runRaw con $ "CREATE TABLE " <> tname <> " (" <> vals <> ")"
      where
        vals = Query $ TL.pack $ intercalate ", "
               $ map (\(col :: Int, fname) -> "val" ++ show col ++ " " ++ (TL.unpack $ unQuery fname))
               $ zip [1..] fnames
    shortRC (tname, func) = recreateTable tname [func tf]

allTestsGroup :: (Connection con) => con -> Test
allTestsGroup con = testGroup "tests from package"
                    [ insertSelectTests con
                    , functionalProperties con
                    , testCases con
                    ]

functionalProperties :: (Connection con) => con -> Test
functionalProperties con = testGroup "Functional properties"
                           [ testProperty "select sum of integers" $ selectSumIntegers con
                           , testProperty "select ordered list" $ selectOrderedList con
                           ]

selectSumIntegers :: (Connection con) => con -> NonEmptyList Int32 -> Property
selectSumIntegers con v = QM.monadicIO $ do
  let vals = getNonEmpty v
  res <- QM.run $ withTransaction con $ do
    runRaw con "delete from integers"
    runMany con "insert into integers(val1) values (?)" $ map ((:[]) . toSql) vals
    withStatement con "select sum(val1) from integers" $ \st -> do
      executeRaw st
      [[r]] <- fetchAllRows st
      return $ fromSql r
  QM.stop $ res ?== (sum $ map toInteger vals)

selectOrderedList :: (Connection con) => con -> [Int32] -> Property
selectOrderedList con vals = QM.monadicIO $ do
  res <- QM.run $ withTransaction con $ do
    runRaw con "delete from integers"
    runMany con "insert into integers(val1) values (?)" $ map ((:[]) . toSql) vals
    withStatement con "select val1 from integers order by val1" $ \st -> do
      executeRaw st
      r <- fetchAllRows st
      return $ map (\[x] -> fromSql x) r
  QM.stop $ res ?== (sort vals)

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
             $ \(x :: [(Integer, Decimal)]) -> setsEqual c "intdecs" 2 x
           , testProperty "Insert many text"
             $ \(x :: [(Maybe Integer, UUID, Maybe B.ByteString)]) -> setsEqual c "intublobs" 3 x
           ]

setsEqual :: (Connection con, SqlRow row, Eq row, Ord row, Show row) => con -> Query -> Int -> [row] -> Property
setsEqual conn tname vcount values = QM.monadicIO $ do
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
               $ map (\c -> "val" ++ show c) [1..vcount]
    qmarks = Query $ TL.pack $ intercalate ", "
             $ replicate vcount "?"

preciseEqual :: (Eq a, Show a, FromSql a, ToSql a, Connection con) => con -> Query -> a -> Property
preciseEqual conn tname val = QM.monadicIO $ do
  res <- QM.run $ runInsertSelect conn tname val
  QM.stop $ res ?== val


approxEqual :: (Show a, AEq a, FromSql a, ToSql a, Connection con) => con -> Query -> a -> Property
approxEqual conn tname val = QM.monadicIO $ do
  res <- QM.run $ runInsertSelect conn tname val
  QM.stop $ res ?~== val

runInsertSelect :: (FromSql a, ToSql a, Connection con) => con -> Query -> a -> IO a
runInsertSelect conn tname val = withTransaction conn $ do
  runRaw conn $ "delete from " <> tname
  run conn ("insert into " <> tname <> "(val1) values (?)") [toSql val]
  withStatement conn ("select val1 from " <> tname) $ \st -> do
    executeRaw st
    [[ret]] <- fetchAllRows st
    return $ fromSql ret

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


-- | Check whether statement status changing properly or not
stmtStatus :: (Connection con) => con -> Assertion
stmtStatus c = do
  runRaw c "delete from integers"
  s <- prepare c "select * from integers"
  statementStatus s >>= (@?= StatementNew)
  executeRaw s
  statementStatus s >>= (@?= StatementExecuted)
  Nothing <- fetchRow s
  statementStatus s >>= (@?= StatementFetched)
  finish s
  statementStatus s >>= (@?= StatementFinished)
  reset s
  statementStatus s >>= (@?= StatementNew)

-- | Check whether `inTransaction` return True inside transaction or not
inTransactionStatus :: (Connection con) => con -> Assertion
inTransactionStatus c = do
  inTransaction c >>= (@?= False)
  withTransaction c $ do
    inTransaction c >>= (@?= True)

-- | Fresh connection has good status
connStatusGood :: (Connection con) => con -> Assertion
connStatusGood c = connStatus c >>= (@?= ConnOK)

-- | `clone` creates new independent connection
connClone :: (Connection con) => con -> Assertion
connClone c = do
  newc <- clone c
  connStatus newc >>= (@?= ConnOK)
  withTransaction newc $ inTransaction c >>= (@?= False)
  withTransaction c $ inTransaction newc >>= (@?= False)
  disconnect newc
  connStatus newc >>= (@?= ConnDisconnected)

-- | Checks that `getColumnNames` and `getColumnsCount` return right result
checkColumnNames :: (Connection con) => con -> Assertion
checkColumnNames c = do
  withStatement c "select val1, val2, val3 from table1" $ \s -> do
    executeRaw s
    getColumnNames s >>= (@?= ["val1", "val2", "val3"])
    getColumnsCount s >>= (@?= 3)

concurrentInserts :: (Connection con) => con -> Assertion
concurrentInserts c = do
  let threads = 1000
  v <- newTVarIO threads
  withTransaction c $ do
    runRaw c "delete from integers"
    replicateM_ threads $ forkIO $ onethread v
    atomically $ do               -- wait until all threads done
      x <- readTVar v
      when (x > 0) retry
  a <- withStatement c "select sum(val1) from integers" $ \st -> do
    executeRaw st
    [[res]] <- fetchAllRows st
    return $ fromSql res
  a @?= threads

  where
    onethread var = do
      run c "insert into integers (val1) values (?)" [toSql (1 :: Int)]
      atomically $ modifyTVar var (\a -> a - 1)
      return ()

testCases :: (Connection con) => con -> Test
testCases c = testGroup "Fixed tests"
           [ testCase "Statement status" $ stmtStatus c
           , testCase "inTransaction return right value" $ inTransactionStatus c
           , testCase "Connection status is good" $ connStatusGood c
           , testCase "Connection clone works" $ connClone c
           , testCase "Check right column names" $ checkColumnNames c
           , testCase "Concurent inserts dont fail" $ concurrentInserts c
           ]
