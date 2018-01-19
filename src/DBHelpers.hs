{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

module DBHelpers where

import qualified Control.Monad.Catch as Catch
import qualified Control.Monad.IO.Class as CM
import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import Data.String (fromString)
import Data.Monoid ((<>))
import qualified Database.Beam as DB
import qualified Database.Beam.Backend.SQL as DBS
import qualified Database.Beam.Backend.SQL.SQL92 as SQL92
import qualified Database.Beam.Query as DBQ
import qualified Database.Beam.Query.Internal as DBQI
import qualified Database.Beam.Sqlite as DBS -- Doesn't like my selectOne type
                                             -- unless I import this, even
                                             -- though I don't use it explicitly
import qualified Database.Beam.Sqlite.Syntax as DBSS
import qualified Database.Beam.Sqlite.Types as DBST
import qualified Database.SQLite.Simple as SQ
import qualified System.Random as Random

data SelectOne a = None | One a | Some a a deriving (Show, Eq, Generic, Typeable)

{- | Returns None, One or Some of a collection.
     selectOne :: (MonadBeam syntax be handle m, FromBackendRow be a) => syntax -> m (Maybe (Maybe a))
-}
selectOne' ::
     (DBS.MonadBeam cmd be handle m, DBS.FromBackendRow be a, DBS.IsSql92Syntax cmd)
  => DBQ.SqlSelect (DBS.Sql92SelectSyntax cmd) t
  -> m (SelectOne a)
selectOne' (DBQ.SqlSelect s) =
  DBS.runReturningMany (DBS.selectCmd s) $ \next -> do
    a <- next
    case a of
      Nothing -> pure None
      Just x -> do
        a' <- next
        case a' of
          Nothing -> pure (One x)
          Just x2 -> pure (Some x x2)

-- | Bundling up a ton of those Beam wrappers into the command. I think I can't
-- really annotate this w/o the partial type sig, which stands for
-- Database.Beam.Query.QueryInaccessible

selectExactlyOne ::
     ( CM.MonadIO io
     , (DBQI.ProjectibleWithPredicate DBQI.ValueContext DBSS.SqliteExpressionSyntax res)
     , (DBQI.ProjectibleWithPredicate DBQI.AnyType DBSS.SqliteExpressionSyntax res)
     , (DBS.FromBackendRow DBST.Sqlite a)
     )
  => SQ.Connection
  -> DBQI.Q DBSS.SqliteSelectSyntax db _ res
  -> io (SelectOne a)
selectExactlyOne conn query =
  (CM.liftIO
     (DBS.withDatabaseDebug putStrLn conn (selectOne' (DBQ.select query))))

{-
-- Database.Beam.Sqlite.Syntax specializes IsSql92SelectTableSyntax SqliteSelectTableSyntax
-- This can't actually be explicitly typed because SqliteProjectionSyntax isn't exported
all_
  :: (Database db,
      Table table) =>
     DatabaseEntity be db (TableEntity table)
     -> Q SqliteSelectSyntax
          db
          s
          (table (QExpr
                    (SqliteExpressionSyntax
                       (SqliteProjectionSyntax
                          SqliteSelectSyntax))
                    s))
-}

-- FIXME this doesn't typecheck
-- | If there are many, still returns the first one.
selectJustOne :: ( CM.MonadIO io
     , (DBQI.ProjectibleWithPredicate DBQI.ValueContext DBSS.SqliteExpressionSyntax res)
     , (DBQI.ProjectibleWithPredicate DBQI.AnyType DBSS.SqliteExpressionSyntax res)
     , (DBS.FromBackendRow DBST.Sqlite a)
     )
  => SQ.Connection
  -> DBQI.Q DBSS.SqliteSelectSyntax db _ res
  -> io (Maybe a)
-- selectJustOne conn query =
--   (CM.liftIO
--      (DBS.withDatabaseDebug putStrLn conn (DBQ.runSelectReturningOne (DBQ.select query))))
selectJustOne = undefined

myAll_
  :: (DB.Database db,
      SQL92.IsSql92SelectSyntax select,
      DB.Table table) =>
     DB.DatabaseEntity be db (DB.TableEntity table)
     -> DBQ.Q select -- DBSS.SqliteCommandSyntax 
          db
          s
          (table (DBQI.QExpr
                    (SQL92.Sql92SelectTableExpressionSyntax
                       (SQL92.Sql92SelectSelectTableSyntax
                         select)) -- DBSS.SqliteSelectSyntax
                    s))
myAll_ = DBQ.all_

-- | Runs the action in a transaction, rolling back on any unhandled exception
withSavepointM :: SQ.Connection -> IO a -> IO (Maybe a)
withSavepointM conn a = do
  gensym <-
    fmap
      (("Backup_withSavepoint" <>) . fromString . show . (`mod` 1000000000))
      ((Random.randomIO) :: IO Int)
  Catch.onException
    (do (SQ.execute_ conn ("SAVEPOINT " <> gensym))
        r <- a
        (SQ.execute_ conn ("RELEASE " <> gensym))
        return (Just r))
    (do (SQ.execute_ conn ("ROLLBACK TO " <> gensym))
        (SQ.execute_ conn ("RELEASE " <> gensym))
        return Nothing)

-- | Executes the action w/in a savepoint, exceptions rollback. (was this version better? I forget? I think this is the exception re-throwing version which is maybe better. Might have to release on the exception handler too?)
withSavepoint :: (CM.MonadIO io, Catch.MonadMask io) => SQ.Connection -> io a -> io a
withSavepoint conn action =
  Catch.mask $ \restore -> do
    idInt <- CM.liftIO Random.randomIO
    let savepoint =
          ("Backup_withSavepoint2" <>
           fromString (show (mod idInt (1000000000 :: Int))))
    CM.liftIO (SQ.execute_ conn ("SAVEPOINT " <> savepoint))
    r <-
      (restore action) `Catch.onException`
      (CM.liftIO
         (do (SQ.execute_ conn ("ROLLBACK TO " <> savepoint))
             (SQ.execute_ conn ("RELEASE " <> savepoint))))
    CM.liftIO (SQ.execute_ conn ("RELEASE " <> savepoint))
    return r

-- TODO: make a Database.Beam.Sqlite.Connection.runInsertReturningList using withSavepoint
-- Or just a non-list-returning one at least to shorten up that super long one I have to write.

-- :set -fno-warn-partial-type-signatures
