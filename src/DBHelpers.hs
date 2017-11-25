{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PartialTypeSignatures #-}

module DBHelpers where

import qualified Control.Monad.IO.Class as CM
import qualified Database.Beam.Backend.SQL as DBS
import qualified Database.Beam.Query as DBQ
import qualified Database.Beam.Query.Internal as DBQI
import qualified Database.Beam.Sqlite as DBS -- Doesn't like my selectOne type
                                             -- unless I import this, even
                                             -- though I don't use it explicitly
import qualified Database.Beam.Sqlite.Syntax as DBSS
import qualified Database.Beam.Sqlite.Types as DBST
import qualified Database.SQLite.Simple as SQ

-- | Returns Nothing if there are more than one, Just (Nothing) if there are
-- none, or Just (Just x) if there is one.
-- selectOne :: (MonadBeam syntax be handle m, FromBackendRow be a) => syntax -> m (Maybe (Maybe a))
selectOne' ::
     (DBS.MonadBeam cmd be handle m, DBS.FromBackendRow be a, DBS.IsSql92Syntax cmd)
  => DBQ.SqlSelect (DBS.Sql92SelectSyntax cmd) t
  -> m (Maybe (Maybe a))
selectOne' (DBQ.SqlSelect s) =
  DBS.runReturningMany (DBS.selectCmd s) $ \next -> do
    a <- next
    case a of
      Nothing -> pure (Just Nothing)
      Just x -> do
        a' <- next
        case a' of
          Nothing -> pure (Just (Just x))
          Just _ -> pure Nothing

-- | Bundling up a ton of those Beam wrappers into the command. I think I can't
-- really annotate this w/o the partial type sig, which stands for
-- Database.Beam.Query.QueryInaccessible

selectOne ::
     ( CM.MonadIO io
     , (DBQI.ProjectibleWithPredicate DBQI.ValueContext DBSS.SqliteExpressionSyntax res)
     , (DBQI.ProjectibleWithPredicate DBQI.AnyType DBSS.SqliteExpressionSyntax res)
     , (DBS.FromBackendRow DBST.Sqlite a)
     )
  => SQ.Connection
  -> DBQI.Q DBSS.SqliteSelectSyntax db _ res
  -> io (Maybe (Maybe a))
selectOne conn query =
  (CM.liftIO
     (DBS.withDatabaseDebug putStrLn conn (selectOne' (DBQ.select query))))

{-
-- Database.Beam.Sqlite.Syntax specializes IsSql92SelectTableSyntax SqliteSelectTableSyntax
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
