{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
-- {-# LANGUAGE TypeSynonymInstances #-}
-- {-# LANGUAGE TypeApplications #-}

module Schema where

import qualified Data.ByteString.Builder as BSB
import qualified Data.DList as DL
import Data.Functor.Identity (Identity)
import Data.Monoid ((<>))
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import Data.String (fromString)
import qualified Data.Time as DT
import qualified Data.Text as T
import Database.Beam.Sqlite.Syntax 
import Database.Beam.Backend.SQL.SQL92 (HasSqlValueSyntax(..))
import qualified Data.String.Combinators as SC
import Database.Beam (Auto, Beamable, Columnar, Database, DatabaseSettings, PrimaryKey, Table, TableEntity, defaultDbSettings, withDatabaseDebug, runSelectReturningOne, select, orderBy_, desc_, guard_, val_, (==.), all_)
import Database.Beam
import Database.Beam.Sqlite
import qualified Database.Beam as B
import qualified Database.Beam.Sqlite.Syntax as BSS
import qualified Database.SQLite.Simple as SQ
import GHC.Generics (Generic)

-- import qualified DBHelpers as DB
import Fields

-- copied from Database.Beam.Sqlite.Syntax to allow UTCTime. Not sure this is
-- right, but seems like the LocalTime that we can write out of the box doesn't
-- serialize the timezone. You can specify timezone in the column, but what
-- enforces you writing the same time-zone back into the DB if you change that
-- later? Besides - Turtle uses UTCTime so this is easier.
emitValue :: SQ.SQLData -> BSS.SqliteSyntax
emitValue v = SqliteSyntax (BSB.byteString "?") (DL.singleton v)

instance HasSqlValueSyntax SqliteValueSyntax UTCTime where
  sqlValueSyntax tm =
    SqliteValueSyntax (emitValue (SQ.SQLText (fromString tmStr)))
    where
      tmStr =
        DT.formatTime
          DT.defaultTimeLocale
          (DT.iso8601DateFormat (Just "%H:%M:%S%Q"))
          tm

data FileStateT f = FileStateT
  { _file_state_id :: Columnar f (Auto Int)
   , _remote :: Columnar f Text
   , _sequence_number :: Columnar f Int
   , _check_time :: Columnar f UTCTime
   , _absolute_path :: Columnar f Text
   , _relative_path :: Columnar f Text
   , _filename :: Columnar f Text
   , _deleted :: Columnar f Int
   , _mod_time :: Columnar f UTCTime
   , _file_size :: Columnar f Int
   , _checksum :: Columnar f Text
   , _encrypted :: Columnar f Int
   , _encryption_key_id :: Columnar f Text
   , _superceded :: Columnar f Int
   , _provenance_type :: Columnar f Int
   , _provenance_id :: PrimaryKey FileStateT (Nullable f)
  } deriving (Generic)

createFileStateTable :: SQ.Query
createFileStateTable =
  "CREATE TABLE IF NOT EXISTS file_state " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "file_state_id INTEGER PRIMARY KEY"
           , "remote TEXT"
           , "sequence_number INTEGER"
           , "check_time TEXT"
           , "absolute_path TEXT"
           , "relative_path TEXT"
           , "filename TEXT"
           , "deleted INTEGER"
           , "mod_time TEXT"
           , "file_size INTEGER"
           , "checksum TEXT"
           , "encrypted INTEGER"
           , "encryption_key_id TEXT"
           , "superceded INTEGER"
           , "provenance_type INTEGER" -- MIRRRORED, INGESTED, UNEXPECTEDLY_CHANGED
           , "provenance_id__file_state_id INTEGER"
           ])))

type FileState = FileStateT Identity

type FileStateId = PrimaryKey FileStateT Identity

deriving instance Show FileStateId

deriving instance Show (PrimaryKey FileStateT (Nullable Identity))

deriving instance Eq FileStateId

deriving instance Eq (PrimaryKey FileStateT (Nullable Identity))

deriving instance Show FileState

deriving instance Eq FileState

-- deriving instance Show (FileStateT (Nullable Identity)) -- This always required for a nullable mixin?
-- deriving instance Eq (FileStateT (Nullable Identity))
-- deriving instance Show (PrimaryKey FileStateT (Nullable Identity))
-- deriving instance Eq (PrimaryKey FileStateT (Nullable Identity))
instance Beamable FileStateT

instance Table FileStateT where
  data PrimaryKey FileStateT f = FileStateId (Columnar f (Auto Int))
                            deriving Generic
  primaryKey = FileStateId . _file_state_id

instance Beamable (PrimaryKey FileStateT)

getFileState :: SQ.Connection -> Int -> IO (Maybe FileState)
getFileState conn fileStateID =
  (withDatabaseDebug
     putStrLn
     conn
     (runSelectReturningOne
        (select
           (do fileState <- all_ (_file_state fileDB)
               guard_ ((_file_state_id fileState) ==. val_ (Auto (Just fileStateID)))
               pure fileState))))

createFileStateSequenceCounterTable :: SQ.Query
createFileStateSequenceCounterTable =
  "CREATE TABLE IF NOT EXISTS file_state_sequence_counter " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "file_state_sequence_remote TEXT PRIMARY KEY"
           , "file_state_sequence_counter INTEGER"
           ])))

data FileDB f = FileDB
  { _file_state :: f (TableEntity FileStateT)
  } deriving (Generic)

instance Database FileDB

fileDB :: DatabaseSettings be FileDB
fileDB = defaultDbSettings
