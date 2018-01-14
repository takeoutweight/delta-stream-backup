{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
-- {-# LANGUAGE TypeSynonymInstances #-}
-- {-# LANGUAGE TypeApplications #-}

module Schema where

import qualified Control.Exception as CE
import qualified Control.Monad.Catch as Catch
import qualified Data.ByteString.Builder as BSB
import qualified Data.DList as DL
import Data.Functor.Identity (Identity)
import Data.Monoid ((<>))
import Data.String (fromString)
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import qualified Data.Time as DT
import qualified Data.Text as T
import Database.Beam.Sqlite.Syntax 
import Database.Beam.Backend.SQL.SQL92 (HasSqlValueSyntax(..))
import qualified Data.String.Combinators as SC
import Database.Beam
       (Auto, Beamable, Columnar, Database, DatabaseSettings, PrimaryKey,
        Table, TableEntity, (==.), all_, defaultDbSettings, desc_, guard_,
        orderBy_, runSelectReturningOne, select, val_, withDatabaseDebug)
import Database.Beam
import Database.Beam.Sqlite
import qualified Database.Beam as B
import qualified Database.Beam.Sqlite.Syntax as BSS
import qualified Database.SQLite.Simple as SQ
import GHC.Generics (Generic)
import qualified System.Random as Random

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
   , _mod_time :: Columnar (Nullable f) UTCTime
   , _file_size :: Columnar (Nullable f) Int
   , _checksum :: Columnar (Nullable f) Text
   , _encrypted :: Columnar (Nullable f) Int
   , _encryption_key_id :: Columnar (Nullable f) Text
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

instance Beamable FileStateT

instance Table FileStateT where
  data PrimaryKey FileStateT f = FileStateId (Columnar f (Auto Int))
                            deriving Generic
  primaryKey = FileStateId . _file_state_id

instance Beamable (PrimaryKey FileStateT)

-- | Makes the file state for a not-deleted file
mkFileState ::
     ( Has StatTime rs
     , Has Remote rs
     , Has RelativePathText rs
     , Has AbsPathText rs
     , Has Filename rs
     , Has ModTime rs
     , Has FileSize rs
     , Has Checksum rs
     , Has IsEncrypted rs
     , Has FileInfoIdText rs
     )
  => Record rs
  -> FileState
mkFileState ctx =
  (FileStateT
   { _file_state_id = Auto Nothing
   , _remote = nget Remote ctx
   , _sequence_number = 0 -- TODO
   , _check_time = nget StatTime ctx
   , _absolute_path = nget AbsPathText ctx
   , _relative_path = nget RelativePathText ctx
   , _filename = nget Filename ctx
   , _deleted = 0
   , _mod_time = Just (nget ModTime ctx)
   , _file_size = Just (nget FileSize ctx)
   , _checksum = Just (nget Checksum ctx)
   , _encrypted =
       Just
         (case ((fget ctx) :: IsEncrypted) of
            Unencrypted -> 0
            Encrypted _ -> 1)
   , _encryption_key_id =
       case ((fget ctx) :: IsEncrypted) of
         Unencrypted -> Nothing
         Encrypted k -> Just k
   , _superceded = 0 -- TODO
   , _provenance_type = 0 -- TODO
   , _provenance_id = FileStateId Nothing -- TODO FileInfoId (nget FileInfoIdText ctx)
   })

mkGoneFileState :: 
     ( Has StatTime rs
     , Has Remote rs
     , Has RelativePathText rs
     , Has AbsPathText rs
     , Has Filename rs
     , Has FileInfoIdText rs
     )
  => Record rs
  -> FileState
mkGoneFileState ctx =
  (FileStateT
   { _file_state_id = Auto Nothing
   , _remote = nget Remote ctx
   , _sequence_number = 0 -- TODO
   , _check_time = nget StatTime ctx
   , _absolute_path = nget AbsPathText ctx
   , _relative_path = nget RelativePathText ctx
   , _filename = nget Filename ctx
   , _deleted = 1
   , _mod_time = Nothing
   , _file_size = Nothing
   , _checksum = Nothing
   , _encrypted = Nothing
   , _encryption_key_id = Nothing
   , _superceded = 0 -- TODO
   , _provenance_type = 0 -- TODO
   , _provenance_id = FileStateId Nothing -- TODO FileInfoId (nget FileInfoIdText ctx)
   })

data BadDBEncodingException = BadDBEncodingException
  { table :: !Text
  , id :: !(Auto Int)
  , errMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception BadDBEncodingException

type FileStateF = Record '[ Remote, SequenceNumber, StatTime, AbsPathText, RelativePathText, Filename, Superceded, Provenance, FileDetails]

-- | Can throw BadDBEncodingException if the assumptions are violated (which shouldn't happen if we're in control of the DB)
unFileState ::
     FileState
  -> FileStateF
unFileState fs
    -- FileStateIdF (_file_state_id fs) &: --
 =
  Remote (_remote fs) &: --
  SequenceNumber (_sequence_number fs) &:
  StatTime (_check_time fs) &:
  AbsPathText (_absolute_path fs) &:
  RelativePathText (_relative_path fs) &:
  Filename (_filename fs) &:
  Superceded
    (case (_superceded fs) of
       0 -> False
       1 -> True
       _ ->
         CE.throw
           (BadDBEncodingException
              "file_state"
              (_file_state_id fs)
              "Superceded not 0 or 1")) &:
  -- Provenance
  (case ((_provenance_type fs), undefined) {-(_provenance_id fs)-}
         of
     (0, Just pid) -> Mirror pid
     (1, Nothing) -> Novel
     (2, Nothing) -> Unexpected
     _ ->
       CE.throw
         (BadDBEncodingException
            "file_state"
            (_file_state_id fs)
            "Bad provenance encoding")) &:
  FileDetails
    (case ( (_deleted fs)
          , (_mod_time fs)
          , (_file_size fs)
          , (_checksum fs)
          , (_encrypted fs)
          , (_encryption_key_id fs)) of
       (1, Nothing, Nothing, Nothing, Nothing, Nothing) -> Nothing
       (0, Just mt, Just fsz, Just cs, Just enc, encKey) ->
         Just
           (ModTime mt &: --
            FileSize fsz &:
            Checksum cs &:
            (case (enc, encKey) of
               (0, Nothing) -> Unencrypted
               (1, Just id) -> Encrypted id
               _ ->
                 CE.throw
                   (BadDBEncodingException
                      "file_state"
                      (_file_state_id fs)
                      "Bad encryption encoding")) &:
            Nil)
       _ ->
         CE.throw
           (BadDBEncodingException
              "file_state"
              (_file_state_id fs)
              "Unexpected data stored on with respect to deleted/undeleted status")) &:
  Nil

getFileStateById :: SQ.Connection -> Int -> IO (Maybe FileState)
getFileStateById conn fileStateID =
  (withDatabaseDebug
     putStrLn
     conn
     (runSelectReturningOne
        (select
           (do fileState <- all_ (_file_state fileDB)
               guard_ ((_file_state_id fileState) ==. val_ (Auto (Just fileStateID)))
               pure fileState))))

-- Could probably go via archive and relative path too - these are denormalized.

getFileState ::
     (Has Remote rs, Has Archive rs, Has AbsPath rs)
  => SQ.Connection
  -> Record rs
  -> IO (Maybe FileState)
getFileState conn ctx = undefined
--   (withDatabaseDebug
--      putStrLn
--      conn
--      (runSelectReturningOne
--         (select
--            (orderBy_
--               (\s -> (desc_ (_check_time s)))
--               (do shaCheck <- all_ (_file_state fileDB)
--                   guard_
--                     ((_sc_file_info_id shaCheck) ==.
--                      val_ (FileInfoId fileInfoID))
--                   return shaCheck)))))

updateFileState :: SQ.Connection -> FileState -> IO ()
updateFileState conn fileState =
    (withDatabaseDebug
     putStrLn
     conn
     (runUpdate (save (_file_state fileDB) fileState)))

insertFileState :: SQ.Connection -> FileState -> IO ()
insertFileState conn fileState =
  (withDatabaseDebug
     putStrLn
     conn
     (runInsert (insert (_file_state fileDB) (insertValues [fileState]))))

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
