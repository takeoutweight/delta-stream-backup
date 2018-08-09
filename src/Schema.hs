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
import Control.Lens ((&), op)
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
        Table, TableEntity, (>=.), (==.), all_, defaultDbSettings, desc_, guard_,
        orderBy_, runSelectReturningOne, runSelectReturningList, select, val_, withDatabaseDebug)
import Database.Beam
import Database.Beam.Sqlite
import qualified Database.Beam as B
import qualified Database.Beam.Sqlite.Syntax as BSS
import qualified Database.SQLite.Simple as SQ
import GHC.Generics (Generic)
import qualified Filesystem.Path.CurrentOS as FP
import qualified System.Random as Random
import Prelude hiding (FilePath, head)
import qualified Turtle as Turtle

import qualified DBHelpers as DB
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
   , _location :: Columnar f Text
   , _sequence_number :: Columnar f Int
   , _check_time :: Columnar (Nullable f) UTCTime
   , _absolute_path :: Columnar f Text
   , _relative_path :: Columnar f Text
   , _filename :: Columnar f Text
   , _deleted :: Columnar f Int
   , _mod_time :: Columnar (Nullable f) UTCTime
   , _file_size :: Columnar (Nullable f) Int
   , _checksum :: Columnar (Nullable f) Text
   , _encrypted :: Columnar (Nullable f) Int
   , _encryption_key_id :: Columnar (Nullable f) Text
   , _actual :: Columnar f Int
   , _canonical :: Columnar f Int
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
           , "location TEXT"
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
           , "actual INTEGER"
           , "canonical INTEGER"
           , "provenance_type INTEGER" -- 0=MIRRRORED, 1=INGESTED, 2=UNEXPECTEDLY_CHANGED
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
     ( Has FileStateIdF rs
     , Has CheckTime rs
     , Has SequenceNumber rs
     , Has Location rs
     , Has RelativePathText rs
     , Has AbsPathText rs
     , Has Filename rs
     , Has Provenance rs
     , Has Canonical rs
     , Has Actual rs
     , Has FileDetailsR rs
     )
  => Record rs
  -> FileState
mkFileState ctx =
  (FileStateT
   { _file_state_id = Auto (nget FileStateIdF ctx)
   , _location = nget Location ctx
   , _sequence_number = nget SequenceNumber ctx
   , _check_time = nget CheckTime ctx
   , _absolute_path = nget AbsPathText ctx
   , _relative_path = nget RelativePathText ctx
   , _filename = nget Filename ctx
   , _deleted =
       case (nget FileDetailsR ctx) of
         Just _ -> 0
         Nothing -> 1
   , _mod_time = fmap (nget ModTime) (nget FileDetailsR ctx)
   , _file_size = fmap (nget FileSize) (nget FileDetailsR ctx)
   , _checksum = fmap (nget Checksum) (nget FileDetailsR ctx)
   , _encrypted =
       fmap
         (\fd ->
            (case fget fd of
               Unencrypted -> 0
               Encrypted _ -> 1))
         (nget FileDetailsR ctx)
   , _encryption_key_id =
       (case fmap fget (nget FileDetailsR ctx) of
          Just (Encrypted k) -> Just k
          _ -> Nothing)
   , _canonical =
       case fget ctx of
         NonCanonical -> 0
         Canonical -> 1
   , _actual =
       case fget ctx of
         Historical -> 0
         Actual -> 1
   , _provenance_type =
       case fget ctx of
         Mirrored _ -> 0
         Ingested -> 1
   , _provenance_id =
       case fget ctx of
         Mirrored i -> FileStateId (Just (Auto (Just i)))
         Ingested -> FileStateId Nothing
   })

-- | Inserts as Actual, disabling any previous state's Actuality if its not
-- already known to be gone
insertGoneFileState ::
     ( Has SQ.Connection rs
     , Has CheckTime rs
     , Has Location rs
     , Has RelativePathText rs
     , Has AbsPathText rs
     , Has Filename rs
     , Has Provenance rs
     )
  => Record rs
  -> IO ()
insertGoneFileState ctx =
  let insert =
        (do sequence <- nextSequenceNumber ctx
            (insertFileState
               (fget ctx)
               (mkFileState
                  (FileStateIdF Nothing &: SequenceNumber sequence &: Actual &:
                   FileDetailsR Nothing &:
                   NonCanonical &:
                   ctx))))
  in do afs <- getActualFileState ctx
        case fmap (\fs -> (fs, (nget FileDetailsR fs))) afs of
          -- Just bump stat time
          Just (fs, Nothing) ->
            updateFileState
              (fget ctx)
              (mkFileState (((fget ctx) :: CheckTime) &: fs))
          -- Make a new active entry
          Just (fs, Just _) -> do
            updateFileState (fget ctx) (mkFileState (Historical &: fs))
            insert
          Nothing -> return () -- no need for a gone entry for a file that was never seen.

{- | Inserts as Actual, disabling any previous state's Actuality if its not
     already known to be gone
-}
insertDetailedFileState ::
     ( Has SQ.Connection rs
     , Has CheckTime rs
     , Has Location rs
     , Has RelativePathText rs
     , Has AbsPathText rs
     , Has Filename rs
     , Has Provenance rs
     , HasFileDetails rs
     )
  => Record rs
  -> Maybe FileStateF
  -> IO ()
insertDetailedFileState ctx prevState =
  let insert canonical =
        (do sequence <- nextSequenceNumber ctx
            (insertFileState
               (fget ctx)
               (mkFileState
                  (FileStateIdF Nothing &: SequenceNumber sequence &: Actual &:
                   FileDetailsR (Just (fcast ctx)) &:
                   canonical &:
                   ctx))))
  in case prevState of
       Just fs ->
         case (nget FileDetailsR fs) == Just (fcast ctx) of
           True ->
             updateFileState
               (fget ctx)
               (mkFileState (((fget ctx) :: CheckTime) &: fs))
           False -> do
             updateFileState (fget ctx) (mkFileState (Historical &: fs))
             insert NonCanonical
       Nothing -> insert Canonical

data BadDBEncodingException = BadDBEncodingException
  { table :: !Text
  , id :: !(Auto Int)
  , errMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception BadDBEncodingException

data BadFileStateException = BadFileStateException
  { fs :: !FileStateF
  , bfErrMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception BadFileStateException

data FSNotFoundException = FSNotFoundException
  { fnfid :: !Int
  , fnfErrMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception FSNotFoundException

data ImpossibleDBResultException = ImpossibleDBResultException
  { idrRable :: !Text
  , idrErrMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception ImpossibleDBResultException

-- Concrete, non-extensible version of the file state table.
type FileStateF = Record '[FileStateIdF, Location, SequenceNumber, CheckTime, AbsPathText, RelativePathText, Filename, Provenance, Canonical, Actual, FileDetailsR]

-- | Can throw BadDBEncodingException if the assumptions are violated (which shouldn't happen if we're in control of the unFileState ::
unFileState :: FileState -> FileStateF
unFileState fs =
  FileStateIdF (unAuto (_file_state_id fs)) &: --
  Location (_location fs) &: --
  SequenceNumber (_sequence_number fs) &:
  CheckTime (_check_time fs) &:
  AbsPathText (_absolute_path fs) &:
  RelativePathText (_relative_path fs) &:
  Filename (_filename fs) &:
  -- Provenance
  (case ((_provenance_type fs), (_provenance_id fs))
         of
     (0, FileStateId (Just (Auto (Just pid)))) -> Mirrored pid
     (1, FileStateId Nothing) -> Ingested
     _ ->
       CE.throw
         (BadDBEncodingException
            "file_state"
            (_file_state_id fs)
            "Bad provenance encoding")) &:
  (case (_canonical fs) of
     0 -> NonCanonical
     1 -> Canonical
     _ ->
       CE.throw
         (BadDBEncodingException
            "file_state"
            (_file_state_id fs)
            "Bad Canonical encoding")) &:
  (case (_actual fs) of
     0 -> Historical
     1 -> Actual
     _ ->
       CE.throw
         (BadDBEncodingException
            "file_state"
            (_file_state_id fs)
            "Bad Actual encoding")) &:
  FileDetailsR
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

getActualFileState ::
     (Has SQ.Connection rs, Has Location rs, Has AbsPathText rs)
  => Record rs
  -> IO (Maybe FileStateF)
getActualFileState ctx =
  (withDatabaseDebug
     putStrLn
     ((fget ctx) :: SQ.Connection)
     (runSelectReturningOne
        (select
           (do fileState <- all_ (_file_state fileDB)
               guard_ (((_actual fileState) ==. val_ 1) &&.
                       ((_absolute_path fileState) ==. val_ (nget AbsPathText ctx)))
               pure fileState)))) &
  fmap (fmap unFileState)

getActualFileStateRelative ::
     SQ.Connection -> Location -> RelativePathText -> IO (Maybe FileStateF)
getActualFileStateRelative conn loc rel =
  (withDatabaseDebug
     putStrLn
     conn
     (runSelectReturningOne
        (select
           (do fileState <- all_ (_file_state fileDB)
               guard_ (((_actual fileState) ==. val_ 1) &&.
                       ((_relative_path fileState) ==. val_ (op RelativePathText rel)))
               pure fileState)))) &
  fmap (fmap unFileState)

--               (orderBy_
--               (\s -> (desc_ (_check_time s)))
--               (do shaCheck <- all_ (_file_state fileDB)
--                   guard_
--                     ((_sc_file_info_id shaCheck) ==.
--                      val_ (FileInfoId fileInfoID))
--                   return shaCheck))

getChangesSince :: (Has SequenceNumber rs,  Has SQ.Connection rs, Has Location rs) => Record rs -> IO [FileStateF]
getChangesSince ctx =
  (withDatabaseDebug
     putStrLn
     ((fget ctx) :: SQ.Connection)
     (runSelectReturningList
        (select
           (do fileState <- all_ (_file_state fileDB)
               guard_ ((_location fileState) ==. val_ (nget Location ctx))
               guard_ ((_actual fileState) ==. val_ 1)
               guard_
                 ((_sequence_number fileState) >=.
                  val_ (nget SequenceNumber ctx))
               pure fileState)))) &
  fmap (fmap unFileState)

{- | FilePath seems to only treat paths with trailing slashes as "directories" but
     eg `pwd` doesn't give a trailing slash.
-}
ensureTrailingSlash :: FP.FilePath -> FP.FilePath
ensureTrailingSlash fp = fp FP.</> ""

toRelative :: (Has Location r, Has AbsPath r) => Record r -> Maybe FP.FilePath
toRelative ctx =
  FP.stripPrefix
    (ensureTrailingSlash (fromString (T.unpack (nget Location ctx))))
    (nget AbsPath ctx)

toAbsolute :: (Has Location r, Has RelativePathText r) => Record r -> Text
toAbsolute ctx = (nget Location ctx) <> (nget RelativePathText ctx)

-- | Uncontroversial copy, i.e. if the file exists on target, the existing
--  entry's provenance was the same source.  source FileStateF MUST have an id
--  so we can track provenance. Records an intent.
copyFileState :: SQ.Connection -> FileStateF -> Location -> IO ()
copyFileState conn source target =
  case (nget FileStateIdF source) of
    Nothing ->
      (CE.throw (BadFileStateException source "Source FileState needs an id"))
    Just sourceId ->
      (case fget source of
         NonCanonical ->
           CE.throw
             (BadFileStateException source "Source FileState must be canonical")
         Canonical ->
           (do mPrevState <-
                 (getActualFileStateRelative
                    conn
                    target
                    ((fget source) :: RelativePathText))
               case mPrevState of
                 Nothing -> do
                   sequence <- nextSequenceNumber (conn &: target &: Nil)
                   (let nextState =
                          (SequenceNumber sequence &: Mirrored sourceId &:
                           CheckTime Nothing &:
                           target &:
                           source)
                    in (insertFileState
                          conn
                          (mkFileState
                             ((AbsPathText (toAbsolute nextState)) &: nextState))))
                 -- copy over if uncontroversial, Possibly mark existing record as historical etc.
                 -- Q: Cleaner if we enforced all values from a DB would DEFINITELY have an id?
                 Just prevState ->
                   case (nget FileStateIdF prevState) of
                     Nothing ->
                       (CE.throw
                          (BadFileStateException
                             source
                             "A value from the db doesn't have a DB in copyFileState. This should not happen."))
                     Just targetId ->
                       (do sourceIn <- getIngestionFS conn targetId
                           targetIn <- getIngestionFS conn targetId
                           -- "Noncontroversial" or not. The source ingestion point can update its own files w/o concern.
                           case (((fget sourceIn :: Location) ==
                                  (fget targetIn :: Location)))
                             -- Q: properly log a warning message that we're not propagating a change?
                                 of
                             False ->
                               Turtle.echo
                                 (Turtle.repr
                                    ("Warning: Not propagating, different ingestion location " ++
                                     (show source) ++
                                     ", sourceIn" ++
                                     (show sourceIn) ++
                                     ", targetIn" ++ (show targetIn)))
                             True ->
                               let sourceSeq = nget SequenceNumber sourceIn
                                   targetSeq = nget SequenceNumber targetIn
                               in case compare sourceSeq targetSeq of
                                    LT ->
                                      Turtle.echo
                                        (Turtle.repr
                                           ("Warning: Not propagating, source is an earlier version than target " ++
                                            (show source) ++
                                            ", sourceIn" ++
                                            (show sourceIn) ++
                                            ", targetIn" ++ (show targetIn)))
                                    EQ -> return () -- Benign NOOP, already copied this exact version.
                                    GT -> do
                                      sequence <-
                                        nextSequenceNumber
                                          (conn &: target &: Nil)
                                      (let nextState =
                                             (SequenceNumber sequence &:
                                              Mirrored sourceId &:
                                              CheckTime Nothing &:
                                              target &:
                                              source)
                                       in do (updateFileState
                                                conn
                                                (mkFileState
                                                   (Historical &: prevState)))
                                             (insertFileState
                                                conn
                                                (mkFileState
                                                   ((AbsPathText
                                                       (toAbsolute nextState)) &:
                                                    nextState)))))))


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

data FileDB f = FileDB
  { _file_state :: f (TableEntity FileStateT)
  } deriving (Generic)

instance Database FileDB

fileDB :: DatabaseSettings be FileDB
fileDB = defaultDbSettings

createFileStateSequenceCounterTable :: SQ.Query
createFileStateSequenceCounterTable =
  "CREATE TABLE IF NOT EXISTS file_state_sequence_counters " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "file_state_sequence_location TEXT PRIMARY KEY"
           , "file_state_sequence_counter INTEGER"
           ])))

-- | Follow mirror provenance to find the original ingestion record
-- Throws FSNotFoundException if any of the provenance pointers can't be resolved (this should not happen)
-- FIXME: Will infinitely loop if somehow a loop has created in the db.
getIngestionFS :: SQ.Connection -> Int -> IO FileStateF
getIngestionFS conn fileStateID = do
  fs <- (getFileStateById conn fileStateID)
  (case fs of
     Nothing ->
       (CE.throw
          (FSNotFoundException
             fileStateID
             "Can't find fileState in getIngestionVersion."))
     Just fs ->
       (let ufs = (unFileState fs)
        in (case (fget ufs) of
              Ingested -> return ufs
              Mirrored id' -> getIngestionFS conn id')))

-- | starts at 1, which is important as 0 is the "I haven't seen anything yet"
nextSequenceNumber :: (Has SQ.Connection rs, Has Location rs) => Record rs -> IO Int
nextSequenceNumber ctx =
  DB.withSavepoint
    (fget ctx)
    (do r <-
          SQ.query
            (fget ctx)
            "SELECT file_state_sequence_counter FROM file_state_sequence_counters WHERE file_state_sequence_location = ?"
            (SQ.Only (nget Location ctx))
        let num =
              case r of
                [SQ.Only n] -> n
                [] -> 0
                _ ->
                  CE.throw
                    (BadDBEncodingException
                       "file_state_sequence_counters"
                       (Auto Nothing)
                       "Too many location entries")
        SQ.execute
          (fget ctx)
          "INSERT OR REPLACE INTO file_state_sequence_counters (file_state_sequence_location, file_state_sequence_counter) VALUES (?,?)"
          ((nget Location ctx), num + 1)
        return num)

-- | 0 represents no previous squence number, since sequences start at 1.
getLastRequestSourceSequence :: SQ.Connection -> Location -> Location -> IO Int
getLastRequestSourceSequence conn (Location from) (Location to) = do
  r <-
    SQ.query
      conn
      "SELECT MAX(source_sequence) FROM requests WHERE active = 1 AND source_location = ? AND destination_location = ?"
      (from, to)
  case r of
    [SQ.Only (Just n)] -> return n
    [SQ.Only (Nothing)] -> return 0
    _ ->
      CE.throw
        (ImpossibleDBResultException
           "requests"
           "Returning muliple results for MAX?")

-- | TODO - copyFileState's all "uncontroversial" novelty from the source
-- location to the desination.
mirrorChangesFromLocation :: SQ.Connection -> Location -> Location -> IO ()
mirrorChangesFromLocation conn (Location from) (Location to) = undefined

-- | Records successful requests
createRequestTable :: SQ.Query
createRequestTable =
  "CREATE TABLE IF NOT EXISTS requests " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "request_id INTEGER PRIMARY KEY"
           , "destination_server TEXT KEY"
           , "destination_location TEXT"
           , "source_server TEXT"
           , "source_location TEXT"
           , "source_sequence INTEGER"
           , "active INTEGER"
           ])))

createDB filename =
  SQ.withConnection
    filename
    (\conn ->
       DB.withSavepoint
         conn
         (do (SQ.execute_ conn createFileStateSequenceCounterTable)
             (SQ.execute_ conn createFileStateTable)
             (SQ.execute_ conn createRequestTable)))

