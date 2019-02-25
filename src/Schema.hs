{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NamedFieldPuns#-}
-- {-# LANGUAGE TypeSynonymInstances #-}
-- {-# LANGUAGE TypeApplications #-}

module Schema where

import qualified Control.Exception as CE
import Control.Lens ((&), op)
import qualified Control.Monad.Catch as Catch
import qualified Data.Binary as Binary
import qualified Data.ByteString.Base64 as Base64
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Char8 as BSChar8
import qualified Data.ByteString.Lazy as BSLazy
import qualified Data.List as List
import qualified Data.List.Extra as Extra
import qualified Data.DList as DL
import Data.Functor.Identity (Identity)
import qualified Data.Hashable as Hashable
import qualified Data.Maybe as Maybe
import Data.Monoid ((<>))
import Data.String (fromString)
import Data.Time.Clock (UTCTime)
import qualified Data.Time as DT
import qualified Data.Time.Clock as DTC
import qualified Data.Text as T
import qualified Data.Text.Encoding as Encoding
import Data.Text (Text)
import Database.Beam.Sqlite (runBeamSqliteDebug)
import Database.Beam.Sqlite.Syntax 
import Database.Beam.Backend.SQL.SQL92 (HasSqlValueSyntax(..))
import qualified Data.String.Combinators as SC
import Database.Beam
       (Beamable, Columnar, Database, DatabaseSettings, PrimaryKey, Table,
        TableEntity, (/=.), (==.), (>.), all_, defaultDbSettings, desc_,
        guard_, orderBy_, select, val_)
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
-- myEmitValue :: SQ.SQLData -> BSS.SqliteSyntax
-- myEmitValue v = SqliteSyntax (BSB.byteString "?") (DL.singleton v)
-- 
-- instance HasSqlValueSyntax SqliteValueSyntax UTCTime where
--   sqlValueSyntax tm =
--     SqliteValueSyntax (myEmitValue (SQ.SQLText (fromString tmStr)))
--     where
--       tmStr =
--         DT.formatTime
--           DT.defaultTimeLocale
--           (DT.iso8601DateFormat (Just "%H:%M:%S%Q"))
--           tm

data FileStateT f = FileStateT
  { _file_state_id :: Columnar f Int -- Auto
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
  data PrimaryKey FileStateT f = FileStateId (Columnar f Int)
                            deriving Generic
  primaryKey = FileStateId . _file_state_id

instance Beamable (PrimaryKey FileStateT)


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
               (SequenceNumber sequence &: Actual &: FileDetailsR Nothing &:
                NonCanonical &:
                ctx)))
  in do afs <- getActualFileState ctx
        case fmap (\fs -> (fs, (nget FileDetailsR fs))) afs
          -- Just bump stat time
              of
          Just (fs, Nothing) ->
            updateFileState (fget ctx) (((fget ctx) :: CheckTime) &: fs)
          -- Make a new active entry
          Just (fs, Just _) -> do
            updateFileState (fget ctx) (Historical &: fs)
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
               (SequenceNumber sequence &: Actual &:
                FileDetailsR (Just (fcast ctx)) &:
                canonical &:
                ctx)))
  in case prevState of
       Just fs ->
         case (nget FileDetailsR fs) == Just (fcast ctx) of
           True -> updateFileState (fget ctx) (((fget ctx) :: CheckTime) &: fs)
           False -> do
             updateFileState (fget ctx) (Historical &: fs)
             insert NonCanonical
       Nothing -> insert Canonical

data BadDBEncodingException = BadDBEncodingException
  { table :: !Text
  , id :: !Int
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

data NewlinesInFilenameException = NewlinesInFilenameException
  { nlFilename :: !Text
  } deriving (Show, Typeable)

instance CE.Exception NewlinesInFilenameException

-- Concrete, non-extensible version of the file state table.
type FileStateF = Record '[FileStateIdF, Location, SequenceNumber, CheckTime, AbsPathText, RelativePathText, Filename, Provenance, Canonical, Actual, FileDetailsR]

-- | Can throw BadDBEncodingException if the assumptions are violated (which shouldn't happen if we're in control of the unFileState ::
unFileState :: FileState -> FileStateF
unFileState fs =
  FileStateIdF (_file_state_id fs) &: --
  Location (_location fs) &: --
  SequenceNumber (_sequence_number fs) &:
  CheckTime (_check_time fs) &:
  AbsPathText (_absolute_path fs) &:
  RelativePathText (_relative_path fs) &:
  Filename (_filename fs) &:
  -- Provenance
  (case ((_provenance_type fs), (_provenance_id fs))
         of
     (0, FileStateId (Just pid)) -> Mirrored pid
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
  (DB.runSelectOne
     conn
     (select
        (do fileState <- all_ (_file_state fileDB)
            guard_ ((_file_state_id fileState) ==. val_ fileStateID)
            pure fileState)))

-- Could probably go via archive and relative path too - these are denormalized.

getActualFileState ::
     (Has SQ.Connection rs, Has AbsPathText rs)
  => Record rs
  -> IO (Maybe FileStateF)
getActualFileState ctx =
  (DB.runSelectOne
     ((fget ctx) :: SQ.Connection)
     (select
        (do fileState <- all_ (_file_state fileDB)
            guard_
              (((_actual fileState) ==. val_ 1) &&.
               ((_absolute_path fileState) ==. val_ (nget AbsPathText ctx)))
            pure fileState))) &
  fmap (fmap unFileState)

-- | search by relative filename in a possibly different root Location.
getActualFileStateRelative ::
     SQ.Connection -> Location -> RelativePathText -> IO (Maybe FileStateF)
getActualFileStateRelative conn (Location loc) rel =
  (DB.runSelectOne
     conn
     (select
        (do fileState <- all_ (_file_state fileDB)
            guard_
              (((_actual fileState) ==. val_ 1) &&.
               ((_location fileState) ==. val_ loc) &&.
               ((_relative_path fileState) ==. val_ (op RelativePathText rel)))
            pure fileState))) &
  fmap (fmap unFileState)

--               (orderBy_
--               (\s -> (desc_ (_check_time s)))
--               (do shaCheck <- all_ (_file_state fileDB)
--                   guard_
--                     ((_sc_file_info_id shaCheck) ==.
--                      val_ (FileInfoId fileInfoID))
--                   return shaCheck))

-- | after, not including, sequenceNumber. Sorted by sequence number.
getChangesSince :: SQ.Connection -> Int -> Location -> IO [FileStateF]
getChangesSince conn sequenceNumber (Location location) =
  (DB.runSelectList
     conn
     (select
        (orderBy_
           (\s -> (asc_ (_sequence_number s)))
           (do fileState <- all_ (_file_state fileDB)
               guard_ ((_location fileState) ==. val_ location)
               guard_ ((_actual fileState) ==. val_ 1)
               guard_ ((_sequence_number fileState) >. val_ sequenceNumber)
               pure fileState)))) &
  fmap (fmap unFileState)

{- | FilePath seems to only treat paths with trailing slashes as "directories" but
     eg `pwd` doesn't give a trailing slash.
-}
ensureTrailingSlash :: FP.FilePath -> FP.FilePath
ensureTrailingSlash fp = fp FP.</> ""

toRelative :: (Has Location r, Has AbsPath r) => Record r -> Maybe FP.FilePath
toRelative ctx =
  let (host, root) = (T.breakOn "/" (nget Location ctx))
  in FP.stripPrefix
       (ensureTrailingSlash (fromString (T.unpack root)))
       (nget AbsPath ctx)

toAbsolute :: (Has Location r, Has RelativePathText r) => Record r -> Text
toAbsolute ctx =
  let (host, root) = (T.breakOn "/" (nget Location ctx))
  in root <> (nget RelativePathText ctx)


-- | Uncontroversial copy, i.e. if the file exists on target, the existing
--  entry's provenance was the same source.  source FileStateF MUST have an id
--  so we can track provenance. Records an intent. Returns True if nothing
--  potentially novel has not been propagated. (This means we can bump the
--  sequence number for detecting novelty)
copyFileState :: SQ.Connection -> FileStateF -> Location -> IO Bool
copyFileState conn source target =
  let sourceId = (nget FileStateIdF source)
  in (case fget source of
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
                   in do (insertFileState
                            conn
                            ((AbsPathText (toAbsolute nextState)) &: nextState))
                         return True)
                 -- copy over if uncontroversial, Possibly mark existing record as historical etc.
                 -- Q: Cleaner if we enforced all values from a DB would DEFINITELY have an id?
                Just prevState ->
                  let targetId = (nget FileStateIdF prevState)
                  in (do sourceIn <- getIngestionFS conn sourceId
                         targetIn <- getIngestionFS conn targetId
                           -- "Noncontroversial" or not. The source ingestion point can update its own files w/o concern.
                         case (((fget sourceIn :: Location) ==
                                (fget targetIn :: Location)))
                             -- Q: properly log a warning message that we're not propagating a change?
                               of
                           False -> do
                             Turtle.echo
                               (Turtle.repr
                                  ("Error: Not propagating, different ingestion location " ++
                                   (show source) ++
                                   ", sourceIn" ++
                                   (show sourceIn) ++
                                   ", targetIn" ++ (show targetIn)))
                             return False
                           True ->
                             let sourceSeq = nget SequenceNumber sourceIn
                                 targetSeq = nget SequenceNumber targetIn
                             in case compare sourceSeq targetSeq of
                                  LT -> do
                                    Turtle.echo
                                      (Turtle.repr
                                         ("Warning: Not propagating, source is an earlier version than target " ++
                                          (show source) ++
                                          ", sourceIn" ++
                                          (show sourceIn) ++
                                          ", targetIn" ++ (show targetIn)))
                                    return True -- Benign. but maybe indicates an upstream pull is needed.
                                  EQ -> do
                                    Turtle.echo
                                      (Turtle.repr ("noop " ++ (show source))) -- REMOVEME
                                    return True -- Benign NOOP, already copied this exact version.
                                  GT -> do
                                    sequence <-
                                      nextSequenceNumber (conn &: target &: Nil)
                                    (let nextState =
                                           (SequenceNumber sequence &:
                                            Mirrored sourceId &:
                                            CheckTime Nothing &:
                                            target &:
                                            source)
                                     in do (updateFileState
                                              conn
                                              (Historical &: prevState))
                                           (insertFileState
                                              conn
                                              ((AbsPathText
                                                  (toAbsolute nextState)) &:
                                               nextState))
                                           (return True)))))


updateFileState ::
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
  => SQ.Connection
  -> Record rs
  -> IO ()
updateFileState conn ctx =
  let fs =
        (FileStateT
         { _file_state_id = nget FileStateIdF ctx
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
               Mirrored i -> FileStateId (Just i)
               Ingested -> FileStateId Nothing
         })
  in (runBeamSqliteDebug
        putStrLn
        conn
        (runUpdate (save (_file_state fileDB) fs)))
{-
  let fs =
        (FileStateT
         { _file_state_id = default_
         , _location = val_ ((nget Location ctx) :: Text)
         , _absolute_path = val_ (nget AbsPathText ctx)
         , _relative_path = val_ (nget RelativePathText ctx)
         , _filename = val_ (nget Filename ctx)
         , _deleted =
             case (nget FileDetailsR ctx) of
               Just _ -> val_ 0
               Nothing -> val_ 1
         , _mod_time = val_ (fmap (nget ModTime) (nget FileDetailsR ctx))
         , _file_size = val_ (fmap (nget FileSize) (nget FileDetailsR ctx))
         , _checksum = val_ (fmap (nget Checksum) (nget FileDetailsR ctx))
         , _encrypted =
             val_
               (fmap
                  (\fd ->
                     (case fget fd of
                        Unencrypted -> 0
                        Encrypted _ -> 1))
                  (nget FileDetailsR ctx))
         , _encryption_key_id =
             (case fmap fget (nget FileDetailsR ctx) of
                Just (Encrypted k) -> val_ (Just k)
                _ -> val_ Nothing)
         , _canonical =
             case fget ctx of
               NonCanonical -> val_ 0
               Canonical -> val_ 1
         , _actual =
             case fget ctx of
               Historical -> val_ 0
               Actual -> val_ 1
         , _provenance_type =
             case fget ctx of
               Mirrored _ -> val_ 0
               Ingested -> val_ 1
         , _provenance_id = undefined
          --       case fget ctx of
          --         Mirrored i -> FileStateId (Just (Just i))
          --         Ingested -> FileStateId Nothing
         })
        -}
{-
dog =
  (FileStateT
   { _file_state_id = undefined
   , _location = val_ ("Hello" :: Text)
   , _sequence_number = undefined
   , _check_time = undefined
   , _absolute_path = undefined
   , _relative_path = undefined
   , _filename = undefined
   , _deleted = undefined
   , _mod_time = undefined
   , _file_size = undefined
   , _checksum = undefined
   , _encrypted = undefined
   , _encryption_key_id = undefined
   , _canonical = undefined
   , _actual = undefined
   , _provenance_type = undefined
   , _provenance_id = undefined
   }) -}

-- | Insert a new file state, (with a new auto-incrementing id.)
--   I can't figure out how to get this to work with Beam.
insertFileState ::
     ( Has CheckTime rs
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
  => SQ.Connection
  -> Record rs
  -> IO ()
insertFileState conn ctx =
  SQ.execute
    conn
    "INSERT INTO file_state (location, sequence_number, check_time, absolute_path, relative_path, filename, deleted, mod_time, file_size, checksum, encrypted, encryption_key_id, actual, canonical, provenance_type, provenance_id__file_state_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    (( nget Location ctx
     , nget SequenceNumber ctx
     , nget CheckTime ctx
     , nget AbsPathText ctx
     , nget RelativePathText ctx
     , nget Filename ctx
     , case (nget FileDetailsR ctx) of
         Just _ -> 0 :: Int
         Nothing -> 1 :: Int
     , fmap (nget ModTime) (nget FileDetailsR ctx)
     , fmap (nget FileSize) (nget FileDetailsR ctx)
     , fmap (nget Checksum) (nget FileDetailsR ctx)) SQ.:.
     ( fmap
         (\fd ->
            (case fget fd of
               Unencrypted -> 0 :: Int
               Encrypted _ -> 1 :: Int))
         (nget FileDetailsR ctx)
     , (case fmap fget (nget FileDetailsR ctx) of
          Just (Encrypted k) -> Just k
          _ -> Nothing)
     , case fget ctx of
         NonCanonical -> 0 :: Int
         Canonical -> 1 :: Int
     , case fget ctx of
         Historical -> 0 :: Int
         Actual -> 1 :: Int
     , case fget ctx of
         Mirrored _ -> 0 :: Int
         Ingested -> 1 :: Int
     , case fget ctx of
         Mirrored i -> Just i
         Ingested -> Nothing))

data FileDB f = FileDB
  { _file_state :: f (TableEntity FileStateT)
  } deriving (Generic)

instance Database be FileDB

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

-- | Follow mirror provenance and return as a list.
-- Throws FSNotFoundException if any of the provenance pointers can't be resolved (this should not happen)
-- FIXME: Will infinitely loop if somehow a loop has created in the db.
provenanceChain :: SQ.Connection -> Int -> IO [FileStateF]
provenanceChain conn fileStateID = do
  fs <- (getFileStateById conn fileStateID)
  (case fs of
     Nothing ->
       (CE.throw
          (FSNotFoundException
             fileStateID
             "Can't find fileState in getIngestionFS"))
     Just fs ->
       (let ufs = (unFileState fs)
        in (case (fget ufs) of
              Ingested -> return [ufs]
              Mirrored id' -> do
                rest <- (provenanceChain conn id')
                return ([ufs] ++ rest))))

-- | Follow mirror provenance to find the original ingestion record
getIngestionFS :: SQ.Connection -> Int -> IO FileStateF
getIngestionFS conn fileStateID = do
  provChain <- provenanceChain conn fileStateID
  return
    (case (List.find
             (\fs ->
                case (fget fs) of
                  Ingested -> True
                  _ -> False)
             provChain) of
       Nothing ->
         (CE.throw
            (FSNotFoundException
               fileStateID
               "Can't find fileState in getIngestionFS"))
       Just fs -> fs)

-- | Follow Mirrored provenance links until we find a canonical source
getFirstCanonicalProvenance :: SQ.Connection -> Int -> IO (Maybe FileStateF)
getFirstCanonicalProvenance conn fileStateID = do
  provChain <- provenanceChain conn fileStateID
  return
    (List.find
       (\fs ->
          case (fget fs) of
            Canonical -> True
            _ -> False)
       provChain)

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
                       0
                       "Too many location entries")
        SQ.execute
          (fget ctx)
          "INSERT OR REPLACE INTO file_state_sequence_counters (file_state_sequence_location, file_state_sequence_counter) VALUES (?,?)"
          ((nget Location ctx), num + 1)
        return (num + 1))

-- | 0 represents no previous squence number, since sequences start at
-- 1. Represents the sequence number of the last successfully mirrored file
-- state.
getLastRequestSourceSequence :: SQ.Connection -> Location -> Location -> IO Int
getLastRequestSourceSequence conn (Location from) (Location to) = do
  Turtle.echo (Turtle.repr ("getlastRequest " ++ (show (from, to))))
  r <-
    SQ.query
      conn
      "SELECT MAX(source_sequence) FROM requests WHERE active = 1 AND source_location = ? AND target_location = ?" -- I think the max is redundant, since I just expect one active result, no?
      (from, to)
  case r of
    [SQ.Only (Just n)] -> return n
    [SQ.Only (Nothing)] -> return 0
    _ ->
      CE.throw
        (ImpossibleDBResultException
           "requests"
           "Returning muliple results for MAX?")

-- | REQUIRES sources to be sorted by SequenceNumber
mirrorChangesFromLocation' :: SQ.Connection -> [FileStateF] -> Location -> Int -> Bool -> IO Int
mirrorChangesFromLocation' conn sources target lastSeq failed =
  case sources of
    [] -> return lastSeq
    f:fs -> do
      res <- copyFileState conn f target
      (mirrorChangesFromLocation'
         conn
         fs
         target
         (case (failed, res) of
            (False, True) -> (nget SequenceNumber f)
            _ -> lastSeq)
         (failed || (not res)))


-- | copyFileState's all "uncontroversial" novelty from the source location to
-- the desination. It will propagate changes beyond the first
-- failure-to-propagate-potential-novelty, but will only bump the sequence
-- number up to just before first failure. This way, re-trying the copy after
-- conflicts are resolved will cause all subsequent changes to be
-- re-attempted. This should be safe because copying is idempotent.
-- Will always add a new "requests" entry, even if nothing is copied.
-- Does not EFFECT the changes, merely marks them as intended outcome in the db
mirrorChangesFromLocation :: SQ.Connection -> Location -> Location -> IO ()
mirrorChangesFromLocation conn source@(Location sourceT) target@(Location targetT) =
  DB.withSavepoint
    conn
    (do now <- DTC.getCurrentTime
        lastSeq <- getLastRequestSourceSequence conn source target
        changes <- getChangesSince conn lastSeq source
        Turtle.echo (Turtle.repr ("changes since " ++ (show source) ++ " , "  ++ (show lastSeq) ++ " : " ++ (show changes)))
        nextSeq <- mirrorChangesFromLocation' conn changes target lastSeq False
        SQ.execute
          conn
          "UPDATE requests SET active = 0 WHERE active = 1 AND source_location = ? AND target_location = ?"
          (sourceT, targetT)
        SQ.execute
          conn
          "INSERT INTO requests (source_location, target_location, source_sequence, check_time, active) VALUES (?,?,?,?,1)"
          (sourceT, targetT, nextSeq, now)
        return ())

data SharedRelativeCp = SharedRelativeCp
  { _rsFile :: RelativePathText
  , _rsFrom :: Location
  , _rsTo :: Location
  } deriving (Show)

data AlteredRelativeCp = AlteredRelativeCp
  { _cpFrom :: AbsPathText
  , _cpTo :: AbsPathText
  } deriving (Show)

data CopyEntry = CopySharedRelative SharedRelativeCp | CopyAlteredRelative AlteredRelativeCp

copyEntryAbsTo :: CopyEntry -> AbsPathText
copyEntryAbsTo ce =
  (case ce of
     CopySharedRelative (SharedRelativeCp {_rsFile, _rsFrom, _rsTo}) ->
       AbsPathText (toAbsolute (_rsFile &: _rsTo &: Nil))
     CopyAlteredRelative (AlteredRelativeCp {_cpFrom, _cpTo}) -> _cpTo)

data CopySort
  = SortSharedRelative Location
  | SortAlteredRelativeCp AbsPathText
  deriving (Eq, Ord)

-- | Has there ever been a file known to be at this location? (i.e. are
-- copies/updates to this filename expected to overwrite an existing file?) If
-- not we know we can have a "preserve existing" flag on rsynce, etc, to help us
-- avoid clobbering data.
fileExpected :: SQ.Connection -> AbsPathText -> IO Bool
fileExpected conn (AbsPathText pathtext) = do
  fss <-
    (DB.runSelectOne
       conn
       (select
          (do fileState <- all_ (_file_state fileDB)
              guard_ ((_absolute_path fileState) ==. val_ pathtext)
              guard_ ((_check_time fileState) /=. val_ Nothing)
              pure fileState)))
  return
    (case fss of
       Just a -> True
       nothing -> False)
             
-- | Returns a list of proposed copy commands. Namely, any expected file in the
-- db that hasn't been sha verified yet. Can propose overwrites as it doesn't
-- check the filesystem - so trusts that the copy command used (eg rsync)
-- doesn't execute overwrites.
proposeCopyCmds :: SQ.Connection -> IO [CopyEntry]
proposeCopyCmds conn = do
  fss <-
    (DB.runSelectList
       conn
       (select
          (do fileState <- all_ (_file_state fileDB)
              guard_ ((_check_time fileState) ==. val_ Nothing)
              guard_ ((_provenance_type fileState) ==. val_ 0)
              guard_ ((_actual fileState) ==. 1)
              pure fileState)))
  cps <-
    (traverse
       (\target ->
          case (_provenance_id target) of
            FileStateId Nothing ->
              CE.throw
                (BadDBEncodingException
                   "file_state"
                   (_file_state_id target)
                   "Should have provenance id defined given our query")
            FileStateId (Just prov_id) -> do
              msource <- (getFirstCanonicalProvenance conn prov_id)
              return
                (fmap
                   (\source ->
                      case ((nget RelativePathText source) ==
                            (nget RelativePathText (unFileState target))) of
                        False ->
                          CopyAlteredRelative
                            (AlteredRelativeCp
                             { _cpFrom = (fget source)
                             , _cpTo = (fget (unFileState target))
                             })
                        True ->
                          CopySharedRelative
                            (SharedRelativeCp
                             { _rsFile = (fget source)
                             , _rsFrom = (fget source)
                             , _rsTo = (fget (unFileState target))
                             }))
                   msource))
       fss)
  return (Maybe.catMaybes cps)
 
--     (Extra.groupSortOn (\cp -> (_cpFrom cp, _cpTo cp)) )

-- Sorts copy commands into groups that could live together in the same rsync command
-- I think I should just do this explicitly and get a pair of SharedRelativeCp's and AlteredRelativeCp's.
rsyncSort :: [(CopyEntry, Bool)] -> [((CopySort, Bool), [CopyEntry])]
rsyncSort entries =
  (Extra.groupSort
     (map
        (\(ce, update) ->
           ( ( (case ce of
                  CopySharedRelative (SharedRelativeCp {_rsFile, _rsFrom, _rsTo}) ->
                    SortSharedRelative _rsTo
                  CopyAlteredRelative (AlteredRelativeCp {_cpFrom, _cpTo}) ->
                    SortAlteredRelativeCp _cpTo)
             , update)
           , ce))
        entries))

matchCopySharedRelative :: CopyEntry -> Maybe SharedRelativeCp
matchCopySharedRelative ce =
  case ce of
    CopySharedRelative sr -> Just sr
    _ -> Nothing

matchCopyAlteredRelative :: CopyEntry -> Maybe AlteredRelativeCp
matchCopyAlteredRelative ce =
  case ce of
    CopyAlteredRelative ar -> Just ar
    _ -> Nothing

groupShared :: [CopyEntry] -> [((Location, Location), [SharedRelativeCp])]
groupShared entries =
  entries & map matchCopySharedRelative & Maybe.catMaybes &
  map (\sr -> ((_rsFrom sr, _rsTo sr), sr)) &
  Extra.groupSort

groupAltered :: [CopyEntry] -> [(AbsPathText, [AlteredRelativeCp])]
groupAltered entries =
  entries & map matchCopyAlteredRelative & Maybe.catMaybes &
  map (\ar -> (_cpTo ar, ar)) &
  Extra.groupSort

type RsyncCommand = (Text, FP.FilePath, [Turtle.Line])

sharedCommand ::
     Text -> ((Location, Location), [SharedRelativeCp]) -> RsyncCommand
sharedCommand flags ((Location locFrom, Location locTo), srs) =
  let filesFrom = srs & map ((op RelativePathText) . _rsFile)
      filesFromFilename = "/tmp/files-from/" <> (asciiHash filesFrom)
      linedFilesFrom =
        filesFrom &
        map
          (\f ->
             case (Turtle.textToLine f) of
               Just l -> l
               Nothing -> CE.throw (NewlinesInFilenameException f))
  in ( ("rsync -arv " <> flags <> " --files-from " <> filesFromFilename <> " " <>
        locFrom <>
        locTo)
     , Turtle.fromText filesFromFilename
     , linedFilesFrom)

alteredCommand :: Text -> (AbsPathText, [AlteredRelativeCp]) -> RsyncCommand
alteredCommand flags ((AbsPathText absTo), srs) =
  let filesFrom = srs & map ((op AbsPathText) . _cpFrom)
      filesFromFilename = "/tmp/files-from/" <> (asciiHash filesFrom)
      linedFilesFrom =
        filesFrom &
        map
          (\f ->
             case (Turtle.textToLine f) of
               Just l -> l
               Nothing -> CE.throw (NewlinesInFilenameException f))
  in ( ("rsync -arv " <> flags <> " --files-from " <> filesFromFilename <> " " <>
        "/" <>
        " " <>
        absTo)
     , Turtle.fromText filesFromFilename
     , linedFilesFrom)

-- | returns the rsync command, filename for the includes form and the includes from itself
rsyncCommands :: [(CopyEntry, Bool)] -> [RsyncCommand]
rsyncCommands entries =
  let newShared = entries & (filter (not . snd)) & map fst & groupShared
      updateShared = entries & (filter snd) & map fst & groupShared
      newAltered = entries & (filter (not . snd)) & map fst & groupAltered
      updateAltered = entries & (filter snd) & map fst & groupAltered
  in (newShared & map (sharedCommand "--ignore-existing")) ++
     (updateShared & map (sharedCommand "")) ++
     (newAltered & map (alteredCommand "--ignore-existing")) ++
     (updateAltered & map (alteredCommand ""))

proposeCopyCmdsText :: SQ.Connection -> IO [RsyncCommand]
proposeCopyCmdsText conn = do
  cmds <- proposeCopyCmds conn
  entries <-
    cmds &
    traverse
      (\ce -> do
         exp <- fileExpected conn (copyEntryAbsTo ce)
         return (ce, exp))
  return (rsyncCommands entries)

-- | Writes the files-from files to disk
writeFilesFrom :: [RsyncCommand] -> IO [()]
writeFilesFrom rsCommands =
  traverse
    (\(_, fileName, contents) -> Turtle.output fileName (Turtle.select contents))
    rsCommands

-- | echo the rsync commands
echoRsyncCmds :: [RsyncCommand] -> IO [()]
echoRsyncCmds rsCommands =
  traverse (\(command, _, _) -> Turtle.echo (Turtle.repr command)) rsCommands

-- asciiHash :: Hashable.Hashable a => a -> BSChar8.ByteString
asciiHash obj =
  (BSChar8.filter
     (\c -> c /= '=')
     (BSChar8.map
        (\c ->
           case c of
             '+' -> '-'
             '/' -> '_'
             c -> c)
        (Base64.encode (BSLazy.toStrict (Binary.encode (Hashable.hash obj)))))) &
  Encoding.decodeLatin1

-- | Records successful requests
createRequestTable :: SQ.Query
createRequestTable =
  "CREATE TABLE IF NOT EXISTS requests " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "request_id INTEGER PRIMARY KEY"
           , "source_location TEXT"
           , "source_sequence INTEGER"
           , "target_location TEXT"
           , "check_time TEXT"
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

