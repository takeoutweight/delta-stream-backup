-- Module for presenting queries in the higher level (non-raw SQL) encodigngs
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NamedFieldPuns#-}
-- {-# LANGUAGE PartialTypeSignatures #-}
-- {-# LANGUAGE NamedWildCards #-}
-- {-# LANGUAGE TypeSynonymInstances #-}
-- {-# LANGUAGE TypeApplications #-}

module Query where

import qualified Control.Exception as CE
import Control.Lens ((&), op)
import qualified Data.Binary as Binary
import qualified Data.ByteString.Base64 as Base64
import qualified Data.ByteString.Char8 as BSChar8
import qualified Data.ByteString.Lazy as BSLazy
import qualified Data.List as List
import qualified Data.List.Extra as Extra
import qualified Data.Hashable as Hashable
import qualified Data.Maybe as Maybe
import Data.Monoid ((<>))
import Data.String (fromString)
import qualified Data.Time.Clock as DTC
import qualified Data.Text as T
import qualified Data.Text.Encoding as Encoding
import Data.Text (Text)
import Data.Typeable (Typeable)
import Database.Beam.Sqlite (runBeamSqliteDebug)
import Database.Beam.Sqlite.Syntax
import Database.Beam
       ((&&.), (/=.), (==.), (>.), all_, asc_, desc_, guard_, orderBy_,
        runUpdate, save, select, val_)
import qualified Database.Beam as Beam
import qualified Database.SQLite.Simple as SQ
import qualified Filesystem.Path.CurrentOS as FP
import Prelude hiding (FilePath, head)
import qualified Turtle as Turtle

import qualified DBHelpers as DB
import Schema
import FileState
import Fields

-- Concrete, non-extensible version of the file state table.
type FileStateF = Record '[FileStateIdF, Location, SequenceNumber, CheckTime, AbsPathText, RelativePathText, Filename, Provenance, Canonical, Actual, FileDetailsR]

data BadDBEncodingException = BadDBEncodingException
  { table :: !Text
  , id :: !Int
  , errMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception BadDBEncodingException

data ImpossibleDBResultException = ImpossibleDBResultException
  { idrRable :: !Text
  , idrErrMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception ImpossibleDBResultException

data FSNotFoundException = FSNotFoundException
  { fnfid :: !Int
  , fnfErrMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception FSNotFoundException

data BadFileStateException = BadFileStateException
  { fs :: !FileStateF
  , bfErrMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception BadFileStateException

data NewlinesInFilenameException = NewlinesInFilenameException
  { nlFilename :: !Text
  } deriving (Show, Typeable)

instance CE.Exception NewlinesInFilenameException

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

-- Just experimenting w/ pulling out query fragments.
allFileStates ::
     Beam.Q SqliteSelectSyntax FileDB s (FileStateT (Beam.QExpr SqliteExpressionSyntax s))
allFileStates = all_ (_file_state fileDB)

-- But the type for these are horrendous.
hasCheckTime fileState = guard_ ((_check_time fileState) /=. val_ Nothing)

noCheckTime fileState = guard_ ((_check_time fileState) ==. val_ Nothing)

absPathIs pathtext fileState = guard_ ((_absolute_path fileState) ==. val_ pathtext)

isMirrored fileState = guard_ ((_provenance_type fileState) ==. val_ 0)

isActual fileState = guard_ ((_actual fileState) ==. 1)

qGuard :: Monad m => m a -> (a -> m b) -> m a
qGuard mx mf = do
  x <- mx
  mf x
  return x

-- This works but I don't know how to get rid of the type wildcard. Doesn't
-- really make anyone's type better, just removes the need for that redundant
-- "select":

-- selectFileStateList ::
--      res ~ (FileStateT (QExpr SqliteExpressionSyntax s))
--   => SQ.Connection
--   -> Q SqliteSelectSyntax FileDB _ res
--   -> IO [FileState]
-- selectFileStateList conn query = (DB.runSelectList conn (select query))

-- Accepting only guards doesn't help either afaict.
-- Found type wildcard ‘_s’ standing for ‘Database.Beam.Query.QueryInaccessible’

-- selectFileStateList ::
--      SQ.Connection
--   -> ((FileStateT (QExpr SqliteExpressionSyntax _s)) -> (Q SqliteSelectSyntax FileDB _s (FileStateT (QExpr SqliteExpressionSyntax _s))))
--   -> IO [FileStateT Identity]
-- selectFileStateList conn guard =
--   (DB.runSelectList conn (select (allFileStates `qGuard` guard)))

-- | Has there ever been a file known to be at this location? (i.e. are
-- copies/updates to this filename expected to overwrite an existing file?) If
-- not we know we can have a "preserve existing" flag on rsynce, etc, to help us
-- avoid clobbering data.
fileExpected :: SQ.Connection -> AbsPathText -> IO Bool
fileExpected conn (AbsPathText pathtext) = do
  fss <-
    (DB.runSelectOne
       conn
       (select (allFileStates `qGuard` absPathIs pathtext `qGuard` hasCheckTime)))
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
          (allFileStates `qGuard` noCheckTime `qGuard` isMirrored `qGuard`
           isActual)))
  cps <-
    (traverse
       (\fs ->
          let target = (unFileState fs)
          in case (fget target :: Provenance) of
               Ingested ->
                 CE.throw
                   (BadDBEncodingException
                      "file_state"
                      (nget FileStateIdF target)
                      "Should have provenance id defined given our query")
               Mirrored prov_id -> do
                 msource <- (getFirstCanonicalProvenance conn prov_id)
                 return
                   (fmap
                      (\source ->
                         case ((nget RelativePathText source) ==
                               (nget RelativePathText target)) of
                           False ->
                             CopyAlteredRelative
                               (AlteredRelativeCp
                                { _cpFrom = (fget source)
                                , _cpTo = (fget target)
                                })
                           True ->
                             CopySharedRelative
                               (SharedRelativeCp
                                { _rsFile = (fget source)
                                , _rsFrom = (fget source)
                                , _rsTo = (fget target)
                                }))
                      msource))
       fss)
  return (Maybe.catMaybes cps)


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
        " " <>
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

createDB filename =
  SQ.withConnection
    filename
    (\conn ->
       DB.withSavepoint
         conn
         (do (SQ.execute_ conn createFileStateSequenceCounterTable)
             (SQ.execute_ conn createFileStateTable)
             (SQ.execute_ conn createRequestTable)))

