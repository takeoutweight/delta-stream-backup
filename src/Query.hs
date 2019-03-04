-- Module for presenting queries in the higher level (non-raw SQL) encodings,
-- whose definitions require raw DB terminology.
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
import Data.Text (Text)
import Data.Typeable (Typeable)
import Database.Beam.Sqlite (runBeamSqliteDebug)
import Database.Beam.Sqlite.Syntax
import Database.Beam
       ((/=.), (==.), (>.), all_, asc_, guard_, orderBy_, runUpdate, save,
        val_)
import qualified Database.Beam as Beam
import qualified Database.SQLite.Simple as SQ
import Prelude hiding (FilePath, head)
import qualified Turtle as Turtle

import qualified DBHelpers as DB
import Schema
import FileState
import Fields

-- Concrete, non-extensible version of the file state table.
type FileStateF
   = Record '[ FileStateIdF
             , Location
             , SequenceNumber
             , CheckTime
             , AbsPathText
             , RelativePathText
             , Filename
             , Provenance
             , Canonical
             , Actual
             , FileDetailsR]

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

createDB filename =
  SQ.withConnection
    filename
    (\conn ->
       DB.withSavepoint
         conn
         (do (SQ.execute_ conn createFileStateSequenceCounterTable)
             (SQ.execute_ conn createFileStateTable)
             (SQ.execute_ conn createRequestTable)))

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

-- Just experimenting w/ pulling out query fragments.
allFileStates ::
     Beam.Q SqliteSelectSyntax FileDB s (FileStateT (Beam.QExpr SqliteExpressionSyntax s))
allFileStates = all_ (_file_state fileDB)

hasID fileStateID fileState = guard_ ((_file_state_id fileState) ==. val_ fileStateID)

-- But the type for these are horrendous.
hasCheckTime fileState = guard_ ((_check_time fileState) /=. val_ Nothing)

noCheckTime fileState = guard_ ((_check_time fileState) ==. val_ Nothing)

absPathIs pathtext fileState = guard_ ((_absolute_path fileState) ==. val_ pathtext)

relativePathIs pathtext fileState = guard_ ((_relative_path fileState) ==. val_ pathtext)

locationIs location fileState = guard_ ((_location fileState) ==. val_ location)

isMirrored fileState = guard_ ((_provenance_type fileState) ==. val_ 0)

isActual fileState = guard_ ((_actual fileState) ==. 1)

sequenceGreaterThan sequenceNumber fileState = guard_ ((_sequence_number fileState) >. val_ sequenceNumber)

orderByAscendingSequence query =
  (orderBy_ (\s -> (asc_ (_sequence_number s))) query)

qGuard :: Monad m => m a -> (a -> m b) -> m a
qGuard mx mf = do
  x <- mx
  mf x
  return x

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

