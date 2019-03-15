-- Records the intent to mirror files between locations in the db
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

module MirrorChanges where

import qualified Control.Exception as CE
import Control.Lens ((&))
import qualified Data.List as List
import qualified Data.Time.Clock as DTC
import Database.Beam (select)
import qualified Database.SQLite.Simple as SQ
import Prelude hiding (FilePath, head)
import qualified Turtle as Turtle

import qualified DBHelpers as DB
import Fields
import Query
import Logic

------------ up to here


-- | If the statef is ingested, it's its own head.
getMirroredHead :: FileStateF -> MirroredSource
getMirroredHead fs = case (fget fs) of
  Mirrored {_mirroredHead} -> _mirroredHead
  Ingested -> MirroredSource (fget fs) (fget fs)

-- | Uncontroversial copy, i.e. if the file exists on target, the existing
--  entry's provenance was the same source.  source FileStateF MUST have an id
--  so we can track provenance. Records an intent. Returns True if nothing
--  potentially novel has not been propagated. (This means we can bump the
--  sequence number for detecting novelty)
copyFileState :: SQ.Connection -> FileStateF -> Location -> IO Bool
copyFileState conn source target =
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
                      (SequenceNumber sequence &:
                       Mirrored
                       { _mirroredHead = getMirroredHead source
                       , _mirroredPrev =
                           MirroredSource (fget source) (fget source)
                       } &:
                       CheckTime Nothing &:
                       target &:
                       source)
                in do (insertFileState
                         conn
                         ((AbsPathText (toAbsolute nextState)) &: nextState))
                      return True)
                 -- copy over if uncontroversial, Possibly mark existing record as historical etc.
                 -- Q: Cleaner if we enforced all values from a DB would DEFINITELY have an id?
             Just oldState ->
               let (MirroredSource sourceLoc sourceSeq) =
                     (getMirroredHead source)
                   (MirroredSource oldLoc oldSeq) = (getMirroredHead oldState)
               in (case (sourceLoc == oldLoc)
                                  -- Q: properly log a warning message that we're not propagating a change?
                         of
                     False -> do
                       Turtle.echo
                         (Turtle.repr
                            ("Error: Not propagating, different ingestion location " ++
                             (show source) ++ ", old " ++ (show oldState)))
                       return False
                     True ->
                       case compare sourceSeq oldSeq of
                         LT -> do
                           Turtle.echo
                             (Turtle.repr
                                ("Warning: Not propagating, source is an earlier version than target " ++
                                 (show source) ++ ", old " ++ (show oldState)))
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
                                   Mirrored
                                   { _mirroredHead = getMirroredHead source
                                   , _mirroredPrev =
                                       MirroredSource
                                         (fget source)
                                         (fget source)
                                   } &:
                                   CheckTime Nothing &:
                                   target &:
                                   source)
                            in do (updateFileState
                                     conn
                                     (Historical &: oldState))
                                  (insertFileState
                                     conn
                                     ((AbsPathText (toAbsolute nextState)) &:
                                      nextState))
                                  (return True)))))

-- | after, not including, sequenceNumber. Sorted by sequence number.
getChangesSince :: SQ.Connection -> Int -> Location -> IO [FileStateF]
getChangesSince conn sequenceNumber (Location location) =
  (DB.runSelectList
     conn
     (select
        (orderByAscendingSequence
           (allFileStates `qGuard` locationIs location `qGuard` isActual `qGuard`
            sequenceGreaterThan sequenceNumber)))) &
  fmap (fmap unFileState)

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
