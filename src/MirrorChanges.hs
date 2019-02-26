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
