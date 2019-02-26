-- Logic that does not require raw SQL abstractions for definition that hasn't
-- ended up somewhere more specific. Could also be thought of like "Query (the
-- safely implemented parts)"
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

module Logic where

import qualified Control.Exception as CE
import Control.Lens ((&), op)
import Data.Monoid ((<>))
import Data.String (fromString)
import qualified Data.Text as T
import Data.Text (Text)
import Database.Beam (select)
import qualified Database.SQLite.Simple as SQ
import qualified Filesystem.Path.CurrentOS as FP
import Prelude hiding (FilePath, head)

import qualified DBHelpers as DB
import Fields
import Query

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

getFileStateById :: SQ.Connection -> Int -> IO (Maybe FileStateF)
getFileStateById conn fileStateID =
  (DB.runSelectOne conn (select (allFileStates `qGuard` hasID fileStateID))) &
  fmap (fmap unFileState)

getActualFileState ::
     (Has SQ.Connection rs, Has AbsPathText rs)
  => Record rs
  -> IO (Maybe FileStateF)
getActualFileState ctx =
  (DB.runSelectOne
     ((fget ctx) :: SQ.Connection)
     (select
        (allFileStates `qGuard` isActual `qGuard`
         absPathIs (nget AbsPathText ctx)))) &
  fmap (fmap unFileState)

-- Could probably go via archive and relative path too - these are denormalized.

-- | search by relative filename in a possibly different root Location.
getActualFileStateRelative ::
     SQ.Connection -> Location -> RelativePathText -> IO (Maybe FileStateF)
getActualFileStateRelative conn (Location loc) rel =
  (DB.runSelectOne
     conn
     (select
        (allFileStates `qGuard` isActual `qGuard` locationIs loc `qGuard`
         relativePathIs (op RelativePathText rel)))) &
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
       (case (fget fs) of
          Ingested -> return [fs]
          Mirrored id' -> do
            rest <- (provenanceChain conn id')
            return ([fs] ++ rest)))
