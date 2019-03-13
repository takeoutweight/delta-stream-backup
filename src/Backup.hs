{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- {-# LANGUAGE ConstraintKinds #-}
-- {-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}

-- :set -XOverloadedStrings

module Backup where

import Control.Monad
import qualified Data.Time.Clock as DTC
import Database.Beam
import qualified Database.SQLite.Simple as SQ
import Prelude hiding (FilePath, head)
import Turtle hiding (select)

import Fields
import Query
import Logic
import MirrorChanges
import CopyCommands
import IngestPath

-- eg
-- view $ (filterRegularFiles (lstree "src")) >>= shasum
filterRegularFiles :: Shell FilePath -> Shell FilePath
filterRegularFiles fns = do
  fn <- fns
  s <- lstat fn
  if (isRegularFile s)
    then (return fn)
    else empty

-- | returns the time of check as well.
regularStats :: Shell FilePath -> Shell (DTC.UTCTime, FilePath, FileStatus)
regularStats fns = do
  fn <- fns
  now <- date
  s <- lstat fn
  if (isRegularFile s)
    then (return (now, fn, s))
    else empty

defaultDBFile = "/Users/nathan/src/haskell/backup/resources/archive.sqlite"

defaultCtx =
  (  DBPath defaultDBFile
  &: Location "Nathans-MacBook-Pro-2.local/Users/nathan/Pictures"
  &: Rechecksum defaultRechecksum
  &: Nil
  )

-- | uses the first filename as the filename of the target.
cpToDir :: MonadIO io => FilePath -> FilePath -> io ()
cpToDir from toDir = cp from (toDir </> (filename from))

-- do { rm (fromString (nget DBPath defaultCtx)) ; createDB (nget DBPath defaultCtx) }
-- createDB (nget DBPath defaultCtx)
-- ingestPath defaultCtx "/Users/nathan/Pictures/2013/2013-05-15/"
-- SQ.withConnection (nget DBPath defaultCtx) (\conn -> mirrorChangesFromLocation conn (Location "/Users/nathan/Pictures") (Location "/Users/nathan/SOMEWHEREELSE/Pictures"))
-- Just fs <- SQ.withConnection (nget DBPath defaultCtx) (\conn -> getFileStateById conn 1)

sampleRun = do
  rm (fromString (nget DBPath defaultCtx))
  createDB (nget DBPath defaultCtx)
  ingestPath defaultCtx "/Users/nathan/Pictures/2013/2013-05-15/"
  SQ.withConnection
    (nget DBPath defaultCtx)
    (\conn ->
       mirrorChangesFromLocation
         conn
         (Location "Nathans-MacBook-Pro-2.local/Users/nathan/Pictures")
         (Location
            "Nathans-MacBook-Pro-2.local/Users/nathan/SOMEWHEREELSE/Pictures"))
  rscs <-
    SQ.withConnection
      (nget DBPath defaultCtx)
      (\conn -> proposeCopyCmdsText conn)
  writeFilesFrom rscs
  echoRsyncCmds rscs
