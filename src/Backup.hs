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

import qualified Control.Exception as CE
import qualified Control.Foldl as F
import Control.Monad
import qualified Control.Monad.Catch as Catch
import qualified Control.Monad.Managed as MM
import qualified Crypto.Hash as CH
import qualified Crypto.Hash.Algorithms as CHA
import qualified DBHelpers as DB
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB
import qualified Data.DList as DL
import qualified Data.HashMap.Strict as HashMap
import Data.HashMap.Strict (HashMap)
import qualified Data.String.Combinators as SC
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Time as DT
import Data.Time.Clock (UTCTime)
import qualified Data.Time.Clock as DTC
import qualified Data.Time.Clock.POSIX as POSIX
import Data.Typeable (Typeable)
import Database.Beam
import Database.Beam.Backend.SQL.SQL92
import Database.Beam.Sqlite
import qualified Database.Beam.Sqlite.Syntax as BSS
import qualified Database.SQLite.Simple as SQ
import qualified Database.SQLite.Simple.FromRow as SQS
import qualified Database.SQLite.Simple.ToField as SQTF
import qualified Filesystem.Path.CurrentOS as FP
import Prelude hiding (FilePath, head)
import qualified System.IO.Error as Error
import qualified System.Random as Random
import Turtle hiding (select)
import qualified Turtle.Bytes as TB
import Control.Lens hiding ((:>), Fold, cons)
import qualified Data.Vinyl as V
import Data.Vinyl.Lens (RElem)
import Data.Vinyl.TypeLevel (RIndex)

import Fields
import Query

shasum :: Fold BS.ByteString (CH.Digest CHA.SHA1)
shasum =
  (F.Fold
     (CH.hashUpdate :: CH.Context CHA.SHA1 -> BS.ByteString -> CH.Context CHA.SHA1)
     (CH.hashInit :: CH.Context CHA.SHA1)
     CH.hashFinalize)

-- | CAUTION will throw exception "openFile: inappropriate type (is a directory)" on directories
inshasum :: MonadIO io => FilePath -> io (CH.Digest CHA.SHA1)
inshasum fp =
  fold
    (TB.input fp)
    (F.Fold CH.hashUpdate (CH.hashInit :: CH.Context CHA.SHA1) CH.hashFinalize)

inSizeAndSha :: MonadIO io => FilePath -> io (Int, (CH.Digest CHA.SHA1))
inSizeAndSha fp =
  fold
    (TB.input fp)
    ((,) <$> F.premap BS.length F.sum <*>
     (F.Fold CH.hashUpdate (CH.hashInit :: CH.Context CHA.SHA1) CH.hashFinalize))

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

-- TODO build recursive tree Shell (FilePath, shasum pairs)
-- lstree
-- we get a failure here as it symbolically links to a nonexistent file (if we want to follow symlinks for stat)
-- a <- tryJust (guard . IOE.isDoesNotExistError) (stat "src/.#Backup.hs")
-- fmap isRegularFile a
-- in binary (note that select here will interpret literal strings as binary, so no newline)
-- TB.output "/tmp/gpg-tests/encrypt-binary" testBinary
-- fold testBinary shasum
encrypt :: Shell BS.ByteString -> Shell BS.ByteString
encrypt bs = (TB.inproc "gpg" ["-r", "Nathan Sorenson (SFU)", "--encrypt"] bs)

-- | A FoldM that appends ByteStrings to a file handle.
appendFold :: MonadIO io => Handle -> FoldM io BS.ByteString ()
appendFold handle =
  F.FoldM
    (\_ bs -> do
       liftIO (BS.hPut handle bs)
       return ())
    (return ())
    (\_ -> return ())

-- outputWithChecksum "/tmp/gpg-tests/out-with-check.gpg" (encrypt (TB.input "/tmp/gpg-tests/plain"))
-- | Write the Shell ByteString to a file and return the checksum, in a single pass.
outputWithChecksum ::
     MonadIO io => FilePath -> Shell BS.ByteString -> io (CH.Digest CHA.SHA1)
outputWithChecksum fp bs =
  liftIO
    (MM.with
       (writeonly fp)
       (\h -> foldIO bs ((appendFold h) *> F.generalize shasum)))

-- | Never re-check
defaultRechecksum :: Maybe UTCTime -> Maybe UTCTime -> Bool
defaultRechecksum now prev = False

maybeCheck ::
     ( Has AbsPath rs
     , Has Filename rs
     , Has CheckTime rs
     , Has Rechecksum rs
     , Has SQ.Connection rs
     , Has Location rs
     , Has RelativePathText rs
     , Has AbsPathText rs
     , Has Provenance rs
     )
  => Record rs
  -> FileStatus
  -> IO ()
maybeCheck ctx stat = do
  let modTime = POSIX.posixSecondsToUTCTime (modificationTime stat)
  mPrevState <- (getActualFileState ctx)
  let check = do
        (size, checksum) <- inSizeAndSha (nget AbsPath ctx)
        let checksumText = (T.pack (show checksum))
        let ctx2 =
              (ModTime modTime &: --
               FileSize size &:
               Checksum checksumText &:
               Unencrypted &:
               ctx)
        insertDetailedFileState ctx2 mPrevState
  case mPrevState of
    Nothing -> check
    Just prevState ->
      (when
         (((nget Rechecksum ctx) (nget CheckTime ctx) (nget CheckTime prevState)) ||
          (Just modTime /= (fmap (nget ModTime) (nget FileDetailsR prevState))))
         check)

{- | Given an absolute path, check it - creating the required logical entry if
     needed. This is for ingesting new files.
-}
checkFile ::
     ( Has SQ.Connection r
     , Has Location r
     , Has AbsPath r
     , Has Rechecksum r
     , Has Provenance r
     )
  => Record r
  -> IO ()
checkFile ctx =
  let conn :: SQ.Connection = (fget ctx)
      Location loc = (fget ctx)
      AbsPath absPath = (fget ctx)
  in case toRelative ctx of
       Nothing ->
         err
           (repr
              ("Can't determine relative Path: " ++
               show loc ++ "," ++ show absPath))
       Just relative ->
         (case ( FP.toText relative
               , FP.toText absPath
               , FP.toText (filename absPath)) of
            (Right relText, Right pathText, Right nameText) ->
              (DB.withSavepoint
                 conn
                 (do statTime <- date
                     let ctx2 =
                           (RelativePathText relText &: --
                            AbsPathText pathText &:
                            Filename nameText &:
                            CheckTime (Just statTime) &:
                            ctx)
                     fileStatus :: Either () FileStatus <-
                       (Catch.tryJust
                          (guard . Error.isDoesNotExistError)
                          (lstat absPath))
                     (case fileStatus of
                        Left _ -> insertGoneFileState ctx2
                        Right stat
                          | isRegularFile stat -> maybeCheck ctx2 stat
                        _ ->
                          err
                            (repr
                               ("Can't process file (is it a regular file?)" ++
                                show absPath)))))
            a ->
              err
                (repr
                   ("Can't textify path: " ++
                    show loc ++ ", " ++ show absPath ++ " : " ++ show a)))

-- | Walks dirpath recursively
ingestPath ctx dirpath =
  let checks conn = do
        fp <- (lstree dirpath)
        liftIO (checkFile (AbsPath fp &: conn &: Ingested &: ctx))
  in SQ.withConnection (nget DBPath ctx) (\conn -> (sh (checks conn)))

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
