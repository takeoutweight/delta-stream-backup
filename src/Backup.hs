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
import Schema

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

{- | FilePath seems to only treat paths with trailing slashes as "directories" but
     eg `pwd` doesn't give a trailing slash.
-}
ensureTrailingSlash :: FilePath -> FilePath
ensureTrailingSlash fp = fp FP.</> ""

-- | Never re-check
defaultRechecksum :: UTCTime -> UTCTime -> Bool
defaultRechecksum now prev = False

{- | This is attempting the nested approach to extensible record-esque things
-}
checkFile3 :: (MonadIO io, Has SQ.Connection r) => Record r -> io (Record (Int : r))-- (Maybe ShaCheck)
checkFile3 r = do
  liftIO (SQ.close (fget r))
  return (r & fcons 3)

-- FIXME: Feel like this should work? Maybe `get` only works on concrete Records?
-- :t (fmap (\r -> ((get r) :: Int)) (checkFile3 undefined))
-- this is OK (fmap get ([(3 &: Nil)] :: [Record '[Int]])) :: [Int]
-- but this works: :t (\r -> ((get r) :: Int)) . (\r -> r & fcons (3 :: Int)) So maybe just something with the monad?

-- TODO supercede previous state
-- TODO Re-order and possibly synomize all these constraints
-- | if we're due for a recheck, calculate the sha and add an entry to the db.
doCheck ::
     ( Has AbsPath rs
     , Has Filename rs
     , Has StatTime rs
     , Has Rechecksum rs
     , Has FileInfoIdText rs
     , Has SQ.Connection rs
     , Has Remote rs
     , Has RelativePathText rs
     , Has AbsPathText rs
     , Has MasterRemote rs
     )
  => Record rs
  -> FileState
  -> FileStatus
  -> IO ()
doCheck ctx prevState stat = do
  let conn :: SQ.Connection = (fget ctx)
  (when
     ((nget Rechecksum ctx)
        (nget StatTime ctx)
        (nget StatTime (unFileState prevState)) -- TODO shoul check modtime too, which would force a re-check
      )
     (do (size, checksum) <- inSizeAndSha (nget AbsPath ctx)
         let checksumText = (T.pack (show checksum))
         let modTime = POSIX.posixSecondsToUTCTime (modificationTime stat)
         let ctx2 =
               (ModTime modTime &: --
                FileSize size &:
                Checksum checksumText &:
                Unencrypted &:
                ctx)
         let ufs = (unFileState prevState)
         case (nget FileDetails ufs) of
           Nothing -> (insertFileState conn (mkFileState ctx2))
           Just fds ->
             case ((nget Checksum fds) == checksumText) of
               True ->
                 (updateFileState
                    conn
                    (mkFileState
                       (FileInfoIdText "TODO" &: --
                        ((fget ctx) :: StatTime) &:
                        (fappend fds ufs))))
               False -> (insertFileState conn (mkFileState ctx2)) -- TODO Okay if we're the authority
      ))

foundNewFile ::
     ( Has AbsPath r
     , Has Filename r
     , Has RelativePathText r
     , Has Remote r
     , Has Archive r
     , Has StatTime r
     , Has FileInfoIdText r
     , Has SQ.Connection r
     , Has Rechecksum r
     , Has AbsPathText r
     , Has MasterRemote r
     )
  => Record r
  -> FileStatus
  -> IO ()
foundNewFile ctx stat =  do
  echo (repr ("Adding new file " ++ show (nget AbsPath ctx)))
  (size, checksum) <- inSizeAndSha (nget AbsPath ctx)
  let checksumText = (T.pack (show checksum))
  let modTime = POSIX.posixSecondsToUTCTime (modificationTime stat)
  (insertFileState
     ((fget ctx) :: SQ.Connection)
     (mkFileState
        (ModTime modTime &: --
         FileSize size &:
         Checksum checksumText &:
         Unencrypted &:
         ctx)))
  
-- TODO Supercede previous state
foundGoneFile ::
     ( Has SQ.Connection r
     , Has StatTime r
     , Has Remote r
     , Has Filename r
     , Has RelativePathText r
     , Has AbsPathText r
     , Has FileInfoIdText r
     , Has MasterRemote r
     )
  => Record r
  -> FileState
  -> IO ()
foundGoneFile ctx prevState = do
  echo "gone"
  case (nget FileDetails (unFileState prevState)) of
    Nothing ->
      (insertFileState ((fget ctx) :: SQ.Connection) (mkGoneFileState ctx))
    Just x -> undefined -- TODO OK if we're the master, bad if we're a mirror

{- | Given an absolute path, check it - creating the required logical entry if
     needed. This is for ingesting new files.
-}
checkFile ::
     ( Has SQ.Connection r
     , Has Archive r
     , Has Remote r
     , Has MasterRemote r
     , Has Root r
     , Has AbsPath r
     , Has Rechecksum r
     )
  => Record r
  -> IO ()
checkFile ctx =
  let conn :: SQ.Connection = (fget ctx)
      MasterRemote masterRemote = (fget ctx)
      Root root = (fget ctx)
      AbsPath absPath = (fget ctx)
  in case stripPrefix (ensureTrailingSlash root) absPath of
       Nothing ->
         err
           (repr
              ("Can't determine relative Path: " ++
               show root ++ "," ++ show absPath))
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
                            StatTime statTime &:
                            FileInfoIdText "TODO" &: -- Is this provencance? Probably should name it something else
                            ctx)
                     fileStatus :: Either () FileStatus <-
                       (Catch.tryJust
                          (guard . Error.isDoesNotExistError)
                          (lstat absPath))
                     mFileState :: Maybe FileState <- (getFileState conn ctx)
                     (case (mFileState, fileStatus) of
                        (Nothing, Left _) -> return () -- didn't find the file but didn't have a record of it either. We could store a "deleted" record but why bother.
                        (Nothing, Right stat)
                          | isRegularFile stat -> foundNewFile ctx2 stat
                        (Just fileState, Left _) -> foundGoneFile ctx2 fileState
                        (Just fileState, Right stat)
                          | isRegularFile stat -> doCheck ctx2 fileState stat
                        _ ->
                          err
                            (repr
                               ("Can't process file (is it a regular file?)" ++
                                show absPath))))) &
              fmap (const ())
            a ->
              err
                (repr
                   ("Can't textify path: " ++
                    show root ++ ", " ++ show absPath ++ " : " ++ show a)))

-- | Walks dirpath recursively
ingestPath ctx dirpath =
  let checks conn = do
        fp <- (lstree dirpath)
        liftIO (checkFile (AbsPath fp &: conn &: ctx))
  in SQ.withConnection (nget DBPath ctx) (\conn -> (sh (checks conn)))

defaultCtx =
  (  DBPath defaultDBFile
  &: Archive "archie"
  &: Remote "Nates-MBP-2014"
  &: MasterRemote True
  &: Root "/Users/nathan/"
  &: Rechecksum defaultRechecksum
  &: Nil
  )

-- ingestPath defaultCtx "/Users/nathan/Pictures/2013/2013-05-15/"

-- | uses the first filename as the filename of the target.
cpToDir :: MonadIO io => FilePath -> FilePath -> io ()
cpToDir from toDir = cp from (toDir </> (filename from))

defaultDBFile = "/Users/nathan/src/haskell/backup/resources/archive.sqlite"

createDB filename =
  SQ.withConnection
    filename
    (\conn ->
       (Catch.onException
          (do (SQ.execute_ conn "SAVEPOINT createDB")
--               (SQ.execute_ conn createShaCheckTable)
--               (SQ.execute_ conn createFileGoneCheckTable)
--               (SQ.execute_ conn createFileInfoTable)
              (SQ.execute_ conn "RELEASE createDB"))
          (do (SQ.execute_ conn "ROLLBACK TO createDB")
              (SQ.execute_ conn "RELEASE createDB"))))
