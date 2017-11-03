{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE DeriveDataTypeable #-}

module Backup where

import qualified Control.Exception as CE
import qualified Control.Foldl as F
import Control.Monad
import qualified Control.Monad.Managed as MM
import qualified Control.Monad.Trans.Class as MT
import qualified Crypto.Hash as CH
import qualified Crypto.Hash.Algorithms as CHA
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HashMap
import Data.HashMap.Strict (HashMap)
import qualified Data.List as List
import qualified Data.Text as T
import qualified Data.Time as DT
import qualified Data.Time.Clock as DTC
import Data.Typeable (Typeable, typeOf)
import qualified Database.SQLite.Simple as SQ
import qualified Database.SQLite.Simple.FromField as SQF
import qualified Database.SQLite.Simple.FromRow as SQS
import qualified Database.SQLite.Simple.Ok as SQOK
import qualified Filesystem.Path.CurrentOS as FP
import Prelude hiding (FilePath, head)
import qualified System.IO.Error as IOE
import Turtle
import qualified Turtle.Bytes as TB

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
    ((,) <$> F.length
         <*> (F.Fold CH.hashUpdate (CH.hashInit :: CH.Context CHA.SHA1) CH.hashFinalize))
-- eg
-- view $ (filterRegularFiles (lstree "src")) >>= shasum
filterRegularFiles :: Shell FilePath -> Shell FilePath
filterRegularFiles fns = do
  fn <- fns
  s <- lstat fn
  if (isRegularFile s)
    then (return fn)
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

type Checksum = T.Text
type FileSizeBytes = Int
type ModTime = DTC.UTCTime

data FileStatsR = FileStatsR
  { _modtime :: !DTC.UTCTime
  , _filesize :: !Int
  , _checksum :: !T.Text
  } deriving (Show)

-- | Not sure baking errors into the data form is the right way to do it but
-- works for at least having a way of figuring out something went wrong and
-- proceeding w/o log messages etc.
data FileInfo
  = FileStats !FileStatsR
  | FileProblem !T.Text
  | FileGone
  deriving (Show)

data FileCheck = FileCheck
  { _checktime :: !DTC.UTCTime
  , _filepath :: !FilePath
  , _fileinfo :: !FileInfo
  } deriving (Show)

---- Couldn't get this stuff to work.
data FileEventParseError =
  MismatchedTag
  deriving (Eq, Show, Typeable)

instance CE.Exception FileEventParseError

instance SQS.FromRow FileCheck where
  fromRow = do
    (time, path, tag, modtime, filesize, checksum, errmsg) <-
      ((,,,,,,) <$> SQS.field <*> SQS.field <*> SQS.field <*> SQS.field <*>
       SQS.field <*>
       SQS.field <*>
       SQS.field) :: SQS.RowParser ( DTC.UTCTime
                                   , T.Text
                                   , T.Text
                                   , Maybe DTC.UTCTime
                                   , Maybe Int
                                   , Maybe T.Text
                                   , Maybe T.Text)
    case (tag, modtime, filesize, checksum, errmsg) of
      ("Stats", Just modtime, Just filesize, Just checksum, Nothing) ->
        return
          (FileCheck
             time
             (fromText path)
             (FileStats
                (FileStatsR
                 { _modtime = modtime
                 , _filesize = filesize
                 , _checksum = checksum
                 })))
      ("Problem", Nothing, Nothing, Nothing, Just msg) ->
        return (FileCheck time (fromText path) (FileProblem msg))
      ("Gone", Nothing, Nothing, Nothing, Nothing) ->
        return (FileCheck time (fromText path) FileGone)
      _ ->
        return
          (FileCheck
             time
             (fromText path)
             (FileProblem
                ("FileEvent row not correctly stored: " <>
                 (T.pack (show (modtime, filesize, checksum, errmsg))) -- could we hook into builtin parsing error stuff? MT.lift (MT.lift (SQOK.Errors [CE.toException MismatchedTag]))
                 )))

instance SQ.ToRow FileCheck where
  toRow (FileCheck {_checktime, _filepath, _fileinfo}) =
    SQ.toRow
      (case (toText _filepath) of
         Left msg ->
           ( _checktime
           , "???"
           , "Problem" :: T.Text
           , Nothing
           , Nothing
           , Nothing
           , Just msg)
         Right path ->
           case _fileinfo of
             FileStats (FileStatsR {_modtime, _filesize, _checksum}) ->
               ( _checktime
               , path
               , "Stats"
               , Just _modtime
               , Just _filesize
               , Just _checksum
               , Nothing)
             FileProblem msg ->
               ( _checktime
               , path
               , "Problem"
               , Nothing
               , Nothing
               , Nothing
               , Just msg)
             FileGone ->
               (_checktime, path, "Gone", Nothing, Nothing, Nothing, Nothing))

-- | Fold that writes hashes into a HashMap
fileHashes :: MonadIO io => FoldM io FilePath (HashMap String [FilePath])
fileHashes = F.FoldM step (return HashMap.empty) return where
  step hmap fp = do hash <- inshasum fp
                    return (HashMap.insertWith (++) (show hash) [fp] hmap)

-- | Just use filename for comparison, not checksums
cheapHashes :: Fold FilePath (HashMap String [FilePath])
cheapHashes = F.Fold step HashMap.empty id where
  step hmap fp = (HashMap.insertWith (++) (show (filename fp)) [fp] hmap)

-- | Returns (leftButNotRight, rightButNotLeft)
deepDiff :: MonadIO io => FilePath -> FilePath -> io ([FilePath],[FilePath])
deepDiff leftPath rightPath = do leftMap <- foldIO (filterRegularFiles (lstree leftPath)) fileHashes
                                 rightMap <- foldIO (filterRegularFiles (lstree rightPath)) fileHashes
                                 return (concat (HashMap.elems (HashMap.difference leftMap rightMap)),
                                         concat (HashMap.elems (HashMap.difference rightMap leftMap)))

-- | Returns (leftButNotRight, rightButNotLeft)
cheapDiff :: MonadIO io => FilePath -> FilePath -> io ([FilePath],[FilePath])
cheapDiff leftPath rightPath = do leftMap <- fold (filterRegularFiles (lstree leftPath)) cheapHashes
                                  rightMap <- fold (filterRegularFiles (lstree rightPath)) cheapHashes
                                  return (concat (HashMap.elems (HashMap.difference leftMap rightMap)),
                                          concat (HashMap.elems (HashMap.difference rightMap leftMap)))

-- | returns a FoldM that writes a list of FileEvents to SQLite, opens and closes connection for us.
writeDB :: MonadIO io => SQ.Connection -> FoldM io FileCheck ()
writeDB conn = F.FoldM step (return ()) (\_ -> return ()) where
  step _ fe = do liftIO (SQ.execute conn "INSERT INTO main_file_checks (time, path, tag, modtime, filesize, checksum, errmsg) VALUES (?,?,?,?,?,?,?)" fe)

-- | Converts a filepath to a FileAdd
checkFile :: MonadIO io => FilePath -> io FileCheck
checkFile fp = do hash <- inshasum fp
                  now <- date
                  return (FileCheck now fp (FileStats (FileStatsR {_modtime = now -- TODO
                                                                  ,_filesize = 0 -- TODO
                                                                  ,_checksum = (T.pack (show hash))})))

-- | writes a tree of checksums to a sqlite db
addTreeToDb :: String -> FilePath -> IO ()
addTreeToDb dbpath treepath = let checks = do path <- filterRegularFiles (lstree treepath)
                                              checkFile path in
                                SQ.withConnection dbpath (\conn -> foldIO checks (writeDB conn))

-- | uses the first filename as the filename of the target.
cpToDir :: MonadIO io => FilePath -> FilePath -> io ()
cpToDir from toDir = cp from (toDir </> (filename from))

createDB filename =
  SQ.withConnection
    filename
    (\conn ->
       SQ.execute_
         conn
         "CREATE TABLE IF NOT EXISTS main_file_checks (id INTEGER PRIMARY KEY, time TEXT, path TEXT, tag TEXT, modtime TEXT, filesize INTEGER, checksum TEXT, errmsg TEXT)"
    )

-- SQ.query_ conn "SELECT time, path, type, checksum from main_file_events" :: IO [FileEvent]
-- um :: FileArchive -> FilePath -> (FileArchive, FileEvent)
-- :set -XOverloadedStrings
