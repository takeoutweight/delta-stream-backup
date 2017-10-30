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

data FileEventType
  = FileAdd Checksum
  | FileDelete
  deriving (Show)

data FileEvent =
  FileEvent DTC.UTCTime
            FilePath
            FileEventType
  deriving (Show)

fptotext fp =
  case (toText fp) of
    Left s -> error ("Can't stringify path: " ++ (T.unpack s))
    Right s -> s

---- Couldn't get this stuff to work.
data FileEventParseError =
  MismatchedTag
  deriving (Eq, Show, Typeable)

instance CE.Exception FileEventParseError

instance SQS.FromRow FileEvent where
  fromRow = do
    (time, path, tag, mcs) <-
      ((,,,) <$> SQS.field <*> SQS.field <*> SQS.field <*> SQS.field) :: SQS.RowParser ( DTC.UTCTime
                                                                                       , T.Text
                                                                                       , T.Text
                                                                                       , Maybe T.Text)
    case (tag, mcs) of
      ("Add", Just cs) -> return (FileEvent time (fromText path) (FileAdd cs))
      ("Delete", Nothing) -> return (FileEvent time (fromText path) FileDelete)
      _ ->
        error
          ("FileEvent row not correcntly stored: " ++ (show tag) ++ (show mcs)) -- (maybe we can't hook into the sql parser errors and have to do that elsewhere?) MT.lift (MT.lift (SQOK.Errors [CE.toException MismatchedTag]))

instance SQ.ToRow FileEvent where
  toRow (FileEvent time path (FileAdd cs)) =
    SQ.toRow (time, fptotext path, "Add" :: T.Text, Just cs)
  toRow (FileEvent time path FileDelete) =
    SQ.toRow (time, fptotext path, "Delete" :: T.Text, Nothing :: Maybe T.Text)

fileHashes :: MonadIO io => FoldM io  FilePath (HashMap String [FilePath])
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

-- | uses the first filename as the filename of the target.
cpToDir :: MonadIO io => FilePath -> FilePath -> io ()
cpToDir from toDir = cp from (toDir </> (filename from))

testdb = do
  conn <- SQ.open "/tmp/gpg-tests/thedb.db"
  SQ.execute_
    conn
    "CREATE TABLE IF NOT EXISTS main_file_events (id INTEGER PRIMARY KEY, time TEXT, path TEXT, type TEXT, checksum TEXT)"
  --  SQ.execute conn "INSERT INTO test (path, checksum) VALUES (?,?)" (SQ.Only ("test string 2" :: String))
  path <- pwd
  now <- DTC.getCurrentTime
  SQ.execute
    conn
    "INSERT INTO main_file_events (time, path, type, checksum) VALUES (?,?,?,?)"
    (FileEvent now path (FileAdd "123"))
  SQ.execute
    conn
    "INSERT INTO main_file_events (time, path, type, checksum) VALUES (?,?,?,?)"
    (FileEvent now path FileDelete)
  r <-
    SQ.query_ conn "SELECT time, path, type, checksum from main_file_events" :: IO [FileEvent]
  SQ.close conn
  return (show r)

-- um :: FileArchive -> FilePath -> (FileArchive, FileEvent)
