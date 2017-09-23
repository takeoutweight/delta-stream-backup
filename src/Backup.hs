{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}

module Backup where

import Control.Exception
import qualified Control.Foldl as F
import Control.Monad
import qualified Control.Monad.Managed as MM
import qualified Crypto.Hash as CH
import qualified Crypto.Hash.Algorithms as CHA
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Time.Clock as DTC
import qualified Database.SQLite.Simple as SQ
import qualified Database.SQLite.Simple.FromRow as SQS
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

-- Testin sqlite
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


fptotext fp = case (toText fp) of
  Left a -> a
  Right a -> a

-- instance SQS.FromRow FileEvent where
--   fromRow = FileEvent <$> SQS.field <*> SQS.field

instance SQ.ToRow FileEvent where
  toRow (FileEvent time path (FileAdd cs)) = SQ.toRow (time, fptotext path, "Add" :: T.Text, Just cs)
  toRow (FileEvent time path FileDelete)   = SQ.toRow (time, fptotext path, "Delete" :: T.Text, Nothing :: Maybe T.Text)

testdb = do
  conn <- SQ.open "/tmp/gpg-tests/thedb.db"
  SQ.execute_ conn "CREATE TABLE IF NOT EXISTS main_file_events (id INTEGER PRIMARY KEY, time INTEGER, path TEXT, type TEXT, checksum TEXT)"
  --  SQ.execute conn "INSERT INTO test (path, checksum) VALUES (?,?)" (SQ.Only ("test string 2" :: String))
  path <- pwd
  now <- DTC.getCurrentTime
  SQ.execute conn "INSERT INTO main_file_events (time, path, type, checksum) VALUES (?,?,?,?)" (FileEvent now path (FileAdd "123"))
  SQ.execute conn "INSERT INTO main_file_events (time, path, type, checksum) VALUES (?,?,?,?)" (FileEvent now path FileDelete)
  SQ.close conn
  return "OK"

-- ascii armour'd:
-- stdout $ (inproc "gpg" ["-a", "-r", "Nathan Sorenson (SFU)", "--encrypt"] (select ["hello"]))
someFunc :: IO ()
someFunc = putStrLn "someFunc"
