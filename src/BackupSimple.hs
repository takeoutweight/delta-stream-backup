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

module BackupSimple where

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
import Turtle hiding (select)
import qualified Turtle.Bytes as TB
import Labels hiding (lens)
import qualified Labels as Labels

import Backup

  -- Catch.catch :: (CE.Exception e) => Shell a -> (e -> Shell a) -> Shell a
  -- working off https://hackage.haskell.org/package/turtle-1.4.5/docs/src/Turtle-Prelude.html#nl

-- instance Catch.MonadThrow Shell where
--   throwM = \e -> liftIO (CE.throwIO e)
-- instance Catch.MonadCatch Shell where
--   catch s = Shell _foldIO'
--     where
--       _foldIO' (FoldM step begin done) = _foldIO s (FoldM step' begin' done')
--         where
--           step' st item = step st item -- confusing thing, we get to "pick" what the "items" are for the "passed in" fold? TODO This isn't right yet.
--           begin' = begin
--           done' state = done state
-- using (managed_ (bracket_ init finally))  -- i.e. you give a function taking an action.
-- withSavepoint :: MonadManged managed => managed ()
-- withSavepoint = CE.bracket_
-- problem is, we'd like our action to accept Shell (not just IO) but is that ok?
connection :: MM.MonadManaged managed => String -> managed SQ.Connection
connection filename = using (managed (SQ.withConnection filename))

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
data FileInfoOld
  = FileStats !FileStatsR
  | FileProblem !T.Text
  | FileGoneOld
  deriving (Show)

data FileCheckOld = FileCheckOld
  { _checktime :: !DTC.UTCTime
  , _filepath :: !FilePath
  , _fileroot :: !FilePath
  , _fileoffset :: !FilePath
  , _filename :: !FilePath
  , _filemachine :: !T.Text
  , _fileinfo :: !FileInfoOld
  } deriving (Show)

---- Couldn't get this stuff to work.
data FileEventParseError =
  MismatchedTag
  deriving (Eq, Show, Typeable)

instance CE.Exception FileEventParseError

instance SQS.FromRow FileCheckOld where
  fromRow = do
    (time, path, root, offset, name, machine, tag, modtime, filesize, checksum, errmsg) <-
      ((,,,,,,,,,,) <$> SQS.field <*> SQS.field <*> SQS.field <*> SQS.field <*>
       SQS.field <*>
       SQS.field <*>
       SQS.field <*>
       SQS.field <*>
       SQS.field <*>
       SQS.field <*>
       SQS.field) :: SQS.RowParser ( DTC.UTCTime
                                   , T.Text
                                   , T.Text
                                   , T.Text
                                   , T.Text
                                   , T.Text
                                   , T.Text
                                   , Maybe DTC.UTCTime
                                   , Maybe Int
                                   , Maybe T.Text
                                   , Maybe T.Text)
    return
      (FileCheckOld
         time
         (fromText path)
         (fromText root)
         (fromText offset)
         (fromText name)
         machine
         (case (tag, modtime, filesize, checksum, errmsg) of
            ("Stats", Just modtime, Just filesize, Just checksum, Nothing) ->
              (FileStats
                 (FileStatsR
                  { _modtime = modtime
                  , _filesize = filesize
                  , _checksum = checksum
                  }))
            ("Problem", Nothing, Nothing, Nothing, Just msg) ->
              (FileProblem msg)
            ("Gone", Nothing, Nothing, Nothing, Nothing) -> FileGoneOld
            _ ->
              (FileProblem
                 ("FileEvent row not correctly stored: " <>
                  (T.pack (show (modtime, filesize, checksum, errmsg))) -- could we hook into builtin parsing error stuff? MT.lift (MT.lift (SQOK.Errors [CE.toException MismatchedTag]))
                  ))))

instance SQ.ToRow FileCheckOld where
  toRow (FileCheckOld { _checktime
                      , _filepath
                      , _fileroot
                      , _fileoffset
                      , _filename
                      , _filemachine
                      , _fileinfo
                      }) =
    (case ( (toText _filepath)
          , (toText _fileroot)
          , (toText _fileoffset)
          , (toText _filename)) of
       (Right path, Right root, Right offset, Right name) ->
         ((SQ.toRow
             ( SQTF.toField _checktime
             , SQTF.toField path
             , SQTF.toField root
             , SQTF.toField offset
             , SQTF.toField name
             , SQTF.toField thisMachine)) ++
          (SQ.toRow
             (case _fileinfo of
                FileStats (FileStatsR {_modtime, _filesize, _checksum}) ->
                  ( ("Stats" :: T.Text)
                  , (Just _modtime)
                  , (Just _filesize)
                  , (Just _checksum)
                  , (Nothing :: Maybe T.Text))
                FileProblem msg ->
                  ( ("Problem" :: T.Text)
                  , (Nothing :: Maybe DTC.UTCTime)
                  , (Nothing :: Maybe Int)
                  , (Nothing :: Maybe T.Text)
                  , (Just msg))
                FileGoneOld ->
                  ( ("Gone" :: T.Text)
                  , (Nothing :: Maybe DTC.UTCTime)
                  , (Nothing :: Maybe Int)
                  , (Nothing :: Maybe T.Text)
                  , (Nothing :: Maybe T.Text)))))
       (path, root, offset, name) ->
         [ SQTF.toField _checktime
         , SQTF.toField ("???" :: T.Text)
         , SQTF.toField ("???" :: T.Text)
         , SQTF.toField ("???" :: T.Text)
         , SQTF.toField ("???" :: T.Text)
         , SQTF.toField thisMachine
         , SQTF.toField ("Problem" :: T.Text)
         , SQTF.toField (Nothing :: Maybe DTC.UTCTime)
         , SQTF.toField (Nothing :: Maybe Int)
         , SQTF.toField (Nothing :: Maybe T.Text)
         , SQTF.toField
             (Just ((show path) ++ (show root) ++ (show offset) ++ (show name)))
         ])

data PathToTextException = PathToTextException
  { path :: FilePath
  , errMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception PathToTextException

-- | Fold that writes hashes into a HashMap
fileHashes :: MonadIO io => FoldM io FilePath (HashMap String [FilePath])
fileHashes = F.FoldM step (return HashMap.empty) return
  where
    step hmap fp = do
      hash <- inshasum fp
      return (HashMap.insertWith (++) (show hash) [fp] hmap)

-- | Just use filename for comparison, not checksums
cheapHashes :: Fold FilePath (HashMap String [FilePath])
cheapHashes = F.Fold step HashMap.empty id
  where
    step hmap fp = (HashMap.insertWith (++) (show (filename fp)) [fp] hmap)

-- | Returns (leftButNotRight, rightButNotLeft)
deepDiff :: MonadIO io => FilePath -> FilePath -> io ([FilePath], [FilePath])
deepDiff leftPath rightPath = do
  leftMap <- foldIO (filterRegularFiles (lstree leftPath)) fileHashes
  rightMap <- foldIO (filterRegularFiles (lstree rightPath)) fileHashes
  return
    ( concat (HashMap.elems (HashMap.difference leftMap rightMap))
    , concat (HashMap.elems (HashMap.difference rightMap leftMap)))

-- | Returns (leftButNotRight, rightButNotLeft)
cheapDiff :: MonadIO io => FilePath -> FilePath -> io ([FilePath], [FilePath])
cheapDiff leftPath rightPath = do
  leftMap <- fold (filterRegularFiles (lstree leftPath)) cheapHashes
  rightMap <- fold (filterRegularFiles (lstree rightPath)) cheapHashes
  return
    ( concat (HashMap.elems (HashMap.difference leftMap rightMap))
    , concat (HashMap.elems (HashMap.difference rightMap leftMap)))

unsafeFPToText path =
  case FP.toText path of
    Left err -> CE.throw (PathToTextException path err)
    Right path -> path

-- | returns a FoldM that writes a list of FileEvents to SQLite, opens and closes connection for us.
writeDB :: MonadIO io => SQ.Connection -> FoldM io FileCheckOld ()
writeDB conn = F.FoldM step (return ()) (\_ -> return ())
  where
    step _ fe = do
      liftIO
        (SQ.execute
           conn
           "INSERT INTO main_file_checks (time, path, tag, modtime, filesize, checksum, errmsg) VALUES (?,?,?,?,?,?,?)"
           fe)

-- Old code from the SQLite Simple version of things.
createDBOld filename =
  SQ.withConnection
    filename
    (\conn ->
       SQ.execute_
         conn
         "CREATE TABLE IF NOT EXISTS main_file_checks (id INTEGER PRIMARY KEY, time TEXT, path TEXT, tag TEXT, modtime TEXT, filesize INTEGER, checksum TEXT, errmsg TEXT)")
-- CREATE TABLE IF NOT EXISTS file_checks (bcheckid INTEGER PRIMARY KEY, bchecktime TEXT, bfilemachine TEXT)
-- DROP TABLE file_checks
-- SQ.query_ conn "SELECT time, path, type, checksum from main_file_events" :: IO [FileEvent]
-- um :: FileArchive -> FilePath -> (FileArchive, FileEvent)

{- | Checking should presume there is already a FileInfo. We may have already done
     a stat to add the FileInfo in the first place. 
-}
checkFile3 ::
     (MonadIO io
     , Has "conn" SQ.Connection r
     , Has "rechecksum" (UTCTime -> Maybe UTCTime -> Bool) r
     , Has "fileInfo" FileInfo r
     , Has "statTime" UTCTime r
     , Has "stat" FileStatus r
     , Has "remote" Text  r
     , Has "pathText" Text r
     , Has "absPath" FilePath r
     ) =>
     r -> io (Maybe ShaCheck)
checkFile3 r = do
  lastCheck :: Maybe ShaCheck <- liftIO (getRecentFileCheck2 (get #conn r) (_file_info_id (get #fileInfo r)))
  (if ((get #rechecksum r) (get #statTime r) (fmap _sha_check_time lastCheck))
     then (do (size, checksum) <- inSizeAndSha (get #absPath r)
              let checksumText = (T.pack (show checksum))
              let modTime = POSIX.posixSecondsToUTCTime (modificationTime (get #stat r))
              return
                (Just
                  (ShaCheck
                    { _sha_check_id = Auto Nothing
                    , _sha_check_time = (get #statTime r)
                    , _file_remote = (get #remote r)
                    , _sha_check_absolute_path = (get #pathText r)
                    , _mod_time = modTime
                    , _file_size = size
                    , _actual_checksum = checksumText
                    , _sc_file_info_id = FileInfoId (_file_info_id (get #fileInfo r))
                    })))
     else return Nothing)
