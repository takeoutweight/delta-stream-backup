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
import Turtle hiding (select)
import qualified Turtle.Bytes as TB
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

thisMachine = "Nathans-MacBook-Pro-2" :: T.Text

thisArchive = "main-archive" :: T.Text

data FileCheckOld = FileCheckOld
  { _checktime :: !DTC.UTCTime
  , _filepath :: !FilePath
  , _fileroot :: !FilePath
  , _fileoffset :: !FilePath
  , _filename :: !FilePath
  , _filemachine :: !T.Text
  , _fileinfo :: !FileInfoOld
  } deriving (Show)

-- copied from Database.Beam.Sqlite.Syntax to allow UTCTime. Not sure this is
-- right, but seems like the LocalTime that we can write out of the box doesn't
-- serialize the timezone. You can specify timezone in the column, but what
-- enforces you writing the same time-zone back into the DB if you change that
-- later? Besides - Turtle uses UTCTime so this is easier.
emitValue :: SQ.SQLData -> BSS.SqliteSyntax
emitValue v = SqliteSyntax (BSB.byteString "?") (DL.singleton v)

instance HasSqlValueSyntax SqliteValueSyntax UTCTime where
  sqlValueSyntax tm =
    SqliteValueSyntax (emitValue (SQ.SQLText (fromString tmStr)))
    where
      tmStr =
        DT.formatTime
          DT.defaultTimeLocale
          (DT.iso8601DateFormat (Just "%H:%M:%S%Q"))
          tm

data ShaCheckT f = ShaCheck
  { _sha_check_id :: Columnar f (Auto Int)
  , _sha_check_time :: Columnar f UTCTime
  , _file_remote :: Columnar f Text
  , _sha_check_absolute_path :: Columnar f Text -- This could be redundant, as we should conceivably know the location of the archive on each remote. But maybe we'll support moving these and want a record of the historical location etc?
  , _mod_time :: Columnar f UTCTime
  , _file_size :: Columnar f Int
  , _actual_checksum :: Columnar f Text -- i.e. on-disk checksum, not necessarily the plaintext checksum.
  , _sc_file_info_id :: PrimaryKey FileInfoT f -- Columnar f FileInfoId
  } deriving (Generic)

createShaCheckTable :: SQ.Query
createShaCheckTable =
  "CREATE TABLE IF NOT EXISTS sha_check " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "sha_check_id INTEGER PRIMARY KEY"
           , "sha_check_time TEXT"
           , "file_remote TEXT"
           , "sha_check_absolute_path TEXT"
           , "mod_time TEXT"
           , "file_size INTEGER"
           , "actual_checksum TEXT"
           , "sc_file_info_id TEXT"
           ])))

type ShaCheck = ShaCheckT Identity

type ShaCheckId = PrimaryKey ShaCheckT Identity

deriving instance Show ShaCheck

deriving instance Eq ShaCheck

-- deriving instance Show (ShaCheckT (Nullable Identity)) -- This always required for a nullable mixin?
-- deriving instance Eq (ShaCheckT (Nullable Identity))
-- deriving instance Show (PrimaryKey ShaCheckT (Nullable Identity))
-- deriving instance Eq (PrimaryKey ShaCheckT (Nullable Identity))
instance Beamable ShaCheckT

instance Table ShaCheckT where
  data PrimaryKey ShaCheckT f = ShaCheckId (Columnar f (Auto Int))
                            deriving Generic
  primaryKey = ShaCheckId . _sha_check_id

instance Beamable (PrimaryKey ShaCheckT)

-- TODO This shares lots of structure w/  ShaCheck, would be nice to factor it.
data FileGoneCheckT f = FileGoneCheck
  { _fgc_id :: Columnar f (Auto Int)
  , _fgc_time :: Columnar f UTCTime
  , _fgc_remote :: Columnar f Text
  , _fgc_absolute_path :: Columnar f Text
  , _fgc_file_info_id :: PrimaryKey FileInfoT f
  } deriving (Generic)

createFileGoneCheckTable :: SQ.Query
createFileGoneCheckTable =
  "CREATE TABLE IF NOT EXISTS file_gone_check " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "fgc_id INTEGER PRIMARY KEY"
           , "fgc_time TEXT"
           , "fgc_remote TEXT"
           , "fgc_absolute_path TEXT"
           , "fgc_file_info_id TEXT"
           ])))

type FileGoneCheck = FileGoneCheckT Identity

type FileGoneCheckId = PrimaryKey FileGoneCheckT Identity

deriving instance Show FileGoneCheck

deriving instance Eq FileGoneCheck

instance Beamable FileGoneCheckT

instance Table FileGoneCheckT where
  data PrimaryKey FileGoneCheckT f = FileGoneCheckId (Columnar f (Auto Int))
                            deriving Generic
  primaryKey = FileGoneCheckId . _fgc_id

instance Beamable (PrimaryKey FileGoneCheckT)

data FileInfoT f = FileInfo
  { _file_info_id :: Columnar f Text
  , _seen_change :: Columnar f UTCTime -- Last time we've seen the contents change, to warrant a sync.
  , _exited :: Columnar f (Maybe UTCTime)
  , _archive_checksum :: Columnar f (Maybe Text) -- always plaintext, the source-of-truth in the archive. None can mean "Don't know, haven't checked yet" or "file doesn't exit"
  , _archive :: Columnar f Text -- TODO Ref to an "archive" table, but for now an ad-hoc label. This is so a single DB can contain unrelated archives (mounted at different locations, different update policies, etc)  - each machine can locate each archive on different mountpoints, which is not important, but the path relative to the archive root IS semantic and reflected on all remotes.
  , _file_path :: Columnar f Text -- relative to archive root, including filename, so we can determine "the same file" in different remotes.
  , _file_name :: Columnar f Text -- Just for convenience, for use w/ `locate` or dedup etc.
  } deriving (Generic)

createFileInfoTable :: SQ.Query
createFileInfoTable =
  "CREATE TABLE IF NOT EXISTS file_info " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "file_info_id TEXT PRIMARY KEY"
           , "seen_change TEXT"
           , "exited TEXT"
           , "archive_checksum TEXT"
           , "archive TEXT"
           , "file_path TEXT"
           , "file_name TEXT"
           ])))

type FileInfo = FileInfoT Identity

type FileInfoId = PrimaryKey FileInfoT Identity

deriving instance Show FileInfo

deriving instance Show FileInfoId

deriving instance Eq FileInfo

deriving instance Eq FileInfoId

instance Beamable FileInfoT

instance Beamable (PrimaryKey FileInfoT)

instance Table FileInfoT where
  data PrimaryKey FileInfoT f = FileInfoId (Columnar f Text)
                            deriving Generic
  primaryKey = FileInfoId . _file_info_id

data FileDB f = FileDB
  { _file_info :: f (TableEntity FileInfoT)
  , _sha_check :: f (TableEntity ShaCheckT)
  , _file_gone_check :: f (TableEntity FileGoneCheckT)
  } deriving (Generic)

instance Database FileDB

fileDB :: DatabaseSettings be FileDB
fileDB = defaultDbSettings

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

data PathToTextException = PathToTextException
  { path :: FilePath
  , errMsg :: !Text
  } deriving (Show, Typeable)

instance CE.Exception PathToTextException

unsafeFPToText path =
  case FP.toText path of
    Left err -> CE.throw (PathToTextException path err)
    Right path -> path

-- not safe if machine has colons in it.
fileInfoId :: Text -> FilePath -> Text
fileInfoId machine path = T.intercalate ":" [machine, unsafeFPToText path]

{- | FilePath seems to only treat paths with trailing slashes as "directories" but
     eg `pwd` doesn't give a trailing slash.
-}
ensureTrailingSlash :: FilePath -> FilePath
ensureTrailingSlash fp = fp FP.</> ""

-- | Never re-check
defaultRechecksum :: UTCTime -> Maybe UTCTime -> Bool
defaultRechecksum now Nothing = True
defaultRechecksum now (Just prev) = False

-- | Given an absolute path, check it - creating the required logical entry if needed.
checkFile2 ::
     MonadIO io
  => SQ.Connection
  -> Text
  -> Text
  -> Bool
  -> (UTCTime -> Maybe UTCTime -> Bool)
  -> FilePath
  -> FilePath
  -> io ()
checkFile2 conn archive remote masterRemote rechecksum root absPath =
  case stripPrefix (ensureTrailingSlash root) absPath of
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
           let fileInfoID = (T.intercalate ":" [archive, relText])
           in (liftIO
                 (Catch.onException
                    (do (SQ.execute_ conn "SAVEPOINT Backup_checkFile2")
                        statTime <- date
                        fileStatus :: Either () FileStatus <-
                          (Catch.tryJust
                             (guard . Error.isDoesNotExistError)
                             (lstat absPath))
                        result :: DB.SelectOne FileInfo <-
                          (DB.selectExactlyOne
                             conn
                             (do fileInfo <- all_ (_file_info fileDB)
                                 guard_
                                   ((_file_info_id fileInfo) ==. val_ fileInfoID)
                                 pure fileInfo))
                        (case (result, fileStatus) of
                           (DB.Some _ _, _) ->
                             err
                               (repr ("Many existed!? Error!" ++ show absPath))
                           (DB.None, Left _) -> return () -- didn't find the file but didn't have a record of it either.
                           (DB.None, Right stat)
                             | isRegularFile stat -> do
                               echo "None existed yet, Nice."
                               (withDatabaseDebug
                                  putStrLn
                                  conn
                                  (runInsert
                                     (insert
                                        (_file_info fileDB)
                                        (insertValues
                                           [ FileInfo
                                             { _file_info_id = fileInfoID
                                             , _seen_change = statTime
                                             , _exited = Nothing
                                             , _archive_checksum = Nothing
                                             , _archive = archive
                                             , _file_path = pathText
                                             , _file_name = nameText
                                             }
                                           ]))))
                           (DB.One res, Left _) -> do
                             echo "gone"
                             (withDatabaseDebug
                                putStrLn
                                conn
                                (runInsert
                                   (insert
                                      (_file_gone_check fileDB)
                                      (insertValues
                                         [ FileGoneCheck
                                           { _fgc_id = Auto Nothing
                                           , _fgc_time = statTime
                                           , _fgc_remote = remote
                                           , _fgc_absolute_path = pathText
                                           , _fgc_file_info_id =
                                               FileInfoId fileInfoID
                                           }
                                         ]))))
                             (when
                                (masterRemote == True &&
                                 (_exited res) == Nothing)
                                (withDatabaseDebug
                                   putStrLn
                                   conn
                                   (runUpdate
                                      (save
                                         (_file_info fileDB)
                                         (res
                                          { _seen_change = statTime
                                          , _exited = Just statTime
                                          , _archive_checksum = Nothing
                                          })))))
                           (DB.One res, Right stat)
                             | isRegularFile stat -> do
                               echo "Found one"
                               let modTime =
                                     POSIX.posixSecondsToUTCTime
                                       (modificationTime stat)
                               lastCheck :: Maybe ShaCheck <-
                                 (DB.selectJustOne
                                    conn
                                    (orderBy_
                                       (\s -> (desc_ (_sha_check_time s)))
                                       (do shaCheck <- all_ (_sha_check fileDB)
                                           guard_
                                             ((_sc_file_info_id shaCheck) ==.
                                              val_ (FileInfoId fileInfoID))
                                           return shaCheck)))
                               (when
                                  (rechecksum
                                     statTime
                                     (fmap _sha_check_time lastCheck))
                                  (do (size, checksum) <- inSizeAndSha absPath
                                      let checksumText =
                                            (T.pack (show checksum))
                                      (withDatabaseDebug
                                         putStrLn
                                         conn
                                         (runInsert
                                            (insert
                                               (_sha_check fileDB)
                                               (insertValues
                                                  [ ShaCheck
                                                    { _sha_check_id =
                                                        Auto Nothing
                                                    , _sha_check_time = statTime
                                                    , _file_remote = remote
                                                    , _sha_check_absolute_path =
                                                        pathText
                                                    , _mod_time = modTime
                                                    , _file_size = size
                                                    , _actual_checksum =
                                                        checksumText
                                                    , _sc_file_info_id =
                                                        FileInfoId fileInfoID
                                                    }
                                                  ]))))
                                      (when
                                         (masterRemote == True &&
                                          (_archive_checksum res) /=
                                          (Just checksumText))
                                         (withDatabaseDebug
                                            putStrLn
                                            conn
                                            (runUpdate
                                               (save
                                                  (_file_info fileDB)
                                                  (res
                                                   { _seen_change = statTime
                                                   , _exited = Nothing
                                                   , _archive_checksum =
                                                       Just checksumText
                                                   })))))))
                           -- If it's not a regulard file likely.
                           _ ->
                             err
                               (repr
                                  ("Fallthrough for (is it a regular file?)" ++
                                   show absPath)))
                        (SQ.execute_ conn "RELEASE Backup_checkFile2")
                        return ())
                    (do (SQ.execute_ conn "ROLLBACK TO Backup_checkFile2")
                        (SQ.execute_ conn "RELEASE Backup_checkFile2"))))
         a ->
           err
             (repr
                ("Can't textify path: " ++
                 show root ++ ", " ++ show absPath ++ " : " ++ show a)))

-- mkFileInfo :: MonadIO io => (Maybe ShaCheck) -> ShaCheck -> (Maybe FileInfo) -> (Maybe FileInfo)
-- mkFileInfo = undefined
checkFile :: UTCTime -> FilePath -> FileStatus -> Shell FileCheckOld
checkFile = undefined

{- | writes a ls tree of checksums to a sqlite db, storing check time, modtime,
     filesize and sha.  TODO: We could add an optimization so we only do the
     checkFile and write if the file is new or modtime/filesize changed (or
     checktime is too long ago to trust etc)
-}
addTreeToDb :: String -> FilePath -> IO ()
addTreeToDb dbpath treepath =
  let checks = do
        (now, path, stats) <- regularStats (lstree treepath)
        checkFile now path stats
  in SQ.withConnection dbpath (\conn -> foldIO checks (writeDB conn))

addTreeToDb2 dbpath archive remote masterRemote root absPath =
  let checks conn = do
        fp <- (lstree absPath)
        checkFile2 conn archive remote masterRemote defaultRechecksum root fp
  in SQ.withConnection dbpath (\conn -> (sh (checks conn)))

-- addTreeToDb2 defaultDBFile "archie" "Nates-MBP-2014" True "/Users/nathan/" "/Users/nathan/Pictures/2013/2013-05-15/"

-- | uses the first filename as the filename of the target.
cpToDir :: MonadIO io => FilePath -> FilePath -> io ()
cpToDir from toDir = cp from (toDir </> (filename from))

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
-- :set -XOverloadedStrings

defaultDBFile = "/Users/nathan/src/haskell/backup/resources/archive.sqlite"

createDB filename =
  SQ.withConnection
    filename
    (\conn ->
       (Catch.onException
          (do (SQ.execute_ conn "SAVEPOINT createDB")
              (SQ.execute_ conn createShaCheckTable)
              (SQ.execute_ conn createFileGoneCheckTable)
              (SQ.execute_ conn createFileInfoTable)
              (SQ.execute_ conn "RELEASE createDB"))
          (do (SQ.execute_ conn "ROLLBACK TO createDB")
              (SQ.execute_ conn "RELEASE createDB"))))
