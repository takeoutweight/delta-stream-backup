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
-- {-# LANGUAGE TypeOperators #-}
-- {-# LANGUAGE DataKinds #-}

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
import Turtle hiding (select)
import qualified Turtle.Bytes as TB
import Control.Lens hiding ((:>), Fold, cons)
import Data.Vinyl.Lens (RElem)
import Data.Vinyl.TypeLevel (RIndex)

import Fields

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

thisMachine = "Nathans-MacBook-Pro-2" :: T.Text

thisArchive = "main-archive" :: T.Text

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
           , "sc_file_info_id__file_info_id TEXT" -- Not sure how Beam figures this
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
           , "fgc_file_info_id__file_info_id TEXT"
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
  , _archive_file_size :: Columnar f (Maybe Int)
  , _archive :: Columnar f Text -- TODO Ref to an "archive" table, but for now an ad-hoc label. This is so a single DB can contain unrelated archives (mounted at different locations on different remotes, different update policies, etc)  - each machine can locate each archive on different mountpoints, which is not important, but the path relative to the archive root IS semantic and reflected on all remotes.
  , _source_remote :: Columnar f Text
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
           , "archive_file_size INT"
           , "archive TEXT"
           , "source_remote TEXT"
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

{- | FilePath seems to only treat paths with trailing slashes as "directories" but
     eg `pwd` doesn't give a trailing slash.
-}
ensureTrailingSlash :: FilePath -> FilePath
ensureTrailingSlash fp = fp FP.</> ""

-- | Never re-check
defaultRechecksum :: UTCTime -> Maybe UTCTime -> Bool
defaultRechecksum now Nothing = True
defaultRechecksum now (Just prev) = False

insertFileInfo conn fileInfo =
  (withDatabaseDebug
     putStrLn
     conn
     (runInsert (insert (_file_info fileDB) (insertValues [fileInfo]))))

insertFileGoneCheck conn fileGone =
  (withDatabaseDebug
     putStrLn
     conn
     (runInsert (insert (_file_gone_check fileDB) (insertValues [fileGone]))))

insertShaCheck conn shaCheck =
  (withDatabaseDebug
     putStrLn
     conn
     (runInsert (insert (_sha_check fileDB) (insertValues [shaCheck]))))

updateFileInfo conn fileInfo =
  (withDatabaseDebug
     putStrLn
     conn
     (runUpdate (save (_file_info fileDB) fileInfo)))

-- | Using my custom one (works fine)
getFileInfo conn fileInfoID =
  (DB.selectExactlyOne
     conn
     (do fileInfo <- all_ (_file_info fileDB)
         guard_ ((_file_info_id fileInfo) ==. val_ fileInfoID)
         pure fileInfo))

-- | Without using my custom one (not necessary getFileInfo works)
getFileInfo2 conn fileInfoID = do
  res <-
    (withDatabaseDebug
       putStrLn
       conn
       (runSelectReturningOne
          (select
             (do fileInfo <- all_ (_file_info fileDB)
                 guard_ ((_file_info_id fileInfo) ==. val_ fileInfoID)
                 pure fileInfo))))
  return
    (case res of
       Just res -> DB.One res
       Nothing -> DB.None)

-- | FIXME THis one doesn't work
getRecentFileCheck conn fileInfoID =
  (DB.selectJustOne
     conn
     (orderBy_
        (\s -> (desc_ (_sha_check_time s)))
        (do shaCheck <- all_ (_sha_check fileDB)
            guard_
              ((_sc_file_info_id shaCheck) ==. val_ (FileInfoId fileInfoID))
            return shaCheck)))

getRecentFileCheck2 conn fileInfoID =
  (withDatabaseDebug
     putStrLn
     conn
     (runSelectReturningOne
        (select
           (orderBy_
              (\s -> (desc_ (_sha_check_time s)))
              (do shaCheck <- all_ (_sha_check fileDB)
                  guard_
                    ((_sc_file_info_id shaCheck) ==.
                     val_ (FileInfoId fileInfoID))
                  return shaCheck)))))

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

{- | Given an absolute path, check it - creating the required logical entry if
     needed. This is for ingesting new files.
-}
checkFile2 ::
  ( MonadIO io
  , Has SQ.Connection r
  , Has Archive r
  , Has Remote r
  , Has MasterRemote r
  , Has Root r
  , Has AbsPath r
  , Has Rechecksum r)
  => Record r
  -> io ()
checkFile2 ctx =
  let conn :: SQ.Connection = (fget ctx)
      Archive archive = (fget ctx)
      Remote remote = (fget ctx)
      MasterRemote masterRemote = (fget ctx)
      Root root = (fget ctx)
      AbsPath absPath = (fget ctx)
      Rechecksum rechecksum = (fget ctx) in
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
                        let doCheck res stat = do
                              echo "Found one"
                              let modTime =
                                    POSIX.posixSecondsToUTCTime
                                      (modificationTime stat)
                              lastCheck :: Maybe ShaCheck <-
                                (getRecentFileCheck2 (conn :: SQ.Connection) fileInfoID)
                              (when
                                 (rechecksum
                                    statTime
                                    (fmap _sha_check_time lastCheck))
                                 (do (size, checksum) <- inSizeAndSha absPath
                                     let checksumText = (T.pack (show checksum))
                                     (insertShaCheck
                                        conn
                                        (ShaCheck
                                         { _sha_check_id = Auto Nothing
                                         , _sha_check_time = statTime
                                         , _file_remote = remote
                                         , _sha_check_absolute_path = pathText
                                         , _mod_time = modTime
                                         , _file_size = size
                                         , _actual_checksum = checksumText
                                         , _sc_file_info_id =
                                             FileInfoId fileInfoID
                                         }))
                                     (when
                                        (masterRemote &&
                                         (_archive_checksum res) /=
                                         (Just checksumText))
                                        (updateFileInfo
                                           conn
                                           (res
                                            { _seen_change = statTime
                                            , _exited = Nothing
                                            , _archive_checksum =
                                                Just checksumText
                                            , _archive_file_size = Just size
                                            })))))
                        fileStatus :: Either () FileStatus <-
                          (Catch.tryJust
                             (guard . Error.isDoesNotExistError)
                             (lstat absPath))
                        result :: DB.SelectOne FileInfo <-
                          (getFileInfo conn fileInfoID)
                        (case (result, fileStatus) of
                           (DB.Some _ _, _) ->
                             err
                               (repr ("Many existed!? Error!" ++ show absPath))
                           (DB.None, Left _) -> return () -- didn't find the file but didn't have a record of it either.
                           (DB.None, Right stat)
                             | isRegularFile stat && masterRemote -> do
                               let res =
                                     (FileInfo
                                      { _file_info_id = fileInfoID
                                      , _seen_change = statTime
                                      , _exited = Nothing
                                      , _archive_checksum = Nothing
                                      , _archive_file_size = Nothing
                                      , _archive = archive
                                      , _source_remote = remote
                                      , _file_path = relText
                                      , _file_name = nameText
                                      })
                               echo (repr ("Adding new file " ++ show absPath))
                               (insertFileInfo conn res)
                               (doCheck res stat)
                           (DB.One res, Left _) -> do
                             echo "gone"
                             (insertFileGoneCheck
                                conn
                                (FileGoneCheck
                                 { _fgc_id = Auto Nothing
                                 , _fgc_time = statTime
                                 , _fgc_remote = remote
                                 , _fgc_absolute_path = pathText
                                 , _fgc_file_info_id = FileInfoId fileInfoID
                                 }))
                             (when
                                (masterRemote == True &&
                                 (_exited res) == Nothing)
                                (updateFileInfo
                                   conn
                                   (res
                                    { _seen_change = statTime
                                    , _exited = Just statTime
                                    , _archive_checksum = Nothing
                                    , _archive_file_size = Nothing
                                    })))
                           (DB.One res, Right stat)
                             | isRegularFile stat -> doCheck res stat
                           _ ->
                             err
                               (repr
                                  ("Can't process file (is it a regular file?)" ++
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

-- | Walks dirpath recursively
addTreeToDb2 ctx dirpath =
  let checks conn = do
        fp <- (lstree dirpath)
        checkFile2 (AbsPath fp &: conn &: ctx)
  in SQ.withConnection (dbpath (fget ctx)) (\conn -> (sh (checks conn)))

addTreeDefaults =
  (  DBPath defaultDBFile
  &: Archive "archie"
  &: Remote "Nates-MBP-2014"
  &: MasterRemote True
  &: Root "/Users/nathan/"
  &: Rechecksum defaultRechecksum
  &: Nil
  )

-- addTreeToDb2 addTreeDefaults "/Users/nathan/Pictures/2013/2013-05-15/"

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
              (SQ.execute_ conn createShaCheckTable)
              (SQ.execute_ conn createFileGoneCheckTable)
              (SQ.execute_ conn createFileInfoTable)
              (SQ.execute_ conn "RELEASE createDB"))
          (do (SQ.execute_ conn "ROLLBACK TO createDB")
              (SQ.execute_ conn "RELEASE createDB"))))
