-- ingests files from the disk into database
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NamedFieldPuns#-}
{-# LANGUAGE ScopedTypeVariables #-}
-- {-# LANGUAGE PartialTypeSignatures #-}
-- {-# LANGUAGE NamedWildCards #-}
-- {-# LANGUAGE TypeSynonymInstances #-}
-- {-# LANGUAGE TypeApplications #-}

module IngestPath where

import qualified Control.Foldl as F
import qualified Control.Monad.Catch as Catch
import qualified Crypto.Hash as CH
import qualified Crypto.Hash.Algorithms as CHA
import qualified Data.ByteString as BS
import qualified Data.Time.Clock.POSIX as POSIX
import Data.Time.Clock (UTCTime)
import qualified Data.Text as T
import qualified Database.SQLite.Simple as SQ
import qualified Filesystem.Path.CurrentOS as FP
import qualified System.IO.Error as Error
import Prelude hiding (FilePath, head)
import qualified Turtle as Turtle
import Turtle
       (date, err, filename, fold, guard, isRegularFile, liftIO, lstat,
        lstree, modificationTime, repr, sh, when)
import qualified Turtle.Bytes as TB

import qualified DBHelpers as DB
import Fields
import Logic

------- up to here

inSizeAndSha :: Turtle.MonadIO io => Turtle.FilePath -> io (Int, (CH.Digest CHA.SHA1))
inSizeAndSha fp =
  fold
    (TB.input fp)
    ((,) <$> F.premap BS.length F.sum <*>
     (F.Fold CH.hashUpdate (CH.hashInit :: CH.Context CHA.SHA1) CH.hashFinalize))

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
  -> Turtle.FileStatus
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
                     fileStatus :: Either () Turtle.FileStatus <-
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
