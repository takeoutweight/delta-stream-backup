-- builds commands for executing file transfers
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NamedFieldPuns#-}
-- {-# LANGUAGE PartialTypeSignatures #-}
-- {-# LANGUAGE NamedWildCards #-}
-- {-# LANGUAGE TypeSynonymInstances #-}
-- {-# LANGUAGE TypeApplications #-}

module CopyCommands where

import qualified Control.Exception as CE
import Control.Lens ((&), op)
import qualified Data.Binary as Binary
import qualified Data.ByteString.Base64 as Base64
import qualified Data.ByteString.Char8 as BSChar8
import qualified Data.ByteString.Lazy as BSLazy
import qualified Data.List as List
import qualified Data.List.Extra as Extra
import qualified Data.Hashable as Hashable
import qualified Data.Maybe as Maybe
import Data.Monoid ((<>))
import qualified Data.Text.Encoding as Encoding
import Data.Text (Text)
import Database.Beam (select)
import qualified Database.SQLite.Simple as SQ
import qualified Filesystem.Path.CurrentOS as FP
import Prelude hiding (FilePath, head)
import qualified Turtle as Turtle

import qualified DBHelpers as DB
import Fields
import Query
import Logic

-- | Has there ever been a file known to be at this location? (i.e. are
-- copies/updates to this filename expected to overwrite an existing file?) If
-- not we know we can have a "preserve existing" flag on rsynce, etc, to help us
-- avoid clobbering data.
fileExpected :: SQ.Connection -> AbsPathText -> IO Bool
fileExpected conn (AbsPathText pathtext) = do
  fss <-
    (DB.runSelectOne
       conn
       (select (allFileStates `qGuard` absPathIs pathtext `qGuard` hasCheckTime)))
  return
    (case fss of
       Just a -> True
       nothing -> False)

data SharedRelativeCp = SharedRelativeCp
  { _rsFile :: RelativePathText
  , _rsFrom :: Location
  , _rsTo :: Location
  } deriving (Show)

data AlteredRelativeCp = AlteredRelativeCp
  { _cpFrom :: AbsPathText
  , _cpTo :: AbsPathText
  } deriving (Show)

data CopyEntry = CopySharedRelative SharedRelativeCp | CopyAlteredRelative AlteredRelativeCp

-- | Follow Mirrored provenance links until we find a canonical source
getFirstCanonicalProvenance :: SQ.Connection -> Int -> IO (Maybe FileStateF)
getFirstCanonicalProvenance conn fileStateID = do
  provChain <- provenanceChain conn fileStateID
  return
    (List.find
       (\fs ->
          case (fget fs) of
            Canonical -> True
            _ -> False)
       provChain)

-- | Returns a list of proposed copy commands. Namely, any expected file in the
-- db that hasn't been sha verified yet. Can propose overwrites as it doesn't
-- check the filesystem - so trusts that the copy command used (eg rsync)
-- doesn't execute overwrites.
proposeCopyCmds :: SQ.Connection -> IO [CopyEntry]
proposeCopyCmds conn = do
  fss <-
    (DB.runSelectList
       conn
       (select
          (allFileStates `qGuard` noCheckTime `qGuard` isMirrored `qGuard`
           isActual)))
  cps <-
    (traverse
       (\fs ->
          let target = (unFileState fs)
          in case (fget target :: Provenance) of
               Ingested ->
                 CE.throw
                   (BadDBEncodingException
                      "file_state"
                      (nget FileStateIdF target)
                      "Should have provenance id defined given our query")
               Mirrored prov_id -> do
                 msource <- (getFirstCanonicalProvenance conn prov_id)
                 return
                   (fmap
                      (\source ->
                         case ((nget RelativePathText source) ==
                               (nget RelativePathText target)) of
                           False ->
                             CopyAlteredRelative
                               (AlteredRelativeCp
                                { _cpFrom = (fget source)
                                , _cpTo = (fget target)
                                })
                           True ->
                             CopySharedRelative
                               (SharedRelativeCp
                                { _rsFile = (fget source)
                                , _rsFrom = (fget source)
                                , _rsTo = (fget target)
                                }))
                      msource))
       fss)
  return (Maybe.catMaybes cps)

type RsyncCommand = (Text, FP.FilePath, [Turtle.Line])

asciiHash :: Hashable.Hashable a => a -> Text
asciiHash obj =
  (BSChar8.filter
     (\c -> c /= '=')
     (BSChar8.map
        (\c ->
           case c of
             '+' -> '-'
             '/' -> '_'
             c -> c)
        (Base64.encode (BSLazy.toStrict (Binary.encode (Hashable.hash obj)))))) &
  Encoding.decodeLatin1

sharedCommand ::
     Text -> ((Location, Location), [SharedRelativeCp]) -> RsyncCommand
sharedCommand flags ((Location locFrom, Location locTo), srs) =
  let filesFrom = srs & map ((op RelativePathText) . _rsFile)
      filesFromFilename = "/tmp/files-from/" <> (asciiHash filesFrom)
      linedFilesFrom =
        filesFrom &
        map
          (\f ->
             case (Turtle.textToLine f) of
               Just l -> l
               Nothing -> CE.throw (NewlinesInFilenameException f))
  in ( ("rsync -arv " <> flags <> " --files-from " <> filesFromFilename <> " " <>
        locFrom <>
        " " <>
        locTo)
     , Turtle.fromText filesFromFilename
     , linedFilesFrom)

alteredCommand :: Text -> (AbsPathText, [AlteredRelativeCp]) -> RsyncCommand
alteredCommand flags ((AbsPathText absTo), srs) =
  let filesFrom = srs & map ((op AbsPathText) . _cpFrom)
      filesFromFilename = "/tmp/files-from/" <> (asciiHash filesFrom)
      linedFilesFrom =
        filesFrom &
        map
          (\f ->
             case (Turtle.textToLine f) of
               Just l -> l
               Nothing -> CE.throw (NewlinesInFilenameException f))
  in ( ("rsync -arv " <> flags <> " --files-from " <> filesFromFilename <> " " <>
        "/" <>
        " " <>
        absTo)
     , Turtle.fromText filesFromFilename
     , linedFilesFrom)

matchCopySharedRelative :: CopyEntry -> Maybe SharedRelativeCp
matchCopySharedRelative ce =
  case ce of
    CopySharedRelative sr -> Just sr
    _ -> Nothing

matchCopyAlteredRelative :: CopyEntry -> Maybe AlteredRelativeCp
matchCopyAlteredRelative ce =
  case ce of
    CopyAlteredRelative ar -> Just ar
    _ -> Nothing

groupShared :: [CopyEntry] -> [((Location, Location), [SharedRelativeCp])]
groupShared entries =
  entries & map matchCopySharedRelative & Maybe.catMaybes &
  map (\sr -> ((_rsFrom sr, _rsTo sr), sr)) &
  Extra.groupSort

groupAltered :: [CopyEntry] -> [(AbsPathText, [AlteredRelativeCp])]
groupAltered entries =
  entries & map matchCopyAlteredRelative & Maybe.catMaybes &
  map (\ar -> (_cpTo ar, ar)) &
  Extra.groupSort

-- | returns the rsync command, filename for the includes form and the includes from itself
rsyncCommands :: [(CopyEntry, Bool)] -> [RsyncCommand]
rsyncCommands entries =
  let newShared = entries & (filter (not . snd)) & map fst & groupShared
      updateShared = entries & (filter snd) & map fst & groupShared
      newAltered = entries & (filter (not . snd)) & map fst & groupAltered
      updateAltered = entries & (filter snd) & map fst & groupAltered
  in (newShared & map (sharedCommand "--ignore-existing")) ++
     (updateShared & map (sharedCommand "")) ++
     (newAltered & map (alteredCommand "--ignore-existing")) ++
     (updateAltered & map (alteredCommand ""))

copyEntryAbsTo :: CopyEntry -> AbsPathText
copyEntryAbsTo ce =
  (case ce of
     CopySharedRelative (SharedRelativeCp {_rsFile, _rsFrom, _rsTo}) ->
       AbsPathText (toAbsolute (_rsFile &: _rsTo &: Nil))
     CopyAlteredRelative (AlteredRelativeCp {_cpFrom, _cpTo}) -> _cpTo)

proposeCopyCmdsText :: SQ.Connection -> IO [RsyncCommand]
proposeCopyCmdsText conn = do
  cmds <- proposeCopyCmds conn
  entries <-
    cmds &
    traverse
      (\ce -> do
         exp <- fileExpected conn (copyEntryAbsTo ce)
         return (ce, exp))
  return (rsyncCommands entries)

-- | Writes the files-from files to disk
writeFilesFrom :: [RsyncCommand] -> IO [()]
writeFilesFrom rsCommands =
  traverse
    (\(_, fileName, contents) -> Turtle.output fileName (Turtle.select contents))
    rsCommands

-- | echo the rsync commands
echoRsyncCmds :: [RsyncCommand] -> IO [()]
echoRsyncCmds rsCommands =
  traverse (\(command, _, _) -> Turtle.echo (Turtle.repr command)) rsCommands
