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
testBinary =
  (TB.inproc
     "gpg"
     ["-r", "Nathan Sorenson (SFU)", "--encrypt"]
     (select ["hello"]))

-- TODO Pipe through gpg, and incrementally build the post-compression shasum (w/o loading in memory)
-- folds can be paired with ((,) <$> Fold.minimum <*> Fold.maximum)
-- can writeonly/appendonly to a handle or append to a filepath -- not sure the benefits?
-- maybe generalize w/ :t
appendFold :: MM.MonadManaged m => FilePath -> FoldM m BS.ByteString ()
appendFold fp =
  F.FoldM
    (\h bs -> do
       liftIO (BS.hPut h bs)
       return h)
    (writeonly fp)
    (\h -> return ())

-- liftIO wants IO, not MonadManaged m. So trying this?
appendFold2 handle =
  F.FoldM
    (\_ bs -> do
        liftIO (BS.hPut handle bs)
        return ())
    (return ())
    (\_ -> return ())

-- Q: Any kind of "runManaged" fold? Where we 

-- writeAndChecksum ::
--      MM.MonadManaged m
--   => FilePath
--   -> FoldM m BS.ByteString (CH.Digest CHA.SHA1, ())
writeAndChecksum fp = (appendFold fp) *> F.generalize shasum

writeAndChecksum2
  :: MonadIO m =>
     Handle -> FoldM m BS.ByteString (CH.Digest CHA.SHA1)
writeAndChecksum2 handle = (appendFold2 handle) *> F.generalize shasum

-- Is this what I mean?
-- view (do h <- (writeonly "/tmp/append2.gpg"); foldIO testBinary (writeAndChecksum2 h))

-- ascii armour'd:
-- stdout $ (inproc "gpg" ["-a", "-r", "Nathan Sorenson (SFU)", "--encrypt"] (select ["hello"]))
someFunc :: IO ()
someFunc = putStrLn "someFunc"
