{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}

module Backup where

import Control.Exception
import qualified Control.Foldl as F
import Control.Monad
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

-- ascii armour'd:
-- stdout $ (inproc "gpg" ["-a", "-r", "Nathan Sorenson (SFU)", "--encrypt"] (select ["hello"]))
someFunc :: IO ()
someFunc = putStrLn "someFunc"
