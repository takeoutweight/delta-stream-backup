{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}

module Backup where

import qualified Control.Foldl as F
import Control.Monad
import qualified Crypto.Hash as CH
import qualified Crypto.Hash.Algorithms as CHA
import qualified Data.ByteString.Lazy as BS
import Prelude hiding (head, FilePath)
import Turtle
import qualified Turtle.Bytes as TB

shasum :: MonadIO io => FilePath -> io (CH.Digest CHA.SHA1)
shasum fp = fold (TB.input fp) (F.Fold CH.hashUpdate (CH.hashInit :: CH.Context CHA.SHA1) CH.hashFinalize)

-- eg
-- view $ (lsFiles ".") >>= shasum
lsFiles dir = do
  f <- ls dir
  s <- stat f
  if (isRegularFile s) then (return f) else empty

someFunc :: IO ()
someFunc = putStrLn "someFunc"
