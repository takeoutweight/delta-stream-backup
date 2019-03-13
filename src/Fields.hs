{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}

module Fields where

import Control.Lens ((&))
import Control.Lens.Wrapped (Wrapped(..), op)
import Data.Vinyl
import Data.Proxy (Proxy(..))
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import qualified Data.Vinyl.Functor as VF
import qualified Data.Vinyl.TypeLevel as VT
import GHC.Generics (Generic)
import Turtle (FilePath)
import Prelude hiding (FilePath)

type Record = Rec VF.Identity

pattern Nil :: Rec f '[]
pattern Nil = RNil

type Has e rs = RElem e rs (VT.RIndex e rs)

-- | Pull from a record, where destination type picks the field
fget :: Has e rs => Record rs -> e
fget rs = (rget Proxy rs) & VF.getIdentity

-- | Get unwrapped value by specifying the newtype wrapper
nget ::
     (Wrapped e, Has e rs)
  => (Unwrapped e -> e)
  -> Record rs
  -> Unwrapped e
nget ntc r = op ntc (fget r)

fcons :: r -> Record rs -> Record (r : rs)
fcons e rs = (VF.Identity e) :& rs

fappend :: Record as -> Record bs -> Record (as VT.++ bs)
fappend = rappend

fcast :: RSubset rs ss (VT.RImage rs ss) => Record ss -> Record rs
fcast = rcast

(&:) :: r -> Record rs -> Record (r : rs)
e &: rs = fcons e rs
infixr 5 &:

-- our fields

newtype DBPath = DBPath String deriving (Show, Read, Generic)
instance Wrapped DBPath

-- | Location is a place where a tree of files canbe located (i.e. not the
-- location of a particular file). It is a hostname and a path to the root, like
-- Nathans-MacBook-Pro-2.local/Users/nathan/backup/loc1/. Prepended w/o host to
-- RelativePath for an absolute path.
newtype Location = Location Text deriving (Show, Read, Generic, Eq, Ord)
instance Wrapped Location

-- | AbsPath is the entire real filesystem path (path to location root in
-- Location + relative path from that location root).  What command line tools
-- can use to refer to the file in a particular location.
newtype AbsPath = AbsPath FilePath deriving (Show, Generic)
instance Wrapped AbsPath

-- | Because conversion of paths to text can fail.
newtype AbsPathText = AbsPathText Text deriving (Show, Read, Generic, Eq, Ord)
instance Wrapped AbsPathText

-- | Relative to the root of the location
newtype RelativePathText = RelativePathText Text deriving (Show, Read, Generic)
instance Wrapped RelativePathText

newtype Filename = Filename Text deriving (Show, Read, Generic)
instance Wrapped Filename

newtype Rechecksum = Rechecksum (Maybe UTCTime -> Maybe UTCTime -> Bool) deriving (Generic)
instance Wrapped Rechecksum

-- | Null CheckTime means we know the expected hash but we've never checked
newtype CheckTime = CheckTime (Maybe UTCTime) deriving (Show, Read, Generic)
instance Wrapped CheckTime

newtype ModTime = ModTime UTCTime deriving (Show, Read, Generic, Eq)
instance Wrapped ModTime

newtype FileSize = FileSize Int deriving (Show, Read, Generic, Eq)
instance Wrapped FileSize

newtype Checksum = Checksum Text deriving (Show, Read, Generic, Eq)
instance Wrapped Checksum

newtype FileInfoIdText = FileInfoIdText Text deriving (Show, Read, Generic)
instance Wrapped FileInfoIdText

newtype EventNumber = EventNumber Int deriving (Show, Read, Generic)
instance Wrapped EventNumber

newtype Deleted = Deleted Bool deriving (Show, Read, Generic)
instance Wrapped Deleted

-- | Where the text is the key id used. This is only if the SYSTEM is handling the encryption. It won't detect files that happen to be encrypted on their own.
data IsEncrypted = Encrypted Text | Unencrypted deriving (Show, Read, Generic, Eq)

newtype FileStateIdF = FileStateIdF Int  deriving (Show, Read, Generic)
instance Wrapped FileStateIdF

-- | Each location has its own sequence counter
newtype SequenceNumber = SequenceNumber Int deriving (Show, Read, Generic)
instance Wrapped SequenceNumber

-- | Did this entry represent a mirror from somewhere else? Or straight-from-the-filesystem?
data Provenance = Mirrored Int | Ingested deriving (Show, Read, Generic)

-- | NonCanonical means this file/hash is not meant to be propagated. It possibly represents corrupted data.
data Canonical = NonCanonical | Canonical deriving (Show, Read, Generic)

-- | Actual means the record reflects the most current understanding of the real contents of the filesystem.
data Actual = Historical | Actual  deriving (Show, Read, Generic)

type HasFileDetails rs = (Has ModTime rs, Has FileSize rs, Has Checksum rs, Has IsEncrypted rs)

newtype FileDetailsR =
  FileDetailsR (Maybe (Record '[ ModTime, FileSize, Checksum, IsEncrypted]))
  deriving (Show, Generic, Eq)
instance Wrapped FileDetailsR
