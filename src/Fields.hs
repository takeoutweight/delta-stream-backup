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
import qualified Database.SQLite.Simple as SQ
import GHC.Exts (Constraint)
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

newtype DBPath = DBPath String deriving (Show, Generic)
instance Wrapped DBPath

newtype Server = Server Text deriving (Show, Generic)
instance Wrapped Server

-- | Location is the path to the root, prepending AbsPath
newtype Location = Location Text deriving (Show, Generic, Eq)
instance Wrapped Location

newtype AbsPath = AbsPath FilePath deriving (Show, Generic)
instance Wrapped AbsPath

-- | Because conversion of paths to text can fail
newtype AbsPathText = AbsPathText Text deriving (Show, Generic)
instance Wrapped AbsPathText

-- | Relative to the root of the location
newtype RelativePathText = RelativePathText Text deriving (Show, Generic)
instance Wrapped RelativePathText

newtype Filename = Filename Text deriving (Show, Generic)
instance Wrapped Filename

newtype Rechecksum = Rechecksum (Maybe UTCTime -> Maybe UTCTime -> Bool) deriving (Generic)
instance Wrapped Rechecksum

-- | Null CheckTime means we know the expected hash but we've never checked
newtype CheckTime = CheckTime (Maybe UTCTime) deriving (Show, Generic)
instance Wrapped CheckTime

newtype ModTime = ModTime UTCTime deriving (Show, Generic, Eq)
instance Wrapped ModTime

newtype FileSize = FileSize Int deriving (Show, Generic, Eq)
instance Wrapped FileSize

newtype Checksum = Checksum Text deriving (Show, Generic, Eq)
instance Wrapped Checksum

newtype FileInfoIdText = FileInfoIdText Text deriving (Show, Generic)
instance Wrapped FileInfoIdText

newtype EventNumber = EventNumber Int deriving (Show, Generic)
instance Wrapped EventNumber

newtype Deleted = Deleted Bool deriving (Show, Generic)
instance Wrapped Deleted

-- | Where the text is the key id used. This is only if the SYSTEM is handling the encryption. It won't detect files that happen to be encrypted on their own.
data IsEncrypted = Encrypted Text | Unencrypted deriving (Show, Generic, Eq)

newtype FileStateIdF = FileStateIdF (Maybe Int) deriving (Show, Generic)
instance Wrapped FileStateIdF

-- | Each location has its own sequence counter
newtype SequenceNumber = SequenceNumber Int deriving (Show, Generic)
instance Wrapped SequenceNumber

data Provenance = Mirrored Int | Ingested deriving (Show, Generic)

-- | NonCanonical means this file/hash is not meant to be propagated. It possibly represents corrupted data.
data Canonical = NonCanonical | Canonical deriving (Show, Generic)

-- | Actual means the record reflects the most current understanding of the real contents of the filesystem.
data Actual = Historical | Actual  deriving (Show, Generic)

type HasFileDetails rs = (Has ModTime rs, Has FileSize rs, Has Checksum rs, Has IsEncrypted rs)

newtype FileDetailsR =
  FileDetailsR (Maybe (Record '[ ModTime, FileSize, Checksum, IsEncrypted]))
  deriving (Show, Generic, Eq)
instance Wrapped FileDetailsR
