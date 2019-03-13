{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}

module Fields where

import Control.Lens ((&))
import Control.Lens.Wrapped (Wrapped(..), op)
import Composite.Aeson
       (aesonJsonFormat, defaultJsonFormatRecord, field, field',
        recordJsonFormat)
import qualified Composite.Aeson as CAS
import qualified Data.HashMap.Strict as HM
import qualified Data.Aeson as AS
import Data.Vinyl
import Data.Proxy (Proxy(..))
import qualified Data.Text as T
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import qualified Data.Functor.Identity as DFI
import qualified Data.Typeable as DT
import qualified Data.Vinyl.Functor as VF
import qualified Data.Vinyl.TypeLevel as VT
import GHC.Generics (Generic)
import Turtle (FilePath)
import Prelude hiding (FilePath)

-- type Record = Rec VF.Identity
-- getIdentity = VF.getIdentity
-- setIdentity = VF.Identity

-- had to move to this Identity to work w/ composite aeson stuff?
-- but I think it screwed up my show instance as it shows "Identity" now for each field
type Record = Rec DFI.Identity
getIdentity = DFI.runIdentity
setIdentity = DFI.Identity



pattern Nil :: Rec f '[]
pattern Nil = RNil

type Has e rs = RElem e rs (VT.RIndex e rs)

-- | Pull from a record, where destination type picks the field
fget :: Has e rs => Record rs -> e
fget rs = (rget Proxy rs) & getIdentity

-- | Get unwrapped value by specifying the newtype wrapper
nget ::
     (Wrapped e, Has e rs)
  => (Unwrapped e -> e)
  -> Record rs
  -> Unwrapped e
nget ntc r = op ntc (fget r)

fcons :: r -> Record rs -> Record (r : rs)
fcons e rs = (setIdentity e) :& rs

fappend :: Record as -> Record bs -> Record (as VT.++ bs)
fappend = rappend

fcast :: RSubset rs ss (VT.RImage rs ss) => Record ss -> Record rs
fcast = rcast

(&:) :: r -> Record rs -> Record (r : rs)
e &: rs = fcons e rs
infixr 5 &:

newtype DBPath = DBPath String deriving (Show, Generic)
instance Wrapped DBPath

-- | Location is a place where a tree of files canbe located (i.e. not the
-- location of a particular file). It is a hostname and a path to the root, like
-- Nathans-MacBook-Pro-2.local/Users/nathan/backup/loc1/. Prepended w/o host to
-- RelativePath for an absolute path.
newtype Location = Location Text deriving (Show, Generic, Eq, Ord)
instance Wrapped Location

instance forall a rs. (DT.Typeable a, CAS.RecordToJsonObject rs) => CAS.RecordToJsonObject (a : rs) where
  recordToJsonObject (CAS.ToField aToField :& fs) (DFI.Identity loc :& as) =
    maybe id (HM.insert (T.pack (show (DT.typeRep (Proxy :: Proxy a))))) (aToField loc) $
      CAS.recordToJsonObject fs as

instance forall a rs. (DT.Typeable a, CAS.RecordFromJson rs) => CAS.RecordFromJson (a : rs) where
  recordFromJson (CAS.FromField aFromField :& fs) =
    (:&) <$> (DFI.Identity <$> aFromField (T.pack (show (DT.typeRep (Proxy :: Proxy a))))) <*>
    CAS.recordFromJson fs

instance ( Wrapped a
         , CAS.DefaultJsonFormat (Unwrapped a)
         , CAS.DefaultJsonFormatRecord rs
         ) =>
         CAS.DefaultJsonFormatRecord (a : rs) where
  defaultJsonFormatRecord = field CAS.defaultJsonFormat :& defaultJsonFormatRecord

-- | AbsPath is the entire real filesystem path (path to location root in
-- Location + relative path from that location root).  What command line tools
-- can use to refer to the file in a particular location.
newtype AbsPath = AbsPath FilePath deriving (Show, Generic)
instance Wrapped AbsPath

-- | Because conversion of paths to text can fail.
newtype AbsPathText = AbsPathText Text deriving (Show, Generic, Eq, Ord)
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
instance {-# OVERLAPS #-} (CAS.DefaultJsonFormatRecord rs) => CAS.DefaultJsonFormatRecord (CheckTime : rs) where
  defaultJsonFormatRecord = field (CAS.maybeJsonFormat CAS.iso8601DateTimeJsonFormat) :& defaultJsonFormatRecord

newtype ModTime = ModTime UTCTime deriving (Show, Generic, Eq)
instance Wrapped ModTime
instance {-# OVERLAPS #-} (CAS.DefaultJsonFormatRecord rs) => CAS.DefaultJsonFormatRecord (ModTime : rs) where
  defaultJsonFormatRecord = field CAS.iso8601DateTimeJsonFormat :& defaultJsonFormatRecord

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
instance AS.ToJSON IsEncrypted
instance AS.FromJSON IsEncrypted
instance {-# OVERLAPS #-} (CAS.DefaultJsonFormatRecord rs) => CAS.DefaultJsonFormatRecord (IsEncrypted : rs) where
  defaultJsonFormatRecord = field' aesonJsonFormat :& defaultJsonFormatRecord

newtype FileStateIdF = FileStateIdF Int  deriving (Show, Generic)
instance Wrapped FileStateIdF

-- | Each location has its own sequence counter
newtype SequenceNumber = SequenceNumber Int deriving (Show, Generic)
instance Wrapped SequenceNumber

-- | Did this entry represent a mirror from somewhere else? Or straight-from-the-filesystem?
data Provenance = Mirrored Int | Ingested deriving (Show, Generic)
instance AS.ToJSON Provenance
instance AS.FromJSON Provenance
-- This doesn't help, as Provenance isn't Wrapped
-- instance CAS.DefaultJsonFormat Provenance where defaultJsonFormat = aesonJsonFormat

-- This causes overlapping instances. This seems more specific but idk?? Maybe type constructors obscure that? Is this OK?
instance {-# OVERLAPS #-} (CAS.DefaultJsonFormatRecord rs) => CAS.DefaultJsonFormatRecord (Provenance : rs) where
  defaultJsonFormatRecord = field' aesonJsonFormat :& defaultJsonFormatRecord

-- | NonCanonical means this file/hash is not meant to be propagated. It possibly represents corrupted data.
data Canonical = NonCanonical | Canonical deriving (Show, Generic)
instance AS.ToJSON Canonical
instance AS.FromJSON Canonical
instance {-# OVERLAPS #-} (CAS.DefaultJsonFormatRecord rs) => CAS.DefaultJsonFormatRecord (Canonical : rs) where
  defaultJsonFormatRecord = field' aesonJsonFormat :& defaultJsonFormatRecord

-- | Actual means the record reflects the most current understanding of the real contents of the filesystem.
data Actual = Historical | Actual  deriving (Show, Generic)
instance AS.ToJSON Actual
instance AS.FromJSON Actual
instance {-# OVERLAPS #-} (CAS.DefaultJsonFormatRecord rs) => CAS.DefaultJsonFormatRecord (Actual : rs) where
  defaultJsonFormatRecord = field' aesonJsonFormat :& defaultJsonFormatRecord

type HasFileDetails rs = (Has ModTime rs, Has FileSize rs, Has Checksum rs, Has IsEncrypted rs)

newtype FileDetailsR =
  FileDetailsR (Maybe (Record '[ ModTime, FileSize, Checksum, IsEncrypted]))
  deriving (Show, Generic, Eq)
instance Wrapped FileDetailsR

instance CAS.DefaultJsonFormat (Record '[ ModTime, FileSize, Checksum, IsEncrypted]) where
  defaultJsonFormat = recordJsonFormat defaultJsonFormatRecord
