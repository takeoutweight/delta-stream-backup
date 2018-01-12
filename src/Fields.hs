{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE DeriveGeneric #-}
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

(&:) :: r -> Record rs -> Record (r : rs)
e &: rs = fcons e rs
infixr 5 &:

-- our fields

newtype DBPath = DBPath String deriving (Show, Generic)
instance Wrapped DBPath

newtype Archive = Archive Text deriving (Show, Generic)
instance Wrapped Archive

newtype Remote = Remote Text deriving (Show, Generic)
instance Wrapped Remote

newtype MasterRemote = MasterRemote Bool deriving (Show, Generic)
instance Wrapped MasterRemote

newtype Root = Root FilePath deriving (Show, Generic)
instance Wrapped Root

newtype AbsPath = AbsPath FilePath deriving (Show, Generic)
instance Wrapped AbsPath

-- | Because conversion of paths to text can fail
newtype AbsPathText = AbsPathText Text deriving (Show, Generic)
instance Wrapped AbsPathText

newtype RelativePathText = RelativePathText Text deriving (Show, Generic)
instance Wrapped RelativePathText

newtype Filename = Filename Text deriving (Show, Generic)
instance Wrapped Filename

newtype Rechecksum = Rechecksum (UTCTime -> Maybe UTCTime -> Bool) deriving (Generic)
instance Wrapped Rechecksum

newtype StatTime = StatTime UTCTime deriving (Show, Generic)
instance Wrapped StatTime

newtype ModTime = ModTime UTCTime deriving (Show, Generic)
instance Wrapped ModTime

newtype FileSize = FileSize Int deriving (Show, Generic)
instance Wrapped FileSize

newtype Checksum = Checksum Text deriving (Show, Generic)
instance Wrapped Checksum

newtype FileInfoIdText = FileInfoIdText Text deriving (Show, Generic)
instance Wrapped FileInfoIdText

newtype EventNumber = EventNumber Int deriving (Show, Generic)
instance Wrapped EventNumber

newtype Deleted = Deleted Bool deriving (Show, Generic)
instance Wrapped Deleted

newtype EncryptionKey = EncryptionKey Text deriving (Show, Generic)
instance Wrapped EncryptionKey

newtype Superceded = Superceded Bool deriving (Show, Generic)
instance Wrapped Superceded
