{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE PatternSynonyms #-}

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE PolyKinds #-}
-- {-# LANGUAGE TypeOperators #-}
-- {-# LANGUAGE DataKinds #-}

module Fields where

import Control.Lens ((&))
import Data.Vinyl
import Data.Proxy (Proxy(..))
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import qualified Data.Vinyl.Functor as VF
import qualified Data.Vinyl.TypeLevel as VT
import qualified Database.SQLite.Simple as SQ
import GHC.Exts (Constraint)
import Turtle (FilePath)
import Prelude hiding (FilePath)

type Record = Rec VF.Identity

pattern Nil :: Rec f '[]
pattern Nil = RNil

type Has e rs = RElem e rs (VT.RIndex e rs)

get rs = (rget Proxy rs) & VF.getIdentity

rcons :: r -> Record rs -> Record (r : rs)
rcons e rs = (VF.Identity e) :& rs

(&:) :: r -> Record rs -> Record (r : rs)
e &: rs = rcons e rs
infixr 5 &:

-- our fields
  
newtype DBPath = DBPath {dbpath :: String} deriving Show

newtype Archive = Archive Text deriving Show

newtype Remote = Remote Text deriving Show

newtype MasterRemote = MasterRemote Bool deriving Show

newtype Root = Root FilePath deriving Show

newtype AbsPath = AbsPath {absPath :: FilePath} deriving Show

newtype Rechecksum = Rechecksum (UTCTime -> Maybe UTCTime -> Bool)
