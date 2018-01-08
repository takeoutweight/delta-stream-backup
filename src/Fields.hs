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
import qualified Data.Vinyl.Functor as VF
import qualified Data.Vinyl.TypeLevel as VT
import qualified Database.SQLite.Simple as SQ
import GHC.Exts (Constraint)

type Record = Rec VF.Identity

pattern Nil :: Rec f '[]
pattern Nil = RNil

-- This is probably weird but o/w the raw RElem and RIndex constraints are exposed in lib code types.
class (RElem e rs (VT.RIndex e rs)) => Has e rs where
  get :: Record rs -> e
  get rs = (rget Proxy rs) & VF.getIdentity

rcons :: r -> Record rs -> Record (r : rs)
rcons e rs = (VF.Identity e) :& rs

(&:) :: r -> Record rs -> Record (r : rs)
e &: rs = rcons e rs
infixr 5 &:

-- our fields
  
newtype DBPath = DBPath String deriving Show

newtype Archive = Archive String deriving Show

newtype Remote = Remote String deriving Show

newtype MasterRemote = MasterRemote Bool deriving Show

newtype Root = Root String deriving Show

newtype AbsPath = AbsPath String deriving Show
