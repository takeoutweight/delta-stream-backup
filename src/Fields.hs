{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE PatternSynonyms #-}

-- {-# LANGUAGE TypeOperators #-}
-- {-# LANGUAGE DataKinds #-}

module Fields where

import Control.Lens ((&))
import Data.Vinyl
import Data.Proxy (Proxy(..))
import qualified Data.Vinyl.Functor as VF
import qualified Data.Vinyl.TypeLevel as VT
import qualified Database.SQLite.Simple as SQ

type Record = Rec VF.Identity

pattern Nil :: Rec f '[]
pattern Nil = RNil

type Has e rs = RElem e rs (VT.RIndex e rs)

get :: Has e rs => Record rs -> e
get rs = (rget Proxy rs) & VF.getIdentity

rcons :: r -> Record rs -> Record (r : rs)
rcons e rs = (VF.Identity e) :& rs

(&:) :: r -> Record rs -> Record (r : rs)
e &: rs = rcons e rs
infixr 5 &:
