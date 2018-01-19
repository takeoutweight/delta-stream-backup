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

module LensExperimentsTwo where

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

import Fields


------ experiments

-- This doesn't typecheck, you have to explicitly monomorphize the record: I
-- guess it sort of makes sense. I know that monomorphizing the Num a to Int
-- solves the problem, but the typechecker wouldn't do that in any but the most
-- simple cases. I.e. if there were two Num a's, which should it monomorphize?

classyTest :: Has Int r => Record r -> Int
classyTest r = fget r

-- classyTest (3 &: Nil)
o1 = classyTest ((3 :: Int) &: Nil)

-- Not sure this can even be called? Get ambituity check error, doesn't unify
-- the two typeclass variables. Maybe for similar reasons as before? I know they
-- line up but maybe the typechecker can't make that assumption?

classyTest2 :: (Has a r, Num a) => Record r -> a
classyTest2 r = fget r

-- classyTest2 (3 &: Nil)
-- classyTest2 ((3 :: Int) &: Nil) -- this typechecks on the repl, but can't assing it a toplevel value
