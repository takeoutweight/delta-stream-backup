{-# LANGUAGE TemplateHaskell, DataKinds, TypeOperators, FlexibleContexts, FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ConstraintKinds #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}
-- for labels
{-# LANGUAGE OverloadedLabels, TypeOperators, DataKinds, FlexibleContexts #-}
-- :set -XOverloadedLabels -XTypeOperators -XDataKinds -XFlexibleContexts
{-# LANGUAGE GADTs #-}

module LensExperiments where

-- import Data.Extensible
import Data.Proxy (Proxy(..))
import qualified Control.Lens.TH as LTH
import Control.Lens hiding ((:>), Fold, Identity)
import GHC.Generics
import qualified GHC.Generics as G
import Labels hiding (lens)
import qualified Labels as Labels
import Data.Singletons
import Data.Singletons.TH
import Data.Vinyl
import qualified Data.Vinyl as V
import Data.Vinyl.Lens
import Data.Vinyl.Functor
import qualified Data.Vinyl.Functor as VF
import qualified Data.Vinyl.TypeLevel as VT
import qualified Database.SQLite.Simple as SQ
import GHC.TypeLits
import GHC.Exts (Constraint)
import qualified Data.Vinyl.Functor as V

-- Data.Extensible
-- mkField "field1 field2"

-- type Lens s t a b =
--   forall (f :: * -> *). Functor f => (a -> f b) -> s -> f t
--   	-- Defined in ‘Control.Lens.Type’

-- lens :: Functor f => (s -> a) -> (s -> b -> t) -> (a -> f b) -> s -> f t

data ExtOne r = ExtOne {_extOneName :: String, _extOneAge :: Int, _extOneRest :: r} deriving (Show, Generic)

data ExtTwo r = ExtTwo {_extTwoDate :: String, _extOneEyes :: Int, _extTwoRest :: r} deriving (Show, Generic)

data ExtThree r = ExtThree String r deriving (Show, Generic)

LTH.makeClassy ''ExtOne

-- extOneName :: (Functor f, HasExtOne c r) => (String -> f String) -> c -> f c
-- extOneName :: HasExtOne c r => Lens' c String
-- extOne :: (Functor f, HasExtOne c r) => (ExtOne r -> f (ExtOne r)) -> c -> f c
-- extOne :: HasExtOne c r => Lens' c (ExtOne r)
-- instance HasExtOne (ExtOne r0) r0 where extOne = id
-- fooX = foo . go where go f (Foo x y) = (\x' -> Foo x' y) <$> f x

-- There is maybe an argument I could pass to makeClassy to ignore the
-- polymorphic part?  The trick was to preset a lens to a version with the
-- "rest" hidden via (), Not sure what that signifies.  Somehow I feel its
-- wrong, in that we can "set" with an extone and it just ignores it's rest
-- argument silently?
class HasExtOneB t where
  extOneB :: Lens' t (ExtOne ())
  extOneBName :: Lens' t String
  extOneBName = extOneB . go where go f (ExtOne x y r) = (\x' -> ExtOne x' y r) <$> f x
  extOneBAge :: Lens' t Int
  extOneBAge = extOneB . go where go f (ExtOne x y r) = (\y' -> ExtOne x y' r) <$> f y

instance {-# OVERLAPPING #-} HasExtOneB (ExtOne b) where
  extOneB =
    lens
      (\(ExtOne x y r) -> ExtOne x y ())
      (\(ExtOne x y r) (ExtOne x' y' r') -> ExtOne x' y' r)

instance {-# OVERLAPPING #-} (HasRest g, HasExtOneB a) => HasExtOneB (g a) where
  extOneB = rest . extOneB

-- eg:
test1 = (ExtTwo "hi" 3 (ExtOne "dog" 5 ())) ^. extOneBName
-- Outermost wins
test2 = (ExtOne "hi" 3 (ExtOne "dog" 5 ())) ^. extOneBName

-- Variable ‘r’ occurs more often on LHS than in head (undecidable)
-- instance (HasRestB (g r) r, HasExtOneB r) => HasExtOneB (g r) where
--   extOneB = undefined

-- "pointfree" style?
class HasRest (g :: * -> *) where
  rest :: Lens' (g a) a

instance HasRest ExtOne where
  rest f (ExtOne x y r) = (\r' -> ExtOne x y r') <$> f r

instance HasRest ExtTwo where
  rest f (ExtTwo x y r) = (\r' -> ExtTwo x y r') <$> f r
  
class HasRestB c r | c -> r where
  restb :: Lens' c r

instance HasRestB (ExtOne r) r where
  restb f (ExtOne x y r) = (\r' -> ExtOne x y r') <$> f r

-- Variable ‘c’ occurs more often in the constraint ‘HasExtOne c r’ than in the instance head
-- instance (HasRest f, HasExtOne c r) => HasExtOne (f r) r where
--   extOne = rest . extOne

-- Couldn't match type ‘r’ with ‘f r’ -- we get HasExtOne (f r) (f r) from use of extOne?
-- instance (HasRest g, HasExtOne (f r) r) => HasExtOne (g (f r)) (f r) where
--   extOne = rest . extOne

-- instance (HasRest g, HasExtOne (f r) r) => HasExtOne (g (f r)) (f r) where
--   extOne f g = rest .~ (f ((g ^. rest) ^. extOne))  --  rest . extOne -- S = (g (f r)), A = ExtOne (f r) -- 

-- this is following the type of rest . extOne, but the problem is (g a) doesn't determine r
-- instance (HasRest g, HasExtOne a r) => HasExtOne (g a) r where
--   extOne = undefined

-- Tweaking the above, but get: Functional dependencies conflict between
-- instance declarations: The relationship between the two arguments doesn't
-- agree, i.e. the r isn't the argument to g like it is w/ the builtin
-- HasExtOne. Could I get around this by not having the default instance?  I
-- guess the instance is too general, it overlaps. BUT if I could have the
-- polymorphic part taken out of the classy declaration would we be OK?
-- The OVERLAPPING Doesn't help

-- instance {-# OVERLAPPING #-} (HasRest g, HasExtOne (f r) r) => HasExtOne (g (f r)) r where
--   extOne = undefined

-- :t rest . extOne
-- (HasExtOne a r, Functor f, HasRest g) =>
--     (ExtOne r -> f (ExtOne r)) -> g a -> f (g a)
-- a = ExtOne r
-- b = ExtOne r
-- s = g a
-- t = g a
-- i.e. Lens' (g a) (ExtOne r)
-- i.e. rest . extOne :: (HasRest g, HasExtOne a r) => Lens' (g a) (ExtOne r)
-- so maybe would this correspond to (HasRest g, HasExtOne (ExtOne r) r) => HasExtOne (g (ExtOne r)) (ExtOne r)
-- No, that's not right - that means "the polymorphic "rest" of the ExtOne is, itself, an ExtOne"

-- Needed to reload Backup.hs to get this to work?
-- labelthing = (#foo := "hi", #bar := 123)

data FieldOne = FieldOne String

data FieldTwo = FieldTwo Int

class Hass t where
  type Ret t :: *
  gett :: t -> Ret t

instance {-# OVERLAPPING #-} Hass (FieldOne, r) where
  type Ret (FieldOne, r) = FieldOne
  gett = fst

-- conflicting family instance declaration (same as fundep problem)
-- instance {-# OVERLAPPING #-} Hass b => Hass (a, b) where
--   type Ret (a, b) = Ret b
--   gett = gett . snd

-- Maybe related to overlapping type family instances? (don't work w/ associated types though?)

-- Playing with vinyl, copying a lot of `frames` stuff

type Record = Rec V.Identity

-- | A column's type includes a textual name and the data type of each
-- element.
newtype (:->) (s::Symbol) a = Col { getCol :: a }
  deriving (Eq,Ord,Num,Monoid,Real,RealFloat,RealFrac,Fractional,Floating)

-- | Add a column to the head of a row.
frameCons :: Functor f => f a -> V.Rec f rs -> V.Rec f (s :-> a ': rs)
frameCons = (V.:&) . fmap Col
{-# INLINE frameCons #-}

-- | A @cons@ function for building 'Record' values.
(&:) :: a -> Record rs -> Record (s :-> a ': rs)
x &: xs = frameCons (Identity x) xs
infixr 5 &:

pattern Nil :: Rec f '[]
pattern Nil = V.RNil

data VFields = VName | VAge | VSleeping | VMaster deriving Show

genSingletons [ ''VFields ]

myget p r = (rget p r) & getIdentity & getCol

-- :t 1 &: 2 &: Nil
-- :t (1 &: 2 &: Nil) :: Record '["dog" :-> Int, "cat" :-> Int]
-- This gets out the "pair" "dog" :-> Int, then fetches the value -- seems kind of heavy? But you can name them
-- let (V.Identity (Col r)) = (rget (Proxy :: Proxy ("dog" :-> Int)) ((1 &: 2 &: Nil) :: Record '["dog" :-> Int, "cat" :-> Int]))
-- let dog = (Proxy :: Proxy ("dog" :-> Int))
-- myget dog ((1 &: 2 &: Nil) :: Record '["dog" :-> Int, "cat" :-> Int])

type MHas e rs = RElem e rs (VT.RIndex e rs)

-- Lightweight to define but printing is kind of ugly w/ the "unPath" noise
newtype ConnectionF = ConnectionF String deriving Show
newtype PathF = PathF String deriving Show

-- Weird ghci has Record as output but not the "MHas" as input. Maybe ghci doesn't collapse constraint synonyms?
-- mget :: MHas b rs => s b -> Record rs -> b
-- The downside is if things in there are polymorphic you won't be able to pull them out easily - you need to monomorphize.
mget rs = (rget Proxy rs) & getIdentity

-- Can let the type pick it out. Neat!

-- let (ConnectionF a) = mget example

example =  ((Identity (ConnectionF "ho")) :& (Identity (PathF "hi")) :& V.RNil)

-- eg rget (Proxy :: Proxy PathF) ((Identity (ConnectionF "ho")) :& (Identity (PathF "hi")) :& V.RNil)
-- type is a bit verbose: :t \a -> (rget (Proxy :: Proxy PathF) a, rget (Proxy :: Proxy ConnectionF) a)
-- but can smiplify like :
-- (\a -> (rget (Proxy :: Proxy PathF) a, rget (Proxy :: Proxy ConnectionF) a))
--   :: (MHas ConnectionF rs,
--       MHas PathF rs) =>
--      Rec f rs -> (f PathF, f ConnectionF)
-- Might be able to pull out the f too If I never need it.


-- Trying to have constraints on Records instead of the record param [*]

-- This doesn't let you use "r" as a type, you have to use Record r -> - Not sure there'as a way around that?
-- could do like Has SQ.Connection r0, r ~ Record r0, but not sure if you can stack constraints on the record instead of the record parameter.
type Ha0s e rs = RElem e rs (VT.RIndex e rs)

-- Doesn't work, guess you can't introduce hidden params w/ a constraint synonym? They must be monotype.
-- type Has2 e r = forall rs. (RElem e rs (VT.RIndex e rs), r ~ Record rs)

-- This is me trying to allow constraints on a Record, instead of the Record's argument. But it seems undecidable.
class Has4 e r where
  type Has4C (e :: *) (rs :: [*]) :: Constraint
  get4 :: (Has4C e rs, r ~ record rs) => r -> e

-- instance Has4 e (Record rs) where
--   type Has4C e rs = (RElem e rs (VT.RIndex e rs))
--   get4 = undefined

type family Has5 (el :: *) (r :: *) :: Constraint where
  Has5 e (Record (rs :: [*])) = (RElem e rs (VT.RIndex e rs))

-- This doesn't work -- can't deduce r ~ Record r0 so don't have the quantification I want.
-- But not sure I want this anyway?  I want the parameters for row typing anyway.
-- get5 :: Has5 e r => r -> e
-- get5 r = (rget Proxy r) & VF.getIdentity

-- This works if you specify the constraints explicitly, but it can't figure out the cons case :(
-- This is probably weird but o/w the raw RElem and RIndex constraints are exposed in lib code types.
-- Probably my closest attempt
class (RElem e rs (VT.RIndex e rs)) => Has6 e rs where
  get6 :: Record rs -> e
  get6 rs = (rget Proxy rs) & VF.getIdentity

-- undecidable
-- instance (VT.RIndex r (s : rs) ~ VT.S i, RElem r rs i) => Has e (s : rs) where
--   get = undefined

--- Q: Can we have calculated values in our records? I.e. lenses to values that aren't really there but affect other values? Thinking of eg getting the sql version of a Bool or SUM type. Annoying to have to store those directly.
