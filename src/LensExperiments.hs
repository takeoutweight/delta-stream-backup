{-# LANGUAGE TemplateHaskell, DataKinds, TypeOperators, FlexibleContexts, FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# OPTIONS_GHC -fno-warn-unticked-promoted-constructors #-}

import Data.Extensible
import qualified Control.Lens.TH as LTH
import Control.Lens hiding ((:>), Fold)


mkField "field1 field2"

data ExtOne r = ExtOne {_extOneName :: String, _extOneAge :: Int, _extOneRest :: r}

LTH.makeClassy ''ExtOne

-- extOneName :: (Functor f, HasExtOne c r) => (String -> f String) -> c -> f c
-- extOneName :: HasExtOne c r => Lens' c String
-- extOne :: (Functor f, HasExtOne c r) => (ExtOne r -> f (ExtOne r)) -> c -> f c
-- extOne :: HasExtOne c r => Lens' c (ExtOne r)
-- instance HasExtOne (ExtOne r0) r0 where extOne = id
-- fooX = foo . go where go f (Foo x y) = (\x' -> Foo x' y) <$> f x

-- There is maybe an argument I could pass to makeClassy to ignore the
-- polymorphic part?  but I now can't mention a polymorphic variable in extOneB
-- for "don't care" because it means you have to give a lense to EVERY type (i.e
-- if it's not introduced in the instance head?) So maybe mentioning it is unavoidable
class HasExtOneB t where
  extOneB :: Lens' t (ExtOne ())
  extOneBName :: Lens' t String
  extOneBName = extOneB . go where go f (ExtOne x y r) = (\x' -> ExtOne x' y r) <$> f x
  extOneBAge :: Lens' t Int
  extOneBAge = extOneB . go where go f (ExtOne x y r) = (\y' -> ExtOne x y' r) <$> f y

-- Doesn't work -- the way this is set up means the lens has to work for any b,
-- not the b that happens to be in the data. So that's not possible.
-- instance HasExtOneB (ExtOne b) where extOneB = id

-- I can't make this work for any argument, as that doesn't match the 
instance HasExtOneB (ExtOne ()) where
  extOneB = lens (\(ExtOne x y r) -> ExtOne x y ()) (\old (ExtOne x y r) -> ExtOne x y ()) 

-- "pointfree" style?
class HasRest (g :: * -> *) where
  rest :: Lens' (g a) a

instance HasRest ExtOne where
  rest f (ExtOne x y r) = (\r' -> ExtOne x y r') <$> f r
  
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

-- instance (HasRest g, HasExtOne (f r) r) => HasExtOne (g (f r)) r where
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

