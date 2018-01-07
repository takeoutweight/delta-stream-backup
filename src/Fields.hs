{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleInstances #-}

module Fields where

import qualified Database.SQLite.Simple as SQ

class HasRest (g :: * -> *) where
  rest :: (g a) -> a

data Connection r = Connection SQ.Connection r

class HasConnection r where
  connection :: r -> SQ.Connection

instance {-# OVERLAPPING #-} HasConnection (Connection r) where
  connection (Connection c _) = c

instance {-# OVERLAPPING #-} (HasRest g, HasConnection a) => HasConnection (g a) where
  connection = connection . rest

instance {-# OVERLAPPING #-} HasRest Connection where
  rest (Connection _ r) = r
