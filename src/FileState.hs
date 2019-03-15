-- Includes the raw Beam code for the FileState table

{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NamedFieldPuns#-}
-- {-# LANGUAGE PartialTypeSignatures #-}
-- {-# LANGUAGE NamedWildCards #-}
-- {-# LANGUAGE TypeSynonymInstances #-}
-- {-# LANGUAGE TypeApplications #-}

module FileState where

import Data.Functor.Identity (Identity)
import Data.Monoid ((<>))
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import qualified Data.String.Combinators as SC
import Database.Beam
import qualified Database.SQLite.Simple as SQ
import GHC.Generics (Generic)
import Prelude hiding (FilePath, head)

data FileStateT f = FileStateT
  { _file_state_id :: Columnar f Int -- Auto
   , _location :: Columnar f Text
   , _sequence_number :: Columnar f Int
   , _check_time :: Columnar (Nullable f) UTCTime
   , _absolute_path :: Columnar f Text
   , _relative_path :: Columnar f Text
   , _filename :: Columnar f Text
   , _deleted :: Columnar f Int
   , _mod_time :: Columnar (Nullable f) UTCTime
   , _file_size :: Columnar (Nullable f) Int
   , _checksum :: Columnar (Nullable f) Text
   , _encrypted :: Columnar (Nullable f) Int
   , _encryption_key_id :: Columnar (Nullable f) Text
   , _actual :: Columnar f Int
   , _canonical :: Columnar f Int
   , _provenance_type :: Columnar f Int
   , _provenance_head_location :: Columnar (Nullable f) Text
   , _provenance_head_sequence :: Columnar (Nullable f) Int
   , _provenance_prev_location :: Columnar (Nullable f) Text
   , _provenance_prev_sequence :: Columnar (Nullable f) Int
  } deriving (Generic)

createFileStateTable :: SQ.Query
createFileStateTable =
  "CREATE TABLE IF NOT EXISTS file_state " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "file_state_id INTEGER PRIMARY KEY"
           , "location TEXT"
           , "sequence_number INTEGER"
           , "check_time TEXT"
           , "absolute_path TEXT"
           , "relative_path TEXT"
           , "filename TEXT"
           , "deleted INTEGER"
           , "mod_time TEXT"
           , "file_size INTEGER"
           , "checksum TEXT"
           , "encrypted INTEGER"
           , "encryption_key_id TEXT"
           , "actual INTEGER"
           , "canonical INTEGER"
           , "provenance_type INTEGER"
           , "provenance_head_location TEXT"
           , "provenance_head_sequence INTEGER"
           , "provenance_prev_location TEXT"
           , "provenance_prev_sequence INTEGER"
           ])))

type FileState = FileStateT Identity

type FileStateId = PrimaryKey FileStateT Identity

deriving instance Show FileStateId

deriving instance Show (PrimaryKey FileStateT (Nullable Identity))

deriving instance Eq FileStateId

deriving instance Eq (PrimaryKey FileStateT (Nullable Identity))

deriving instance Show FileState

deriving instance Eq FileState

instance Beamable FileStateT

instance Table FileStateT where
  data PrimaryKey FileStateT f = FileStateId (Columnar f Int)
                            deriving Generic
  primaryKey = FileStateId . _file_state_id

instance Beamable (PrimaryKey FileStateT)

data FileDB f = FileDB
  { _file_state :: f (TableEntity FileStateT)
  } deriving (Generic)

instance Database be FileDB

fileDB :: DatabaseSettings be FileDB
fileDB = defaultDbSettings
