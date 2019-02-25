-- Misc database tables that haven't got their own module

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

module Schema where

import Data.Monoid ((<>))
import qualified Data.String.Combinators as SC
import qualified Database.SQLite.Simple as SQ
import Prelude hiding (FilePath, head)

createFileStateSequenceCounterTable :: SQ.Query
createFileStateSequenceCounterTable =
  "CREATE TABLE IF NOT EXISTS file_state_sequence_counters " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "file_state_sequence_location TEXT PRIMARY KEY"
           , "file_state_sequence_counter INTEGER"
           ])))

-- | Records successful requests
createRequestTable :: SQ.Query
createRequestTable =
  "CREATE TABLE IF NOT EXISTS requests " <>
  (SC.parens
     (mconcat
        (SC.punctuate
           ", "
           [ "request_id INTEGER PRIMARY KEY"
           , "source_location TEXT"
           , "source_sequence INTEGER"
           , "target_location TEXT"
           , "check_time TEXT"
           , "active INTEGER"
           ])))
