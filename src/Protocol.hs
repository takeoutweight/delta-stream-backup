{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}

module Protocol where

import qualified Composite.Aeson as CAS
import Control.Lens.Wrapped (Wrapped(..), op)
import qualified Data.Aeson as AS
import qualified Data.Aeson.BetterErrors as ASBE
import qualified Data.Aeson as AS
import qualified Data.Text as T
import qualified Data.Time.Clock as DTC
import qualified Database.SQLite.Simple as SQ
import GHC.Generics (Generic)
import qualified Network.HTTP.Client as HTTP
import Servant ((:<|>), (:>))
import qualified Servant as SV
import qualified Servant.Client as SVC
import qualified Network.Wai.Handler.Warp as Warp
import Prelude hiding (FilePath, head)
import qualified Turtle as Turtle

import Fields
import Query
import MirrorChanges

newtype FileStateJson = FileStateJson FileStateF deriving (Show, Generic)
instance Wrapped FileStateJson

fileStateFormat :: CAS.JsonFormat e FileStateF
fileStateFormat = CAS.recordJsonFormat CAS.defaultJsonFormatRecord

-- instance CAS.DefaultJsonFormat FileStateF where
--   defaultJsonFormat = fileStateFormat

instance AS.ToJSON FileStateJson where
  toJSON fs = (CAS.toJsonWithFormat (CAS.wrappedJsonFormat fileStateFormat) fs)

instance AS.FromJSON FileStateJson where
  parseJSON = CAS.parseJsonWithFormat' (CAS.wrappedJsonFormat fileStateFormat)

-- import Data.ByteString.Lazy as BSL
-- CAS.toJsonWithFormat fileStateFormat (Location "here" &: Nil)
-- AS.encode (CAS.toJsonWithFormat fileStateFormat (Location "here" &: Nil))
-- AS.encode [(FileStateJson fs)]
-- BSL.putStrLn (AS.encode (CAS.toJsonWithFormat CAS.defaultJsonFormat fs))
-- let Just val = (AS.decode "{\"Location\":\"here\"}" :: Maybe Value)
-- (AS.decode "{\"Location\":\"here\"}" :: Maybe Value) & fmap (ASBE.parseValue (CAS.fromJsonWithFormat fileStateFormat))
-- let Right val = (ASBE.parse (CAS.fromJsonWithFormat fileStateFormat) "{\"FileStateIdF\":3, \"Location\":\"here\"}")


type ServerAPI
   = "api" :> "mirrorFileStates" :> SV.ReqBody '[ SV.JSON] [FileStateJson] :> SV.Post '[ SV.JSON] Int

serverAPI :: SV.Proxy ServerAPI
serverAPI = SV.Proxy

mirrorFileStatesHandler :: [FileStateJson] -> SV.Handler Int
mirrorFileStatesHandler inList = do let a = (map (\(FileStateJson fs) -> (nget SequenceNumber fs)) inList)
                                    return 3

server :: SV.Server ServerAPI
server = mirrorFileStatesHandler

-- [{"SequenceNumber":1,"Canonical":"Canonical","Location":"Nathans-MacBook-Pro-2.local/Users/nathan/Pictures","AbsPathText":"/Users/nathan/Pictures/2013/2013-05-15/hoek.png","Actual":"Actual","RelativePathText":"2013/2013-05-15/hoek.png","FileStateIdF":1,"CheckTime":"2019-03-15T06:23:58.668+0000","FileDetailsR":{"IsEncrypted":{"tag":"Unencrypted"},"Checksum":"61fce871b635b32957b3c8c4e3c523eb2b2ac58f","FileSize":37939,"ModTime":"2013-05-15T14:52:32.000+0000"},"Provenance":{"tag":"Ingested"},"Filename":"hoek.png"}]
-- Warp.run 8081 application
-- curl -X POST -d '[]' -H 'Accept: application/json' -H 'Content-type: application/json' http://localhost:8081/api/mirrorFileStates

application :: SV.Application
application = SV.serve serverAPI server

-- sendChanges :: SQ.Connection -> Location -> Location -> IO ()

mirrorFileStatesCall :: [FileStateJson] -> SVC.ClientM Int
mirrorFileStatesCall = SVC.client serverAPI

-- TODO Refactor w.r.t. MirrorChanges.mirrorChangesFromLocation
mirrorFileRequest :: SQ.Connection -> Location -> Location -> IO ()
mirrorFileRequest conn source@(Location sourceT) target@(Location targetT) = do
  now <- DTC.getCurrentTime
  lastSeq <- getLastRequestSourceSequence conn source target
  changes <- getChangesSince conn lastSeq source
  manager' <- HTTP.newManager HTTP.defaultManagerSettings
  nextSeq <-
    SVC.runClientM
      (mirrorFileStatesCall (map FileStateJson changes))
      (SVC.mkClientEnv manager' (SVC.BaseUrl SVC.Http "localhost" 8081 ""))
  case nextSeq of
    Left err -> Turtle.echo (Turtle.repr (show err))
    Right seq -> do
      SQ.execute
        conn
        "UPDATE requests SET active = 0 WHERE active = 1 AND source_location = ? AND target_location = ?"
        (sourceT, targetT)
      SQ.execute
        conn
        "INSERT INTO requests (source_location, target_location, source_sequence, check_time, active) VALUES (?,?,?,?,1)"
        (sourceT, targetT, seq, now)
