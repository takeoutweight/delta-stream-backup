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
import qualified Data.Text as T
import qualified Data.Aeson as AS
import GHC.Generics (Generic)
import Servant ((:<|>), (:>))
import qualified Servant as SV
import qualified Network.Wai.Handler.Warp as Warp

import Fields
import Query

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
-- [{"SequenceNumber":1,"Canonical":"Canonical","Location":"Nathans-MacBook-Pro-2.local/Users/nathan/Pictures","AbsPathText":"/Users/nathan/Pictures/2013/2013-05-15/hoek.png","Actual":"Actual","RelativePathText":"2013/2013-05-15/hoek.png","FileStateIdF":1,"CheckTime":"2019-03-15T06:23:58.668+0000","FileDetailsR":{"IsEncrypted":{"tag":"Unencrypted"},"Checksum":"61fce871b635b32957b3c8c4e3c523eb2b2ac58f","FileSize":37939,"ModTime":"2013-05-15T14:52:32.000+0000"},"Provenance":{"tag":"Ingested"},"Filename":"hoek.png"}]


type ServerAPI
   = "api" :> "mirrorFileStates" :> SV.ReqBody '[ SV.JSON] [FileStateJson] :> SV.Post '[ SV.JSON] [Int]



serverAPI :: SV.Proxy ServerAPI
serverAPI = SV.Proxy

mirrorFileStatesHandler :: [FileStateJson] -> SV.Handler [Int]
mirrorFileStatesHandler inList = return (map (\(FileStateJson fs) -> (nget SequenceNumber fs)) inList)

server :: SV.Server ServerAPI
server = mirrorFileStatesHandler


-- {"SequenceNumber":1,"Canonical":"Canonical","Location":"Nathans-MacBook-Pro-2.local/Users/nathan/Pictures","AbsPathText":"/Users/nathan/Pictures/2013/2013-05-15/hoek.png","Actual":"Actual","RelativePathText":"2013/2013-05-15/hoek.png","FileStateIdF":1,"CheckTime":"2019-03-15T06:23:58.668+0000","FileDetailsR":{"IsEncrypted":{"tag":"Unencrypted"},"Checksum":"61fce871b635b32957b3c8c4e3c523eb2b2ac58f","FileSize":37939,"ModTime":"2013-05-15T14:52:32.000+0000"},"Provenance":{"tag":"Ingested"},"Filename":"hoek.png"}
-- Warp.run 8081 application
-- curl -X POST -d '[2, 3, 4]' -H 'Accept: application/json' -H 'Content-type: application/json' http://localhost:8081/api/mirrorFileStates

application :: SV.Application
application = SV.serve serverAPI server

