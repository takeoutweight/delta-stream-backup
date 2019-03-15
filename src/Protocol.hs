{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}

module Protocol where

import qualified Data.Text as T
import qualified Data.Aeson as AS
import Servant ((:<|>), (:>))
import qualified Servant as SV
import qualified Network.Wai.Handler.Warp as Warp

type ServerAPI
   = "api" :> "mirrorFileStates" :> SV.ReqBody '[ SV.JSON] [Int] :> SV.Post '[ SV.JSON] Int

serverAPI :: SV.Proxy ServerAPI
serverAPI = SV.Proxy

mirrorFileStatesHandler :: [Int] -> SV.Handler Int
mirrorFileStatesHandler inList = return (length inList)

server :: SV.Server ServerAPI
server = mirrorFileStatesHandler

-- curl -X POST -d '[2, 3, 4]' -H 'Accept: application/json' -H 'Content-type: application/json' http://localhost:8081/api/mirrorFileStates

application :: SV.Application
application = SV.serve serverAPI server
