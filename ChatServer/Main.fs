﻿module ChatServer.Main

open Akka.FSharp
open Replica
open Acceptor
open Leader
open Network

let beatrate = 10.
let aliveThreshold = 100L
let timeout = 100L
let exitDelay = 10

let config =
    (Akka.Configuration.ConfigurationFactory.ParseString
        @"
        akka {
        suppress-json-serializer-warning = on
        }").WithFallback(Configuration.defaultConfig())

[<EntryPoint>]
let main argv =
    match argv with
    | [|id; n; port|] ->
        let system = System.create "system" config
        let replicaRef = spawn system "replica" (replica id beatrate)
        let acceptorRef = spawn system "acceptor" (acceptor id)
        let leaderRef = spawn system "leader" (leader id)
        let serverRef = spawn system "server" (server replicaRef acceptorRef leaderRef ChatServer (20000 + int id) (int id) (int n))
        let mServerRef = spawn system "master-server" (server replicaRef acceptorRef leaderRef MasterServer (int port) (int id) (int n))
        ()

    | _ -> printfn "Incorrect arguments (%A)" argv
    
    System.Console.ReadLine() |> ignore
    0