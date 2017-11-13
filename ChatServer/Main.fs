module ChatServer.Main

open Akka.FSharp
open Replica
open Acceptor
open Leader
open Network

let beatrate = 5000.
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
        let acceptorRef = spawn system "acceptor" (acceptor (int64 id))
        let leaderRef = spawn system "leader" (leader (int64 id) (int n) beatrate)
        let replicaRef = spawn system "replica" (replica (int64 id) beatrate)
        let serverRef = spawn system "server" (server replicaRef acceptorRef leaderRef ChatServer (20000 + int id) (int id) (int n))
        let mServerRef = spawn system "master-server" (server replicaRef acceptorRef leaderRef MasterServer (int port) (int id) (int n))
        ()

    | _ -> printfn "Incorrect arguments (%A)" argv
    
    System.Console.ReadLine() |> ignore
    0