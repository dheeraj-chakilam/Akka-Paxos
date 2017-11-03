﻿module ChatServer.Network

open System.Text
open System.Net
open Akka.FSharp
open Akka.IO
open Types
open System.Diagnostics

type Server =
    | ChatServer
    | MasterServer

let sw =
    let sw = Stopwatch()
    sw.Start()
    sw

let handler world serverType selfID connection (mailbox: Actor<obj>) =  
    let rec loop connection = actor {
        let! msg = mailbox.Receive()

        match msg with
        | :? Tcp.Received as received ->
            //In case we receive multiple messages (delimited by a newline) in the same Tcp.Received message
            let lines = (Encoding.ASCII.GetString (received.Data.ToArray())).Trim().Split([|'\n'|])
            Array.iter (fun (line:string) ->
                let data = line.Split([|' '|], 2)

                match data with
                | [| "broadcast"; message |] ->
                    world <! Broadcast (message.Trim())

                | [| "rebroadcast"; message |] ->
                    world <! Rebroadcast (message.Trim())
            
                | [| "heartbeat"; message |] ->
                    world <! Heartbeat (message.Trim(), mailbox.Self, sw.ElapsedMilliseconds)

                | [| "quit" |] ->
                    world <! Leave mailbox.Self
                    mailbox.Context.Stop mailbox.Self
            
                | [| "alive" |] ->
                    world <! Alive (sw.ElapsedMilliseconds, string selfID)
            
                | [| "get" |] ->
                    world <! Get
            
                | _ ->
                    connection <! Tcp.Write.Create (ByteString.FromString <| sprintf "Invalid request. (%A)\n" data)) lines
    
        | :? Tcp.ConnectionClosed as closed ->
            world <! Leave mailbox.Self
            mailbox.Context.Stop mailbox.Self

        | :? string as response ->
            connection <! Tcp.Write.Create (ByteString.FromString (response + "\n"))

        | _ -> mailbox.Unhandled()

        return! loop connection
    }

    match serverType with
    | ChatServer -> world <! Join mailbox.Self
    | MasterServer -> world <! JoinMaster mailbox.Self
    
    loop connection

let server world serverType port selfID max (mailbox: Actor<obj>) =
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()
        
        match msg with
        | :? Tcp.Bound as bound ->
            printf "Listening on %O\n" bound.LocalAddress

        | :? Tcp.Connected as connected -> 
            printf "%O connected to the server\n" connected.RemoteAddress
            let handlerName = "handler_" + connected.RemoteAddress.ToString().Replace("[", "").Replace("]", "")
            let handlerRef = spawn mailbox handlerName (handler world serverType selfID sender)
            sender <! Tcp.Register handlerRef

        | _ -> mailbox.Unhandled()

        return! loop()
    }

    mailbox.Context.System.Tcp() <! Tcp.Bind(mailbox.Self, IPEndPoint(IPAddress.Any, port),options=[Inet.SO.ReuseAddress(true)])

    if serverType = ChatServer then
        let clientPortList = seq {0 .. max} |> Seq.filter (fun n -> n <> int selfID) |> Seq.map (fun n -> 20000 + n)
        for p in clientPortList do
            mailbox.Context.System.Tcp() <! Tcp.Connect(IPEndPoint(IPAddress.Loopback, p),options=[Inet.SO.ReuseAddress(true)])

    loop()