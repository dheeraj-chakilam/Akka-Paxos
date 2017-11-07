module ChatServer.Network

open System.Text
open System.Net
open Akka.FSharp
open Akka.IO
open System.Diagnostics

open Acceptor
open Replica
open Leader

type Server =
    | ChatServer
    | MasterServer

let sw =
    let sw = Stopwatch()
    sw.Start()
    sw

let handler replica acceptor leader serverType selfID connection (mailbox: Actor<obj>) =  
    let rec loop connection = actor {
        let! msg = mailbox.Receive()

        match msg with
        | :? Tcp.Received as received ->
            //In case we receive multiple messages (delimited by a newline) in the same Tcp.Received message
            let lines = (Encoding.ASCII.GetString (received.Data.ToArray())).Trim().Split([|'\n'|])
            Array.iter (fun (line:string) ->
                let data = line.Split([|' '|], 2)

                match data with
                | [| "heartbeat"; message |] ->
                    replica <! Heartbeat (message.Trim(), mailbox.Self, sw.ElapsedMilliseconds)

                | [| "quit" |] ->
                    replica <! Leave mailbox.Self
                    mailbox.Context.Stop mailbox.Self
            
                | _ ->
                    connection <! Tcp.Write.Create (ByteString.FromString <| sprintf "Invalid request. (%A)\n" data)) lines
    
        | :? Tcp.ConnectionClosed as closed ->
            replica <! Leave mailbox.Self
            mailbox.Context.Stop mailbox.Self

        | :? string as response ->
            connection <! Tcp.Write.Create (ByteString.FromString (response + "\n"))

        | _ -> mailbox.Unhandled()

        return! loop connection
    }

    match serverType with
    | ChatServer ->
        replica <! ReplicaMessage.Join mailbox.Self
        leader <! LeaderMessage.Join mailbox.Self
    | MasterServer ->
        replica <! ReplicaMessage.JoinMaster mailbox.Self
    
    loop connection

let server replica acceptor leader serverType port selfID max (mailbox: Actor<obj>) =
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()
        
        match msg with
        | :? Tcp.Bound as bound ->
            printf "Listening on %O\n" bound.LocalAddress

        | :? Tcp.Connected as connected -> 
            printf "%O connected to the server\n" connected.RemoteAddress
            let handlerName = "handler_" + connected.RemoteAddress.ToString().Replace("[", "").Replace("]", "")
            let handlerRef = spawn mailbox handlerName (handler replica acceptor leader serverType selfID sender)
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