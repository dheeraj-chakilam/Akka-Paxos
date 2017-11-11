module ChatServer.Network

open System.Text
open System.Net
open Akka.FSharp
open Akka.IO
open System.Diagnostics

open Acceptor
open Replica
open Leader
open Types

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
                let data = line.Split([|' '|])

                match data with
                | [| "heartbeat"; message |] ->
                    replica <! ReplicaMessage.Heartbeat (message.Trim(), mailbox.Self, sw.ElapsedMilliseconds)
                    leader <! LeaderMessage.Heartbeat (message.Trim(), mailbox.Self, sw.ElapsedMilliseconds)

                | [| "quit" |] ->
                    replica <! ReplicaMessage.Leave mailbox.Self
                    leader <! LeaderMessage.Leave mailbox.Self
                    mailbox.Context.Stop mailbox.Self

                | [| "msg"; messageID; message |] ->
                    replica <! Request { id = int64 messageID ; message = message }
                
                | [| "get"; "chatLog" |] ->
                    replica <! Get
            
                | [| "propose"; slot ; cid ; commandMessage |] ->
                    leader <! Propose (int64 slot, { id = int64 cid ; message = commandMessage })

                | [| "p1a" ; br ; blid |] ->
                    acceptor <! P1A (mailbox.Self, { round = int64 br; leaderID = int64 blid } )

                | [| "p1b" ; "ballot" ; br ; blid ; "pvalues" ; acceptedString |] ->
                    let pvalues =
                        acceptedString.Trim().Split([|'|'|])
                        |> Array.fold (fun state pvalString ->
                            match pvalString.Split([|','|]) with
                            | [| ballotRound; ballotLeaderID; slot; commandId; commandMessage |] ->
                                let pval = 
                                    {
                                        ballot = { round = int64 ballotRound ; leaderID = int64 ballotLeaderID }
                                        slot = int64 slot
                                        command = { id = int64 commandId ; message = commandMessage }
                                    }
                                Set.add pval state
                            | _ -> state) Set.empty
                    leader <! P1b (mailbox.Self, { round = int64 br; leaderID = int64 blid }, pvalues )
                
                 | [| "p2a" ; br ; blid ; slot ; commandId ; commandMessage |] ->
                    let pval = 
                        { 
                            ballot = { round = int64 br; leaderID = int64 blid }
                            slot = int64 slot
                            command = { id = int64 commandId ; message = commandMessage }
                        }
                    acceptor <! P2A (mailbox.Self, pval)
                
                | [| "p2b" ;  br ; blid ; slot |] -> 
                    leader <! P2b (mailbox.Self, { round = int64 br; leaderID = int64 blid }, int64 slot )
                
                | [| "decision"; slot ; cid ; commandMessage |] ->
                    replica <! Decision (int64 slot, { id = int64 cid ; message = commandMessage })

                | _ ->
                    match connection with
                    | Some c -> c <! Tcp.Write.Create (ByteString.FromString <| sprintf "Invalid request. (%A)\n" data)
                    | None -> printf "Invalid request. (%A)\n" data) lines
    
        | :? Tcp.ConnectionClosed as closed ->
            replica <! Leave mailbox.Self
            mailbox.Context.Stop mailbox.Self

        | :? string as response ->
            match connection with
            | Some c -> c <! Tcp.Write.Create (ByteString.FromString (response + "\n"))
            | None -> mailbox.Self <! Tcp.Received(ByteString.FromString (response + "\n"))

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
        
        // This handler gives a reference to the replica, acceptor, leader residing on the self server
        let handlerName = "handler_Self"
        let selfHandlerRef = spawn mailbox handlerName (handler replica acceptor leader serverType selfID None)

        match msg with
        | :? Tcp.Bound as bound ->
            printf "Listening on %O\n" bound.LocalAddress

        | :? Tcp.Connected as connected -> 
            printf "%O connected to the server\n" connected.RemoteAddress
            let handlerName = "handler_" + connected.RemoteAddress.ToString().Replace("[", "").Replace("]", "")
            let handlerRef = spawn mailbox handlerName (handler replica acceptor leader serverType selfID (Some sender))
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