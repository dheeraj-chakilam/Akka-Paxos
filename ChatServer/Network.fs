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
open System

type Server =
    | ChatServer
    | MasterServer

let sw =
    let sw = Stopwatch()
    sw.Start()
    sw

let handler replica acceptor leader serverType selfID connection (mailbox: Actor<obj>) =  

    let sendInvalidReq data =
        match connection with
                    | Some c -> c <! Tcp.Write.Create (ByteString.FromString <| sprintf "Invalid request. (%A)\n" data)
                    | None -> printf "Invalid request. (%A)\n" data
    
    let parseIdSet (idListString: string) =
        idListString.Trim().Split([|' '|])
        |> Set.ofArray
        |> Set.map (fun id -> int64 id)

    let rec loop connection = actor {
        let! msg = mailbox.Receive()

        match msg with
        | :? Tcp.Received as received ->
            //In case we receive multiple messages (delimited by a newline) in the same Tcp.Received message
            let lines = (Encoding.ASCII.GetString (received.Data.ToArray())).Trim().Split([|'\n'|])
            Array.iter (fun (line:string) ->
                let data = line.Split([|' '|], 2)

                match data with
                | [| "heartbeat" ; message |] ->
                    leader <! LeaderMessage.Heartbeat (int64 (message.Trim()), mailbox.Self)

                | [| "quit" |] ->
                    replica <! ReplicaMessage.Leave mailbox.Self
                    leader <! LeaderMessage.Leave mailbox.Self
                    mailbox.Context.Stop mailbox.Self

                | [| "msg"; rest |] ->
                    match rest.Trim().Split([|' '|]) with
                    | [| messageID; message |] ->
                        //printfn "Received message: %s" message
                        replica <! Request { id = int64 messageID ; message = message }
                    | _ ->
                        sendInvalidReq data
                
                | [| "get"; "chatLog" |] ->
                    //printfn "Recieved a get chatLog request"
                    replica <! Get
            
                | [| "propose"; rest |] ->
                    match rest.Trim().Split([|' '|]) with
                    | [| slot ; cid ; commandMessage |] ->
                        //printfn "Received a propose: Slot %s, CID %s, message %s" slot cid commandMessage
                        leader <! Propose (int64 slot, { id = int64 cid ; message = commandMessage })
                    | _ ->
                        sendInvalidReq data


                | [| "p1a" ; rest |] ->
                    match rest.Trim().Split([|' '|]) with
                    | [| br ; blid ; scoutName |] ->
                        //printfn "Received a p1a"
                        acceptor <! P1A (scoutName, mailbox.Self, { round = int64 br; leaderID = int64 blid })
                    | _ ->
                        sendInvalidReq data

                | [| "p1b" ; rest |] ->
                    match rest.Trim().Split([|' '|]) with
                    | [| "ballot" ; br ; blid ; "pvalues" ; acceptedString ; scoutName |] ->
                        //printfn "Received a p1b"
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
                        leader <! P1b (scoutName, mailbox.Self, { round = int64 br; leaderID = int64 blid }, pvalues )
                    | [| "ballot" ; br ; blid ; "pvalues" ; scoutName |] ->
                        //printfn "Received a p1b"
                        leader <! P1b (scoutName, mailbox.Self, { round = int64 br; leaderID = int64 blid }, Set.empty )
                    | _ ->
                        sendInvalidReq data
                
                | [| "p2a" ; rest|] ->
                    match rest.Trim().Split([|' '|]) with
                    | [| br ; blid ; slot ; commandId ; commandMessage ; commanderName|] ->
                        //printfn "Received a p2a"
                        let pval = 
                            { 
                                ballot = { round = int64 br; leaderID = int64 blid }
                                slot = int64 slot
                                command = { id = int64 commandId ; message = commandMessage }
                            }
                        acceptor <! P2A (commanderName, mailbox.Self, pval)
                    | _ ->
                        sendInvalidReq data
                
                | [| "p2b" ; rest |] ->
                    match rest.Trim().Split([|' '|]) with
                    | [| br ; blid ; slot ; commanderName |] ->
                        //printfn "Received a p2b"
                        leader <! P2b (commanderName,
                                       mailbox.Self,
                                       { round = int64 br; leaderID = int64 blid },
                                       int64 slot )
                    | _ ->
                        sendInvalidReq data
                
                | [| "decision"; rest |] ->
                    match rest.Trim().Split([|' '|]) with
                    | [| slot ; cid ; commandMessage |] ->
                        //printfn "Received a decision"
                        replica <! Decision (int64 slot, { id = int64 cid ; message = commandMessage })
                    | _ ->
                        sendInvalidReq data
                
                | [| "crash" |] ->
                    System.Environment.Exit(0)

                | [| "crashAfterP1b" |] ->
                    acceptor <! CrashAfterP1b

                | [| "crashAfterP2b" |] ->
                    acceptor <! CrashAfterP2b

                | [| "crashP1a" |] ->
                    leader <! CrashP1a Set.empty
                
                | [| "crashP1a" ; pList |] ->
                    leader <! CrashP1a (parseIdSet pList)
                
                | [| "crashP2a" |] ->
                    leader <! CrashP2a Set.empty
                
                | [| "crashP2a" ; pList |] ->
                    leader <! CrashP2a (parseIdSet pList)
                
                | [| "crashDecision" |] ->
                    leader <! CrashDecision Set.empty
                
                | [| "crashDecision" ; pList |] ->
                    leader <! CrashP2a (parseIdSet pList)

                | _ ->
                    sendInvalidReq data) lines
    
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

        match msg with
        | :? Tcp.Bound as bound ->
            printf "Listening on %A\n" bound.LocalAddress

        | :? Tcp.Connected as connected -> 
            printf "%A connected to the server as %A\n" connected.RemoteAddress serverType
            let handlerName = "handler_" + connected.RemoteAddress.ToString().Replace("[", "").Replace("]", "")
            let handlerRef = spawn mailbox handlerName (handler replica acceptor leader serverType selfID (Some sender))
            sender <! Tcp.Register handlerRef

        | _ -> mailbox.Unhandled()

        return! loop()
    }

    // This handler gives a reference to the replica, acceptor, leader residing on the self server
    let handlerName = sprintf "handler_Self"
    let selfHandlerRef = spawn mailbox handlerName (handler replica acceptor leader serverType selfID None)

    mailbox.Context.System.Tcp() <! Tcp.Bind(mailbox.Self, IPEndPoint(IPAddress.Any, port),options=[Inet.SO.ReuseAddress(true)])

    if serverType = ChatServer then
        let clientPortList = seq {0 .. max} |> Seq.filter (fun n -> n <> int selfID) |> Seq.map (fun n -> 20000 + n)
        for p in clientPortList do
            mailbox.Context.System.Tcp() <! Tcp.Connect(IPEndPoint(IPAddress.Loopback, p),options=[Inet.SO.ReuseAddress(true)])

    loop()