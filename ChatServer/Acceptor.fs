module ChatServer.Acceptor

open Akka.FSharp
open Akka.Actor
open Types

type State = {
    leaders: Set<IActorRef>
    master: IActorRef option
    messages: List<string*string>
    beatmap: Map<string,IActorRef*int64>
    ballotNumber: BallotNumber
    accepted: Set<PValue>
}

type AcceptorMessage =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | Heartbeat of string * IActorRef * int64
    | Alive of int64 * string
    | P1A of BallotNumber
    | P2A of PValue
    | Get
    | Leave of IActorRef

// TODO CODECHECK   
// Return true if first is a larger ballot_value than the second
let compareBallots b1 b2 =
    match b1, b2 with
    | (ballotVal1, leaderId1), (ballotVal2, leaderId2) -> 
        if (ballotVal1 > ballotVal2) then true
        else if (ballotVal2 > ballotVal1) then false
        else if (leaderId1 > leaderId2) then true
        else false

let room selfID beatrate aliveThreshold (mailbox: Actor<AcceptorMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(System.TimeSpan.FromMilliseconds 0.,
                                            System.TimeSpan.FromMilliseconds beatrate,
                                            ref,
                                            sprintf "heartbeat %s" selfID)
            
            return! loop { state with leaders = Set.add ref state.leaders }

        | JoinMaster ref ->
            return! loop { state with master = Some ref }

        | Heartbeat (id, ref, ms) ->
            printfn "heartbeat %s" id
            return! loop { state with beatmap = state.beatmap |> Map.add id (ref,ms) }

        | Alive (currMs, selfID) ->
            match state.master with
            | Some m -> 
                let aliveList =
                    state.beatmap
                    |> Map.filter (fun _ (_, ms) -> currMs - ms < aliveThreshold)
                    |> Map.add selfID (Unchecked.defaultof<_>, Unchecked.defaultof<_>)
                    |> Map.toList
                    |> List.map (fun (id,_) -> id)
                m <! (sprintf "alive %s" (System.String.Join(",",aliveList)))
            | None -> ()

            return! loop state

        | Broadcast text -> 
            state.actors
            |> Set.iter (fun a -> a <! (sprintf "rebroadcast %s" text))
            
            return! loop { state with messages = text :: state.messages }

        | Rebroadcast text ->
            return! loop { state with messages = text :: state.messages }

        | Get ->
            match state.master with
            | Some m -> m <! (sprintf "messages %s" (System.String.Join(",",List.rev state.messages)))
            | None -> ()
            
            return! loop state
        
        // TODO CODECHECK   
        | P1A ballotNum -> 
            let state' = 
                if (compareBallots ballotNum state.ballotNumber) then 
                    { state with state.ballotNumber = b }
                else
                    state
                    // TODO: FIX Pvalue-printing
            sender <! (sprintf "p1b")
            return! loop state'
                
        // TODO CODECHECK   
        | P2A pval ->
            match pval with
            | (b, s, c) ->            
                let state' = 
                    if (compareBallots b state.ballotNumber) then
                        { state with state.ballotNumber = b; state.accepted = Set.add (state.ballotNumber, s, c) accepted }
                    else
                        { state with state.ballotNumber = b; state.accepted = Set.add (b, s, c) accepted }
                let (ballot_num, leaderId) = state'.ballotNumber
                 // TODO: Check print statement
                sender <! (sprintf "p2b %i %i %i" ballot_num leaderId s)
                return! loop state'
             
            | _ -> failwith "Incorrect P2A format"

        | Leave ref ->
            return! loop { state with actors = Set.remove ref state.actors }
    }
    loop { leaders = Set.empty ; master = None ; messages = []; beatmap = Map.empty; ballotNumber = (-1L, -1); accepted = Set.empty }