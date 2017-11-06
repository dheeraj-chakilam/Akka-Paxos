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
        
        // TODO CODECHECK   
        | P1A b -> 
            let state' = 
                if (b %> state.ballotNumber) then 
                    { state with ballotNumber = b }
                else
                    state
                    // TODO: FIX Pvalue-printing
            sender <! (sprintf "p1b")
            return! loop state'
                
        // TODO CODECHECK   
        | P2A pval ->
            let (b, s, c) = pval
            let state' = 
                if (b %> state.ballotNumber || b = state.ballotNumber) then
                    { state with ballotNumber = b; accepted = Set.add (b, s, c) state.accepted }
                else
                    state
            let (ballot_num, leaderId) = state'.ballotNumber
                // TODO: Check print statement
            sender <! (sprintf "p2b %i %i %i" ballot_num leaderId s)
            return! loop state'
             
            | _ -> failwith "Incorrect P2A format"
    }
    loop { leaders = Set.empty ; master = None ; messages = []; beatmap = Map.empty; ballotNumber = (-1L, -1L); accepted = Set.empty }