module ChatServer.Scout

open Akka.FSharp
open Akka.Actor
open Types

type State = {
    acceptors: Set<IActorRef>
    ballotNumber: BallotNumber
    waitfor: Set<IActorRef>
    adoptedPValues: Set<PValue>
    slotNum: int64
    proposals: Map<int64, Command>
    decisions: Map<int64, Command>
    leaders: Set<IActorRef>
    master: IActorRef option
    messages: List<Command>
    beatmap: Map<string,IActorRef*int64>
}

type ScoutMessage =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | Heartbeat of string * IActorRef * int64
    | Alive of int64 * string
    | Request of Command
    | Decision of int64 * Command
    | Get
    | Leave of IActorRef
    | P1b of IActorRef * BallotNumber * Set<PValue>

let propose command state =
    let rec findGap state i =
        if (Map.containsKey i state.proposals) || (Map.containsKey i state.decisions) then
            findGap state i + 1L
        else
            i
    if not (Map.exists (fun _ c -> c = command) state.decisions) then
        let newSlot = findGap state 0L
        let proposals' = Map.add newSlot command state.proposals
        let (id, op) = command
        Set.iter (fun r -> r <! sprintf "propose %i %s %s" newSlot id op) state.leaders
        { state with proposals = proposals' }
    else
        state

let perform command state = 
    let prevPerform = 
        state.decisions
        |> Map.filter (fun s _ -> s < state.slotNum)
        |> Map.exists (fun _ c -> command = c)

    if prevPerform then
        { state with slotNum = state.slotNum + 1L }
    else 
        { state with
                slotNum = state.slotNum + 1L ;
                messages = command :: state.messages }

let room selfID beatrate aliveThreshold acceptors ballotNumber (mailbox: Actor<ScoutMessage>) =
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

        | Get ->
            match state.master with
            | Some m -> m <! (sprintf "messages %s" (System.String.Join(",",List.rev state.messages)))
            | None -> ()
            
            return! loop state

        | Leave ref ->
            return! loop { state with leaders = Set.remove ref state.leaders }
        
        | P1b (ref, b, pvals) -> 
            // Is returned ballot greater than the one we sent
            if (b %> state.ballotNumber) then
                ()
                // TODO SEND PREEMPT TO LEADER:  <! (sprintf "Preempted")
                return! loop state
            else
                let state' = 
                    if (state.waitfor.Contains(ref)) then
                        let state'' = { state with waitfor = state.waitfor.Remove(ref) ; adoptedPValues = Set.union state.adoptedPValues pvals }
                        if ((state''.waitfor.Count * 2) < state''.acceptors.Count) then
                            () //TODO SEND ADOPTED MESSAGE TO LEADER
                        state''
                    else
                        state
                return! loop state'
    }

    let state = {
        leaders = Set.empty ;
        master = None ;
        messages = [];
        beatmap = Map.empty;
        proposals = Map.empty;
        decisions = Map.empty;
        slotNum = 0L
        acceptors = acceptors
        waitfor = acceptors
        ballotNumber = ballotNumber
        adoptedPValues = Set.empty
    }

    //TODO: FIX NETWORK PRINT
    Set.iter (fun r -> r <! sprintf "p1a") state.acceptors

    loop state