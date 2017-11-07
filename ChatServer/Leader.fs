module ChatServer.Leader

open Akka.FSharp
open Akka.Actor
open Types
open Commander
open Scout

type LeaderState = {
    ballotNum: BallotNumber
    active: bool
    proposals: Map<int64, Command>
    acceptors: Set<IActorRef>
    replicas: Set<IActorRef>
    commanders: Map<BallotNumber * int64, IActorRef>
    scouts: Map<BallotNumber, IActorRef>
    beatmap: Map<string,IActorRef*int64>
}

let pmax (pvals:Set<PValue>) =
    Set.fold (fun max pval ->
        match Map.tryFind pval.slot max with
        | Some pval' -> if pval.ballot %> pval'.ballot then Map.add pval.slot pval max else max
        | None -> Map.add pval.slot pval max) Map.empty pvals
    |> Map.map (fun slot pval -> pval.command)

let spawnCommander (mailbox: Actor<LeaderMessage>) selfID n slot command (state:LeaderState) =
    spawn mailbox.Context.System (sprintf "commander-%i-%i-%i" state.ballotNum.round slot command.id) (commander selfID n mailbox.Self state.replicas state.acceptors state.ballotNum slot command)

let spawnScout (mailbox: Actor<LeaderMessage>) selfID n (state:LeaderState) =
    spawn mailbox.Context.System (sprintf "scout-%i" state.ballotNum.round) (scout selfID n mailbox.Self state.acceptors state.ballotNum)

let leader selfID n selfAcceptor (mailbox: Actor<LeaderMessage>) =
    let rec loop (state:LeaderState) = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            let acceptors = (Set.add ref state.acceptors)
            let state =
                if Set.count acceptors = (n / 2) + 1 then
                    let scoutRef = spawnScout mailbox selfID n state
                    { state with scouts = Map.add state.ballotNum scoutRef state.scouts }
                else
                    state
            return! loop { state with acceptors = acceptors ; replicas = (Set.add ref state.replicas) }

        | Heartbeat (id, ref, ms) ->
            printfn "heartbeat %s" id
            return! loop { state with beatmap = state.beatmap |> Map.add id (ref,ms) }

        | Leave ref ->
            return! loop { state with acceptors = Set.remove ref state.acceptors; replicas = Set.remove ref state.replicas }

        // The ref is included in propose specifically to add selfReplica
        | Propose (ref, slot, command) ->
            //TODO: Remove
            let state = { state with replicas = Set.add ref state.replicas }
            let state' =
                if not (Map.containsKey slot state.proposals) then
                    let proposals = Map.add slot command state.proposals
                    if state.active then
                        let commanderRef = spawnCommander mailbox selfID n slot command state
                        let commanders = Map.add (state.ballotNum, slot) commanderRef state.commanders
                        { state with proposals = proposals ; commanders = commanders }
                    else
                        { state with proposals = proposals }
                else
                    state
            return! loop state'
        
        | Adopted (ballot, pvals) ->
            let state =
                if ballot = state.ballotNum then
                    let proposals = Map.fold (fun state slot command -> Map.add slot command state) state.proposals (pmax pvals)
                    let commanderRefs =
                        proposals
                        |> Map.toList
                        |> List.map (fun (slot, command) -> ((state.ballotNum, slot), spawnCommander mailbox selfID n slot command state))
                        |> Map.ofList
                    
                    let commanders' = 
                        Map.fold (fun state (slot, command) ref -> Map.add (slot, command) ref state) state.commanders commanderRefs
                    { state with commanders = commanders' ; active = true }
                else
                    state
            return! loop state
        
        | Preempted ballot ->
            let state =
                if ballot %> state.ballotNum then
                    //TODO: Backoff
                    let state' = { state with active = false ; ballotNum = { round = ballot.round + 1L ; leaderID = selfID } }
                    let scoutRef =
                        //TODO: Ensure only one scout is spawned only once per ballot to ensure unique names
                        spawnScout mailbox selfID n state
                    { state' with active = false ; ballotNum = { round = ballot.round + 1L ; leaderID = selfID } ; scouts = Map.add state.ballotNum scoutRef state.scouts }
                else
                    state
            return! loop state
        
        | LeaderMessage.P1b (ref, b, pvals) -> 
            let scoutRef = Map.tryFind b state.scouts
            match scoutRef with
            | Some ref' -> ref' <! ScoutMessage.P1b (ref, b, pvals)
            | None -> ()
            return! loop state

        | LeaderMessage.P2b (ref, b, s) ->
            let commanderRef = Map.tryFind (b, s)  state.commanders
            match commanderRef with
            | Some ref' -> ref' <! CommanderMessage.P2b (ref, b)
            | None -> ()
            return! loop state
    }

    loop {
        ballotNum = {round = 0L ; leaderID = selfID}
        active = false
        acceptors = Set.add selfAcceptor Set.empty
        replicas = Set.empty
        commanders = Map.empty
        scouts = Map.empty
        beatmap = Map.empty
        proposals = Map.empty
    }
