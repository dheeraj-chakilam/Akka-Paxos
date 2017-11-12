module ChatServer.Leader

open Akka.FSharp
open Akka.Actor
open Commander
open Scout
open Types

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
    spawn mailbox.Context.System (sprintf "commander-%i-%i-%i-%i" selfID state.ballotNum.round slot command.id) (commander selfID n mailbox.Self state.replicas state.acceptors state.ballotNum slot command)

let spawnScout (mailbox: Actor<LeaderMessage>) selfID n (state:LeaderState) =
    spawn mailbox.Context.System (sprintf "scout-%i" state.ballotNum.round) (scout selfID n mailbox.Self state.acceptors state.ballotNum)

let leader (selfID: int64) n (mailbox: Actor<LeaderMessage>) =
    let rec loop (state:LeaderState) = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            printfn "Leader %i Received a Join from %O" selfID ref
            let state =
                { state with acceptors = (Set.add ref state.acceptors) }
            let state =
                if Set.count state.acceptors = (n / 2) + 1 then
                    printfn "Spawning a scout"
                    let scoutRef = spawnScout mailbox selfID n state
                    { state with scouts = Map.add state.ballotNum scoutRef state.scouts }
                else
                    state
            return! loop { state with replicas = (Set.add ref state.replicas) }

        | Heartbeat (id, ref, ms) ->
            //printfn "leader heartbeat %s" id
            return! loop { state with beatmap = state.beatmap |> Map.add id (ref,ms) }

        | Leave ref ->
            return! loop { state with acceptors = Set.remove ref state.acceptors; replicas = Set.remove ref state.replicas }

        // The ref is included in propose specifically to add selfReplica
        | Propose (slot, command) ->
            printfn "Leader %i received a propose" selfID
            printfn "Leader has ballot %O" state.ballotNum
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
            printfn "Leader %i received an adopted message with ballot (%i,%i), pvals: %O" selfID ballot.leaderID ballot.round pvals
            printfn "Leader has ballot %O" state.ballotNum
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
            printfn "Leader %i received a pre-empted message with ballot (%i,%i)" selfID ballot.leaderID ballot.round
            let state =
                if ballot %> state.ballotNum then
                    //TODO: Backoff
                    let state' = { state with active = false ; ballotNum = { round = ballot.round + 1L ; leaderID = selfID } }
                    let scoutRef =
                        //TODO: Ensure only one scout is spawned only once per ballot to ensure unique names
                        spawnScout mailbox selfID n state'
                    { state' with scouts = Map.add state'.ballotNum scoutRef state'.scouts }
                else
                    state
            return! loop state
        
        | P1b (ref, b, pvals) -> 
            let scoutRef = Map.tryFind b state.scouts
            match scoutRef with
            | Some ref' -> ref' <! ScoutMessage.P1b (ref, b, pvals)
            | None -> ()
            return! loop state

        | P2b (ref, ob, nb, s) ->
            printfn "Leader %i received a P2b with ballot:%O slot: %i" selfID nb s
            printfn "Leader %i has state.commanders:%O" selfID state.commanders
            let commanderRef = Map.tryFind (ob, s)  state.commanders
            match commanderRef with
            | Some ref' -> ref' <! CommanderMessage.P2b (ref, nb)
            | None -> printfn "ERROR: Found no commander in leader %i" selfID
            return! loop state
    }

    loop {
        ballotNum = {round = 0L ; leaderID = selfID}
        active = false
        acceptors = Set.empty
        replicas = Set.empty
        commanders = Map.empty
        scouts = Map.empty
        beatmap = Map.empty
        proposals = Map.empty
    }
