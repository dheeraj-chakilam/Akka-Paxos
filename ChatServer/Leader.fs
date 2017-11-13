module ChatServer.Leader

open Akka.FSharp
open Akka.Actor
open Commander
open Scout
open Types

type LeaderCrash =
    | AfterP1a of Set<int64>
    | AfterP2a of Set<int64>
    | AfterDecision of Set<int64>

type LeaderState = {
    ballotNum: BallotNumber
    active: bool
    proposals: Map<int64, Command>
    acceptors: Set<IActorRef>
    replicas: Set<IActorRef>
    commanders: Map<string, IActorRef>
    scouts: Map<string, IActorRef>
    refMap: Map<int64, IActorRef>
    crash: LeaderCrash option
}

let pmax (pvals:Set<PValue>) =
    Set.fold (fun max pval ->
        match Map.tryFind pval.slot max with
        | Some pval' -> if pval.ballot %> pval'.ballot then Map.add pval.slot pval max else max
        | None -> Map.add pval.slot pval max) Map.empty pvals
    |> Map.map (fun slot pval -> pval.command)

let crashRefs idSet state =
    idSet
    |> Set.toSeq
    |> Seq.choose (fun id -> Map.tryFind id state.refMap)
    |> Set.ofSeq

let spawnCommander (mailbox: Actor<LeaderMessage>) selfID n slot command (state:LeaderState) =
    let commanderName = sprintf "commander-%i-%i-%i-%i-%i-%i" selfID state.ballotNum.leaderID state.ballotNum.round slot command.id (System.Random().Next())
    let commanderCrash =
        match state.crash with
        | Some (AfterP2a idSet) ->
            Some (CommanderCrash.AfterP2a (crashRefs idSet state))
        | Some (AfterDecision idSet) ->
            Some (CommanderCrash.AfterDecision (crashRefs idSet state))
        | _ -> None
    let commanderRef =
        spawn mailbox.Context.System commanderName (commander selfID commanderName n mailbox.Self state.acceptors state.replicas state.ballotNum slot command commanderCrash)
    (commanderName, commanderRef)

let spawnScout (mailbox: Actor<LeaderMessage>) selfID n (state:LeaderState) =
    let scoutName = sprintf "scout-%i-%i-%i" state.ballotNum.leaderID state.ballotNum.round (System.Random().Next())
    let scoutCrash =
        match state.crash with
        | Some (AfterP1a idSet) ->
            Some (ScoutCrash.AfterP1a (crashRefs idSet state))
        | _ -> None
    let scoutRef =
        spawn mailbox.Context.System scoutName (scout selfID scoutName n mailbox.Self state.acceptors state.ballotNum scoutCrash)
    (scoutName, scoutRef)

let leader (selfID: int64) n beatrate (mailbox: Actor<LeaderMessage>) =
    let rec loop (state:LeaderState) = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            // Start heartbeating (very slowly) just to get id, ref mappings
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(System.TimeSpan.FromMilliseconds 0.,
                                            System.TimeSpan.FromMilliseconds beatrate,
                                            ref,
                                            sprintf "heartbeat %i" selfID)
            let state =
                { state with acceptors = (Set.add ref state.acceptors) ; replicas = (Set.add ref state.replicas) }
            let state =
                if Set.count state.acceptors = (n / 2) + 1 then
                    printfn "Spawning a scout"
                    let (scoutName, scoutRef) = spawnScout mailbox selfID n state
                    { state with scouts = Map.add scoutName scoutRef state.scouts }
                else
                    state
            return! loop state

        | Heartbeat (id, ref) ->
            //printfn "leader heartbeat %s" id
            return! loop { state with refMap = state.refMap |> Map.add id ref }

        | Leave ref ->
            return! loop { state with acceptors = Set.remove ref state.acceptors; replicas = Set.remove ref state.replicas }

        // The ref is included in propose specifically to add selfReplica
        | Propose (slot, command) ->
            printfn "Leader %i received a propose" selfID
            printfn "Leader has ballot %A" state.ballotNum
            let state =
                if not (Map.containsKey slot state.proposals) then
                    let state =
                        { state with proposals = Map.add slot command state.proposals }
                    if state.active then
                        { state with
                            commanders =
                                let name, ref = (spawnCommander mailbox selfID n slot command state)
                                Map.add name ref state.commanders }
                    else
                        state
                else
                    state
            return! loop state
        
        | Adopted (ballot, pvals) ->
            printfn "Leader %i received an adopted message with ballot (%i,%i), pvals: %A" selfID ballot.leaderID ballot.round pvals
            printfn "Leader has ballot %A" state.ballotNum
            let state =
                if ballot = state.ballotNum then
                    let proposals = Map.fold (fun state slot command -> Map.add slot command state) state.proposals (pmax pvals)
                    let commanders =
                        proposals
                        |> Map.fold (fun stateCommanders slot command ->
                            let (name, ref) = spawnCommander mailbox selfID n slot command state
                            Map.add name ref  stateCommanders) state.commanders
                    { state with
                        active = true
                        commanders = commanders }
                else
                    state
            return! loop state
        
        | Preempted ballot ->
            printfn "Leader %i received a pre-empted message with ballot (%i,%i) and selfBallot (%i,%i)" selfID ballot.leaderID ballot.round state.ballotNum.leaderID state.ballotNum.round
            let state =
                if ballot %> state.ballotNum then
                    //TODO: Backoff
                    let state' = { state with active = false ; ballotNum = { round = ballot.round + 1L ; leaderID = selfID } }
                    let (scoutName, scoutRef) =
                        async {
                            do! Async.Sleep(System.Random().Next(1000))
                            return spawnScout mailbox selfID n state'
                        }
                        |> Async.RunSynchronously
                    { state' with scouts = Map.add scoutName scoutRef state'.scouts }
                else
                    state
            return! loop state
        
        | P1b (name, ref, b, pvals) ->
            match Map.tryFind name state.scouts with
            | Some ref' -> ref' <! ScoutMessage.P1b (ref, b, pvals)
            | None -> printfn "ERROR: Found no scout for ballot %A" b
            return! loop state

        | P2b (name, ref, b, s) ->
            printfn "Leader %i received a P2b with ballot:%A slot: %i" selfID b s
            printfn "Leader %i has state.commanders:%A" selfID state.commanders
            match Map.tryFind name state.commanders with
            | Some ref' -> ref' <! CommanderMessage.P2b (ref, b)
            | None -> printfn "ERROR: Found no commander for name: %s" name
            return! loop state
        
        | CrashP1a idSet ->
            return! loop { state with crash = Some (AfterP1a idSet) }
        
        | CrashP2a idSet ->
            return! loop { state with crash = Some (AfterP2a idSet) }
        
        | CrashDecision idSet ->
            return! loop { state with crash = Some (AfterDecision idSet) }
    }

    loop {
        ballotNum = {round = 0L ; leaderID = selfID}
        active = false
        acceptors = Set.empty
        replicas = Set.empty
        commanders = Map.empty
        scouts = Map.empty
        refMap = Map.empty
        proposals = Map.empty
        crash = None
    }
