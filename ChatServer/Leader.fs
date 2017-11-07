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
    commanders: Set<IActorRef>
    scouts: Set<IActorRef>
    beatmap: Map<string,IActorRef*int64>
}

type LeaderMessage =
    | Join of IActorRef
    | Propose of IActorRef * int64 * Command
    | Adopted of BallotNumber * Set<PValue>
    | Preempted of BallotNumber
    | Heartbeat of string * IActorRef * int64
    | Leave of IActorRef

let pmax (pvals:Set<PValue>) =
    Set.fold (fun max pval ->
        match Map.tryFind pval.slot max with
        | Some pval' -> if pval.ballot %> pval'.ballot then Map.add pval.slot pval max else max
        | None -> Map.add pval.slot pval max) Map.empty pvals
    |> Map.map (fun slot pval -> pval.command)

let leader selfID n selfAcceptor (mailbox: Actor<LeaderMessage>) =
    let rec loop (state:LeaderState) = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            return! loop { state with acceptors = (Set.add ref state.acceptors) ; replicas = (Set.add ref state.replicas) }

        | Heartbeat (id, ref, ms) ->
            printfn "heartbeat %s" id
            return! loop { state with beatmap = state.beatmap |> Map.add id (ref,ms) }

        | Leave ref ->
            return! loop { state with acceptors = Set.remove ref state.acceptors; replicas = Set.remove ref state.replicas }

        // The ref is included in propose specifically to add selfReplica
        | Propose (ref, slot, command) ->
            let replicas = Set.add ref state.replicas
            let state =
                if not (Map.containsKey slot state.proposals) then
                    let proposals = Map.add slot command state.proposals
                    if state.active then
                        let commanderRef = spawn mailbox.Context.System (sprintf "commander-%i-%i-%i" state.ballotNum.round slot command.id) (commander selfID n mailbox.Self replicas state.acceptors state.ballotNum slot command)
                        let commanders = Set.add commanderRef state.commanders
                        { state with proposals = proposals ; commanders = commanders }
                    else
                        { state with proposals = proposals }
                else
                    state
            return! loop state
        
        | Adopted (ballot, pvals) ->
            let state =
                if ballot = state.ballotNum then
                    let proposals = Map.fold (fun state slot command -> Map.add slot command state) state.proposals (pmax pvals)
                    let commanderRefs =
                        proposals
                        |> Map.toList
                        |> List.map (fun (slot, command) ->
                            spawn mailbox.Context.System (sprintf "commander-%i-%i-%i" state.ballotNum.round slot command.id) (commander selfID n mailbox.Self state.replicas state.acceptors state.ballotNum slot command))
                        |> Set.ofList
                    { state with commanders = (Set.union commanderRefs state.commanders) ; active = true }
                else
                    state
            return! loop state
        
        | Preempted ballot ->
            let state =
                if ballot %> state.ballotNum then
                    //TODO: Backoff
                    let scoutRef =
                        //TODO: Ensure only one scout is spawned only once per ballot to ensure unique names
                        spawn mailbox.Context.System (sprintf "scout-%i" ballot.round) (scout selfID n mailbox.Self state.acceptors ballot)
                    { state with active = false ; ballotNum = { round = ballot.round + 1L ; leaderID = selfID } ; scouts = Set.add scoutRef state.scouts }
                else
                    state
            return! loop state
    }

    // TODO: Initial spawning of scouts

    loop {
        ballotNum = {round = 0L ; leaderID = selfID}
        active = false
        acceptors = Set.add selfAcceptor Set.empty
        replicas = Set.empty
        commanders = Set.empty
        scouts = Set.empty
        beatmap = Map.empty
        proposals = Map.empty
    }