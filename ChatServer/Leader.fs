module ChatServer.Leader

open Akka.FSharp
open Akka.Actor
open Types

type State = {
    slotNum: int64
    proposals: Map<int64, Command>
    acceptors: Set<IActorRef>
    replicas: Set<IActorRef>
    beatmap: Map<string,IActorRef*int64>
}

type LeaderMessage =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | Heartbeat of string * IActorRef * int64
    | Leave of IActorRef

let leader selfID (mailbox: Actor<ReplicaMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            return! loop { state with acceptors = Set.add ref state.acceptors; replicas = Set.add ref state.replicas }

        | Heartbeat (id, ref, ms) ->
            printfn "heartbeat %s" id
            return! loop { state with beatmap = state.beatmap |> Map.add id (ref,ms) }

        | Leave ref ->
            return! loop { state with acceptors = Set.remove ref state.acceptors; replicas = Set.remove ref state.replicas }
    }

    loop {
        acceptors = Set.empty;
        replicas = Set.empty;
        beatmap = Map.empty;
        proposals = Map.empty;
        slotNum = 0L
    }