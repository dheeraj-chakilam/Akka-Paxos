﻿module ChatServer.Commander

open Akka.FSharp
open Akka.Actor
open Types

type CommanderCrash =
    | AfterP2a of Set<IActorRef>
    | AfterDecision of Set<IActorRef>

type CommanderState = {
    waitfor: Set<IActorRef>
    /// The leader who spawned this commander
    beatmap: Map<string,IActorRef*int64>
    crash: CommanderCrash option
}

type CommanderMessage =
    /// P2b (Acceptor's ref, BallotNumber)
    | P2b of IActorRef * BallotNumber

let commander selfID selfName n leader acceptors replicas ballotNumber (slotNumber: int64) command crash (mailbox: Actor<CommanderMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with        
        | P2b (ref, b) -> 
            printfn "Commander received a P2B with \n ballot %A! \n state.ballot %A" b ballotNumber
            let state =
                // Is returned ballot greater than the one we sent
                if (ballotNumber = b) then
                    let state = 
                        let waitfor = Set.remove ref state.waitfor
                        if Set.count waitfor < n / 2 then
                            let filteredReplicas =
                                match crash with
                                | Some (AfterDecision refSet) -> Set.intersect refSet replicas
                                | _ -> replicas
                            Set.iter (fun r -> r <! sprintf "decision %i %i %s" slotNumber command.id command.message) filteredReplicas
                            match crash with
                            | Some (AfterDecision refSet) -> System.Environment.Exit(0)
                            | _ -> ()
                        { state with waitfor = waitfor }
                    state
                else
                    leader <! LeaderMessage.Preempted b
                    state
            return! loop state
    }

    let filteredAcceptors =
        match crash with
        | Some (AfterP2a refSet) -> Set.intersect refSet acceptors
        | _ -> acceptors

    printfn "Commander %s sending ballot %A" selfName ballotNumber
    Set.iter (fun r -> r <! sprintf "p2a %i %i %i %i %s %s" ballotNumber.round ballotNumber.leaderID slotNumber command.id command.message selfName) filteredAcceptors

    match crash with
    | Some (AfterP2a refSet) -> System.Environment.Exit(0)
    | _ -> ()

    loop { beatmap = Map.empty ; waitfor = acceptors ; crash = crash }