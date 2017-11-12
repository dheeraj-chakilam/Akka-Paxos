module ChatServer.Commander

open Akka.FSharp
open Akka.Actor
open Types

type CommanderState = {
    waitfor: Set<IActorRef>
    /// The leader who spawned this commander
    beatmap: Map<string,IActorRef*int64>
}

type CommanderMessage =
    /// P2b (Acceptor's ref, BallotNumber)
    | P2b of IActorRef * BallotNumber

let commander selfID n leader replicas acceptors ballotNumber (slotNumber: int64) command (mailbox: Actor<CommanderMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with        
        | P2b (ref, b) -> 
            printfn "Commander received a P2B with \n ballot %O! \n state.ballot %O" b ballotNumber
            // Is returned ballot greater than the one we sent
            if (b <> ballotNumber) then
                leader <! LeaderMessage.Preempted b
                return! loop state
            else
                let state = 
                    if (state.waitfor.Contains(ref)) then
                        let waitfor = Set.remove ref state.waitfor
                        if (waitfor.Count * 2) < n then
                            Set.iter (fun r -> r <! sprintf "decision %i %i %s" slotNumber command.id command.message) replicas
                        { state with waitfor = waitfor }
                    else
                        state
                return! loop state
    }

    printfn "Commander sending ballot %O" ballotNumber
    Set.iter (fun r -> r <! sprintf "p2a %i %i %i %i %s" ballotNumber.round ballotNumber.leaderID slotNumber command.id command.message) acceptors

    loop {
        beatmap = Map.empty
        waitfor = acceptors
    }