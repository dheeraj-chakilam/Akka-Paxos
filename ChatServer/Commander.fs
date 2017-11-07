module ChatServer.Commander

open Akka.FSharp
open Akka.Actor
open Types

type CommanderState = {
    acceptors: Set<IActorRef>
    replicas: Set<IActorRef>
    ballotNumber: BallotNumber
    slotNum: int64
    command: Command
    waitfor: Set<IActorRef>
    /// The leader who spawned this commander
    leader: IActorRef
    beatmap: Map<string,IActorRef*int64>
}

type CommanderMessage =
    /// P2b (Acceptor's ref, BallotNumber)
    | P2b of IActorRef * BallotNumber

let commander selfID n leader replicas acceptors ballotNumber slotNumber command (mailbox: Actor<CommanderMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with        
        | P2b (ref, b) -> 
            // Is returned ballot greater than the one we sent
            if (b %> state.ballotNumber) then
                state.leader <! LeaderMessage.Preempted b
                return! loop state
            else
                let state = 
                    if (state.waitfor.Contains(ref)) then
                        let waitfor = Set.remove ref state.waitfor
                        if (waitfor.Count * 2) < n then
                            Set.iter (fun r -> r <! Decid
                        { state with waitfor = waitfor }
                    else
                        state
                return! loop state
    }
    Set.iter (fun r -> r <! sprintf "p2a %i %i %i %i %s" ballotNumber.round ballotNumber.leaderID slotNumber command.id command.message) acceptors

    loop {
        leader = leader
        beatmap = Map.empty
        acceptors = acceptors
        replicas = replicas
        waitfor = acceptors
        ballotNumber = ballotNumber
        slotNum = slotNumber
        command = command
    }