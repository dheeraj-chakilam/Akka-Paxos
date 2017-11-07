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
                ()
                // TODO SEND PREEMPT TO LEADER:  <! (sprintf "Preempted")
                return! loop state
            else
                let state = 
                    if (state.waitfor.Contains(ref)) then
                        let waitfor = Set.remove ref state.waitfor
                        if (waitfor.Count * 2) < n then
                            () //TODO SEND DELIVER MESSAGE TO LEADER
                        { state with waitfor = waitfor }
                    else
                        state
                return! loop state
    }


    //TODO: FIX NETWORK PRINT FOR COMMANDER (Need to use ballot, slot, command)
    Set.iter (fun r -> r <! sprintf "p2a") acceptors

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