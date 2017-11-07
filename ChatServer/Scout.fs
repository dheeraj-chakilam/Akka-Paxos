module ChatServer.Scout

open Akka.FSharp
open Akka.Actor
open Types

type ScoutState = {
    acceptors: Set<IActorRef>
    ballotNumber: BallotNumber
    waitfor: Set<IActorRef>
    acceptedPValues: Set<PValue>
    /// The leader who spawned this commander
    leader: IActorRef
    beatmap: Map<string,IActorRef*int64>
}

type ScoutMessage =
    /// P1b (Acceptor's Ref, BallotNumber, Accepted Values)
    | P1b of IActorRef * BallotNumber * Set<PValue>

let scout selfID n leader acceptors ballotNumber (mailbox: Actor<ScoutMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with        
        | P1b (ref, b, pvals) -> 
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
                            () //TODO SEND ADOPTED MESSAGE TO LEADER
                        { state with waitfor = waitfor ; acceptedPValues = Set.union state.acceptedPValues pvals }
                    else
                        state
                return! loop state
    }


    //TODO: FIX NETWORK PRINT
    Set.iter (fun r -> r <! sprintf "p1a") acceptors

    loop {
        leader = leader
        beatmap = Map.empty
        acceptors = acceptors
        waitfor = acceptors
        ballotNumber = ballotNumber
        acceptedPValues = Set.empty
    }