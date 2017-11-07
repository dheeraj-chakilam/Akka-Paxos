module ChatServer.Scout

open Akka.FSharp
open Akka.Actor
open Types

type State = {
    acceptors: Set<IActorRef>
    ballotNumber: BallotNumber
    waitfor: Set<IActorRef>
    adoptedPValues: Set<PValue>
    proposals: Map<int64, Command>
    decisions: Map<int64, Command>
    leader: IActorRef
    beatmap: Map<string,IActorRef*int64>
}

type ScoutMessage =
    | Join of IActorRef
    | Leave of IActorRef
    /// P1b (Acceptor's Ref, BallotNumber, Accepted Values)
    | P1b of IActorRef * BallotNumber * Set<PValue>

let room selfID beatrate leader acceptors ballotNumber (mailbox: Actor<ScoutMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(System.TimeSpan.FromMilliseconds 0.,
                                            System.TimeSpan.FromMilliseconds beatrate,
                                            ref,
                                            sprintf "heartbeat %s" selfID)
            
            return! loop { state with acceptors = Set.add ref state.acceptors }

        | Leave ref ->
            return! loop { state with acceptors = Set.remove ref state.acceptors }
        
        | P1b (ref, b, pvals) -> 
            // Is returned ballot greater than the one we sent
            if (b %> state.ballotNumber) then
                ()
                // TODO SEND PREEMPT TO LEADER:  <! (sprintf "Preempted")
                return! loop state
            else
                let state' = 
                    if (state.waitfor.Contains(ref)) then
                        let state'' = { state with waitfor = state.waitfor.Remove(ref) ; adoptedPValues = Set.union state.adoptedPValues pvals }
                        if ((state''.waitfor.Count * 2) < state''.acceptors.Count) then
                            () //TODO SEND ADOPTED MESSAGE TO LEADER
                        state''
                    else
                        state
                return! loop state'
    }

    let state = {
        leader = leader
        beatmap = Map.empty
        proposals = Map.empty
        decisions = Map.empty
        acceptors = acceptors
        waitfor = acceptors
        ballotNumber = ballotNumber
        adoptedPValues = Set.empty
    }

    //TODO: FIX NETWORK PRINT
    Set.iter (fun r -> r <! sprintf "p1a") state.acceptors

    loop state