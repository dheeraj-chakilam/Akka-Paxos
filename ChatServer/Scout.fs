module ChatServer.Scout

open Akka.FSharp
open Akka.Actor
open Types

type ScoutCrash =
    | AfterP1a of Set<IActorRef>

type ScoutState = {
    ballotNumber: BallotNumber
    waitfor: Set<IActorRef>
    acceptedPValues: Set<PValue>
    /// The leader who spawned this commander
    beatmap: Map<string,IActorRef*int64>
    crash: ScoutCrash option
}

type ScoutMessage =
    /// P1b (Acceptor's Ref, BallotNumber, Accepted Values)
    | P1b of IActorRef * BallotNumber * Set<PValue>

let scout (selfID: int64) selfName n leader acceptors ballotNumber crash (mailbox: Actor<ScoutMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with        
        | P1b (ref, b, pvals) -> 
            let state =
                // Is returned ballot greater than the one we sent
                if (b = state.ballotNumber) then
                    let state =
                        { state with
                            waitfor = Set.remove ref state.waitfor
                            acceptedPValues = Set.union state.acceptedPValues pvals }
                    if Set.count state.waitfor < n / 2 then
                        leader <! LeaderMessage.Adopted(b, state.acceptedPValues)
                    state
                else
                    leader <! LeaderMessage.Preempted b
                    state
            return! loop state
    }
    
    let filteredAcceptors =
        match crash with
        | Some (AfterP1a refSet) -> Set.intersect refSet acceptors
        | None -> acceptors

    printfn "Scout spawned with ID: %i BallotLeaderID:%i BallotRound: %i Acceptors: %A" selfID ballotNumber.leaderID ballotNumber.round acceptors
    Set.iter (fun r -> r <! sprintf "p1a %i %i %s" ballotNumber.round ballotNumber.leaderID selfName) filteredAcceptors

    Option.iter (fun _ -> System.Environment.Exit(0)) crash

    loop {
        beatmap = Map.empty
        waitfor = acceptors
        ballotNumber = ballotNumber
        acceptedPValues = Set.empty
        crash = crash
    }