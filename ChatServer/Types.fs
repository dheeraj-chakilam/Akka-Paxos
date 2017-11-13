module Types

open Akka.Actor

type Command = { id: int64 ; message: string }
type BallotNumber = { round: int64 ; leaderID: int64 }
type PValue = { ballot: BallotNumber; slot: int64; command: Command}

let (%>) (b1:BallotNumber) (b2:BallotNumber) =
    if not (b1.round = b2.round) then
        b1.round > b2.round
    else
        // The leaderID is the tie breaker
        b1.leaderID > b2.leaderID

type LeaderMessage =
    | Join of IActorRef
    | Propose of int64 * Command
    | Adopted of BallotNumber * Set<PValue>
    | Preempted of BallotNumber
    // To forward to the scout
    | P1b of string * IActorRef * BallotNumber * Set<PValue>
    // To forward to the commander
    | P2b of string * IActorRef * BallotNumber * int64
    | Heartbeat of int64 * IActorRef
    | Leave of IActorRef
    | CrashP1a of Set<int64>
    | CrashP2a of Set<int64>
    | CrashDecision of Set<int64>