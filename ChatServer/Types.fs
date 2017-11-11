module Types

open Akka.FSharp
open Akka.Actor

type Command = { id: int64 ; message: string }
type BallotNumber = { round: int64 ; leaderID: int64 }
type PValue = { ballot: BallotNumber; slot: int64; command: Command}

type LeaderMessage =
    | Join of IActorRef
    | Propose of int64 * Command
    | Adopted of BallotNumber * Set<PValue>
    | Preempted of BallotNumber
    // To forward to the scout
    | P1b of IActorRef * BallotNumber * Set<PValue>
    // To forward to the commander
    | P2b of IActorRef * BallotNumber * int64
    | Heartbeat of string * IActorRef * int64
    | Leave of IActorRef

let (%>) (b1:BallotNumber) (b2:BallotNumber) =
    if not (b1.round = b2.round) then
        b1.round > b2.round
    else
        // The leaderID is the tie breaker
        b1.leaderID > b2.leaderID