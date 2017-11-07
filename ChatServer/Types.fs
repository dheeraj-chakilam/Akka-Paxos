module Types

open Akka.FSharp
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