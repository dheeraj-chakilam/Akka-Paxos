module Types

open Akka.FSharp
open Akka.Actor

/// A command of the form (id, message)
type Command = { id: int64 ; message: string }
/// (Round, LeaderID)
type BallotNumber = { round: int64 ; leaderID: int64 }
/// (BallotNumber, Slot, Command)
type PValue = {ballot: BallotNumber; slot: int64; command: Command}

let (%>) (b1:BallotNumber) (b2:BallotNumber) =
    if not (b1.round = b2.round) then
        b1.round > b2.round
    else
        b1.leaderID > b2.leaderID