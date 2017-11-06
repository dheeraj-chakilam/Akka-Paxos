module Types

open Akka.FSharp
open Akka.Actor

/// A command of the form (id, message)
type Command = string * string
type BallotNumber = int64 * int64
type PValue = BallotNumber * int64 * Command

let (%>) (b1:BallotNumber) (b2:BallotNumber) =
    let (ballotVal1, leaderId1) = b1
    let (ballotVal2, leaderId2) = b2
    if (ballotVal1 > ballotVal2) then
        true
    else
        if (ballotVal1 = ballotVal2) then
            (leaderId1 > leaderId2)
        else
            false