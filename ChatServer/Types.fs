module Types

open Akka.FSharp
open Akka.Actor

/// A command of the form (id, message)
type Command = string * string
type BallotNumber = int64 * int
type PValue = BallotNumber * int64 * Command