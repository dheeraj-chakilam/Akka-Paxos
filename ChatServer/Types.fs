module Types

open Akka.FSharp
open Akka.Actor

/// A command of the form (id, message)
type Command = string * string