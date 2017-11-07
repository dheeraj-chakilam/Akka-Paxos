module ChatServer.Acceptor

open Akka.FSharp
open Akka.Actor
open Types

type State = {
    ballotNumber: BallotNumber
    accepted: Set<PValue>
}

type AcceptorMessage =
    | P1A of IActorRef * BallotNumber
    | P2A of IActorRef * PValue

let acceptor selfID (mailbox: Actor<AcceptorMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with        
        // TODO CODECHECK   
        | P1A (scoutRef, b) -> 
            let state' = 
                if (b %> state.ballotNumber) then 
                    { state with ballotNumber = b }
                else
                    state
                    // TODO: FIX Pvalue-printing
            scoutRef <! (sprintf "p1b")
            return! loop state'
                
        // TODO CODECHECK   
        | P2A (commanderRef, pval) ->
            let (b, s, c) = pval
            let state' = 
                if (b %> state.ballotNumber || b = state.ballotNumber) then
                    { state with ballotNumber = b; accepted = Set.add (b, s, c) state.accepted }
                else
                    state
            let (ballot_num, leaderId) = state'.ballotNumber
                // TODO: Check print statement
            commanderRef <! (sprintf "p2b %i %i %i" ballot_num leaderId s)
            return! loop state'
    }
    loop { ballotNumber = (-1L, -1L); accepted = Set.empty }