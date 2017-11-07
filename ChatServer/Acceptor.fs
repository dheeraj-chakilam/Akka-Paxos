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
        | P2A (commanderRef, p) ->
            let state' = 
                if (p.ballot %> state.ballotNumber || p.ballot = state.ballotNumber) then
                    { state with ballotNumber = p.ballot; accepted = Set.add p state.accepted }
                else
                    state
            // TODO: Fix sprintf
            commanderRef <! (sprintf "p2b %i %i %i" p.ballot.round p.ballot.leaderID p.slot)
            return! loop state'
    }
    loop {
        ballotNumber = { round = -1L; leaderID = -1L }
        accepted = Set.empty }