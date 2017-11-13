module ChatServer.Acceptor

open Akka.FSharp
open Akka.Actor
open Types

type AcceptorCrash =
    | AfterP1b
    | AfterP2b

type State = {
    ballotNumber: BallotNumber
    accepted: Set<PValue>
    crash: AcceptorCrash option
}

type AcceptorMessage =
    | P1A of string * IActorRef * BallotNumber
    | P2A of string * IActorRef * PValue
    | CrashAfterP1b
    | CrashAfterP2b

let acceptor selfID (mailbox: Actor<AcceptorMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | P1A (scoutName, scoutRef, b) -> 
            let state = 
                if (b %> state.ballotNumber) then 
                    { state with ballotNumber = b }
                else
                    state
            // TODO: FIX Pvalue-printing
            let acceptedString =
                state.accepted
                |> Set.toSeq
                |> Seq.map (fun pval -> sprintf "%i,%i,%i,%i,%s" pval.ballot.round pval.ballot.leaderID pval.slot pval.command.id pval.command.message)
                |> String.concat "|"
            scoutRef <! (sprintf "p1b ballot %i %i pvalues %s %s" state.ballotNumber.round state.ballotNumber.leaderID acceptedString scoutName)

            match state.crash with
            | Some AfterP1b -> System.Environment.Exit(0)
            | _ -> ()

            return! loop state
                
        // TODO CODECHECK   
        | P2A (commanderName, commanderRef, p) ->
            let state = 
                if (p.ballot %> state.ballotNumber || p.ballot = state.ballotNumber) then
                    printfn "Accepter accepted a ballot with \n p.ballot %A! \n state.ballot %A" p.ballot state.ballotNumber
                    { state with ballotNumber = p.ballot; accepted = Set.add p state.accepted }
                else
                    state
            // TODO: Fix sprintf
            commanderRef <! (sprintf "p2b %i %i %i %s" state.ballotNumber.round state.ballotNumber.leaderID p.slot commanderName)
            
            match state.crash with
            | Some AfterP2b -> System.Environment.Exit(0)
            | _ -> ()

            return! loop state
        
        | CrashAfterP1b ->
            return! loop { state with crash = Some AfterP1b }
        
        | CrashAfterP2b ->
            return! loop { state with crash = Some AfterP2b }
    }
    loop {
        ballotNumber = { round = -1L; leaderID = -1L }
        accepted = Set.empty
        crash = None }