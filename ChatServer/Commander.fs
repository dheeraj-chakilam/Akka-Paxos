module ChatServer.Commander

open Akka.FSharp
open Akka.Actor
open Types

type State = {
    acceptors: Set<IActorRef>
    ballotNumber: BallotNumber
    slotNum: int64
    command: Command
    waitfor: Set<IActorRef>
    proposals: Map<int64, Command>
    decisions: Map<int64, Command>
    /// The leader who spawned this commander
    leader: IActorRef
    beatmap: Map<string,IActorRef*int64>
}

type CommanderMessage =
    | Join of IActorRef
    | Heartbeat of string * IActorRef * int64
    | Request of Command
    | Decision of int64 * Command
    | Leave of IActorRef
    /// P2b (Acceptor's ref, BallotNumber)
    | P2b of IActorRef * BallotNumber

let room selfID beatrate leader acceptors ballotNumber slotNumber command (mailbox: Actor<CommanderMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(System.TimeSpan.FromMilliseconds 0.,
                                            System.TimeSpan.FromMilliseconds beatrate,
                                            ref,
                                            sprintf "heartbeat %s" selfID)
            
            return! loop { state with acceptors = Set.add ref state.acceptors }

        | Heartbeat (id, ref, ms) ->
            printfn "heartbeat %s" id
            return! loop { state with beatmap = state.beatmap |> Map.add id (ref,ms) }

        | Leave ref ->
            return! loop { state with acceptors = Set.remove ref state.acceptors }
        
        | P2b (ref, b) -> 
            // Is returned ballot greater than the one we sent
            if (b %> state.ballotNumber) then
                ()
                // TODO SEND PREEMPT TO LEADER:  <! (sprintf "Preempted")
                return! loop state
            else
                let state' = 
                    if (state.waitfor.Contains(ref)) then
                        let state'' = { state with waitfor = state.waitfor.Remove(ref) }
                        if ((state''.waitfor.Count * 2) < state''.acceptors.Count) then
                            () //TODO SEND DELIVER MESSAGE TO LEADER
                        state''
                    else
                        state
                return! loop state'
    }

    let state = {
        leader = leader;
        beatmap = Map.empty;
        proposals = Map.empty;
        decisions = Map.empty;
        acceptors = acceptors;
        waitfor = acceptors;
        ballotNumber = ballotNumber;
        slotNum = slotNumber;
        command = command
    }

    //TODO: FIX NETWORK PRINT FOR COMMANDER (Need to use ballot, slot, command)
    Set.iter (fun r -> r <! sprintf "p2a") state.acceptors

    loop state