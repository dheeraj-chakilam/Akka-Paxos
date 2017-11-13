module ChatServer.Replica

open Akka.FSharp
open Akka.Actor
open Types

type State = {
    slotNum: int64
    /// Map (Slot, Command)
    proposals: Map<int64, Command>
    /// Map (Slot, Command)
    decisions: Map<int64, Command>
    leaders: Set<IActorRef>
    master: IActorRef option
    messages: List<Command>
}

type ReplicaMessage =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | Get
    | Request of Command
    /// Decision (Slot, Command)
    | Decision of int64 * Command
    | Leave of IActorRef

let propose command state =
    printfn "Trying to propose %A with proposals: %A decisions: %A" command state.proposals state.decisions
    let rec findGap state i =
        if (Map.containsKey i state.proposals) || (Map.containsKey i state.decisions) then
            findGap state (i + 1L)
        else
            i
    if not (Map.exists (fun _ c -> c = command) state.decisions) then
        let newSlot = findGap state 0L
        let proposals' = Map.add newSlot command state.proposals
        Set.iter (fun r -> r <! sprintf "propose %i %i %s" newSlot command.id command.message) state.leaders
        { state with proposals = proposals' }
    else
        state

let perform command state =
    let prevPerform =
        state.decisions
        |> Map.filter (fun s _ -> s < state.slotNum)
        |> Map.exists (fun _ c -> command = c)

    if prevPerform then
        { state with slotNum = state.slotNum + 1L }
    else
        match state.master with
        | Some m -> m <! sprintf "ack %i %i" command.id (List.length state.messages)
        | None -> ()
        { state with
                slotNum = state.slotNum + 1L ;
                messages = command :: state.messages }

let replica (selfID: int64) beatrate (mailbox: Actor<ReplicaMessage>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->            
            return! loop { state with leaders = Set.add ref state.leaders }

        | JoinMaster ref ->
            return! loop { state with master = Some ref }

        | Leave ref ->
            return! loop { state with leaders = Set.remove ref state.leaders }

        | Get ->
            match state.master with
            | Some m ->
                let messageList = List.map (fun c -> c.message )state.messages
                m <! (sprintf "chatLog %s" (System.String.Join(",",List.rev messageList)))
            | None -> ()

            return! loop state

        | Request command->
            // On Request, Propose
            let state' = propose command state
            return! loop state'
        
        | Decision (s, p) ->
            printfn "Replica %i received a decision with slot: %A command: %A" selfID s p
            // Recursive function to Perform all possible commands (until gap)
            let rec performPossibleCommands state =
                printfn "In performPossibleCommands"
                match Map.tryFind state.slotNum state.decisions with
                | Some p' ->
                    let state =
                        match Map.tryFind state.slotNum state.proposals with
                        | Some p'' when p'' <> p' ->
                            propose p'' state
                        | _ ->
                            state
                    state
                    |> perform p'
                    |> performPossibleCommands
                | None ->
                    state
            
            let state =
                { state with decisions = Map.add s p state.decisions }
                |> performPossibleCommands
            printfn "Out of decision with proposals: %A decisions: %A" state.proposals state.decisions
            return! loop state
    }

    loop {
        leaders = Set.empty
        master = None
        messages = []
        proposals = Map.empty
        decisions = Map.empty
        slotNum = 0L
    }