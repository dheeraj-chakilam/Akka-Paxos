module ChatServer.Replica

open Akka.FSharp
open Akka.Actor
open Types

type State = {
    slotNum: int64
    proposals: Map<int64, Command>
    decisions: Map<int64, Command>
    leaders: Set<IActorRef>
    master: IActorRef option
    messages: List<Command>
    beatmap: Map<string,IActorRef*int64>
}

type ReplicaMessage =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | Heartbeat of string * IActorRef * int64
    | Get
    | Request of Command
    /// Decision (Slot, Command)
    | Decision of int64 * Command
    | Leave of IActorRef

let propose command state =
    printfn "Trying to propose %O" command
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
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(System.TimeSpan.FromMilliseconds 0.,
                                            System.TimeSpan.FromMilliseconds beatrate,
                                            ref,
                                            sprintf "heartbeat %i" selfID)
            
            return! loop { state with leaders = Set.add ref state.leaders }

        | JoinMaster ref ->
            return! loop { state with master = Some ref }

        | Heartbeat (id, ref, ms) ->
            //printfn "replica heartbeat %s" id
            return! loop { state with beatmap = state.beatmap |> Map.add id (ref,ms) }

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
        
        | Decision (slot, command) ->
            printfn "Replica %i received a decision" selfID
            // Recursive function to Perform all possible commands (until gap)
            let rec performPossibleCommands st = 
                let commandInDecision = 
                    st.decisions
                    |> Map.containsKey st.slotNum
                
                if not commandInDecision then
                    st
                else
                    let commandInProposal =
                        st.proposals
                        |> Map.containsKey st.slotNum
                    
                    // We ensure on re-proposal that the decision was not our proposal
                    let st' = 
                        if not commandInProposal then
                            st
                        else
                            let c1 = Map.find st.slotNum st.decisions
                            let c2 = Map.find st.slotNum st.proposals
                      
                            if c1 <> c2 then
                                propose c2 st
                            else
                                st
                    let st'' = perform command st'
                    performPossibleCommands st''
            
            let state' = { state with decisions = Map.add slot command state.decisions }
            let state'' = performPossibleCommands state'
            return! loop state''
    }

    loop {
        leaders = Set.empty
        master = None
        messages = []
        beatmap = Map.empty
        proposals = Map.empty
        decisions = Map.empty
        slotNum = 0L
    }