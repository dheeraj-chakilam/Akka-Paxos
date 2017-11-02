module ChatServer.Replica

open Akka.FSharp
open Akka.Actor

type State = {
    leaders: Set<IActorRef>
    master: IActorRef option
    messages: List<string>
    beatmap: Map<string,IActorRef*int64>
}

type Message =
    | Join of IActorRef
    | JoinMaster of IActorRef
    | Heartbeat of string * IActorRef * int64
    | Alive of int64 * string
    | Request of string * string
    | Decision of string
    | Get
    | Leave of IActorRef

let room selfID beatrate aliveThreshold (mailbox: Actor<Message>) =
    let rec loop state = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        match msg with
        | Join ref ->
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly(System.TimeSpan.FromMilliseconds 0.,
                                            System.TimeSpan.FromMilliseconds beatrate,
                                            ref,
                                            sprintf "heartbeat %s" selfID)
            
            return! loop { state with actors = Set.add ref state.actors }

        | JoinMaster ref ->
            return! loop { state with master = Some ref }

        | Heartbeat (id, ref, ms) ->
            printfn "heartbeat %s" id
            return! loop { state with beatmap = state.beatmap |> Map.add id (ref,ms) }

        | Alive (currMs, selfID) ->
            match state.master with
            | Some m -> 
                let aliveList =
                    state.beatmap
                    |> Map.filter (fun _ (_, ms) -> currMs - ms < aliveThreshold)
                    |> Map.add selfID (Unchecked.defaultof<_>, Unchecked.defaultof<_>)
                    |> Map.toList
                    |> List.map (fun (id,_) -> id)
                m <! (sprintf "alive %s" (System.String.Join(",",aliveList)))
            | None -> ()

            return! loop state

        | Broadcast text -> 
            state.actors
            |> Set.iter (fun a -> a <! (sprintf "rebroadcast %s" text))
            
            return! loop { state with messages = text :: state.messages }

        | Rebroadcast text ->
            return! loop { state with messages = text :: state.messages }

        | Get ->
            match state.master with
            | Some m -> m <! (sprintf "messages %s" (System.String.Join(",",List.rev state.messages)))
            | None -> ()
            
            return! loop state

        | Leave ref ->
            return! loop { state with actors = Set.remove ref state.actors }
    }
    loop { actors = Set.empty ; master = None ; messages = []; beatmap = Map.empty }