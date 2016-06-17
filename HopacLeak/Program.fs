open Hopac
open Hopac.Infixes
open System

type Task =
    { Id: int
      Result: IVar<unit> }

module Worker =
    type State =
        { Capacity: int
          Load: int }
        member x.IsFull = x.Load >= x.Capacity

    let spawn (input: Alt<Task>) (capacity: int) =
        let resultCh = Ch()
        let getStateCh = Ch()

        Job.iterateServer { Capacity = capacity; Load = 0 } (fun state ->
            let resultAlt = resultCh ^-> fun () -> { state with Load = state.Load - 1 }
            let getStateAlt = getStateCh *<- state ^->. state

            if state.IsFull then
                getStateAlt <|> resultAlt
            else
                getStateAlt
                <|>
                (input ^=> fun task ->
                     let state = { state with Load = state.Load + 1 }
                     Job.tryInDelay 
                         (fun _ -> job { return () })
                         (IVar.fill task.Result)
                         (IVar.fillFailure task.Result)
                     >>=. resultCh *<+ ()
                     |> Job.queue
                     >>-. state
                )
                <|>
                resultAlt
        )
        |> queue
        getStateCh

type SlotTaskPool =
    { Add: Task -> Job<unit>
      Get: Alt<Task> }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module SlotTaskPool =
    let create () =
        let addTaskCh = Ch<Task>()
        let getTaskCh = Ch<Task>()

        Job.iterateServer (Map.empty, []) (fun (mbsByTaskId: Map<int, Mailbox<_>>, mbs: Mailbox<_> list) ->
            (addTaskCh ^=> fun task ->
                 match mbsByTaskId |> Map.tryFind task.Id with
                 | Some mb -> mb *<<+ task >>-. (mbsByTaskId, mbs)
                 | None ->
                     let mb = Mailbox()
                     mb *<<+ task >>-. (mbsByTaskId |> Map.add task.Id mb, mb :: mbs)
            )
            <|>
            Alt.chooser mbs ^=> fun task ->
                Ch.give getTaskCh task >>-. (mbsByTaskId, mbs)
        )
        |> queue

        { Add = Ch.send addTaskCh
          Get = getTaskCh }

type TaskPool =
    { Add: Task -> Job<unit>
      CreateSource: unit -> Alt<Task> }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TaskPool =
    let create () = 
        let slotTaskPool = SlotTaskPool.create()

        { Add = fun task -> slotTaskPool.Add task
          CreateSource = fun () -> slotTaskPool.Get }

type AsyncExecutor() =
    let taskPool = TaskPool.create()
    let worker = Worker.spawn (taskPool.CreateSource()) 10
    member __.Perform (task: Task) = taskPool.Add task >>=. task.Result

[<EntryPoint>]
let main argv = 
    let executor = AsyncExecutor()
    while true do
        printfn "Press a key to process a task"
        Console.ReadKey() |> ignore
        executor.Perform { Id = 2; Result = IVar() } |> run
    0