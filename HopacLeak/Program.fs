open Hopac
open Hopac.Extensions
open Hopac.Infixes
open System

type File = 
    { Id: int
      Depth: int }

type FileWithHash =
    { File: File
      Hash: byte[] }

type ExtractResult = 
    { ExtractedFiles: File seq }

type UnpackResult =
    { File: File
      Children: FileWithHash seq }

type SlotId = SlotId of int
type TaskId = TaskId of int

type Task =
    { Id: TaskId
      SlotId: SlotId
      File: File
      Result: ExtractResult IVar }

module Worker =
    type State =
        { Capacity: int
          Load: int }
        member x.IsFull = x.Load >= x.Capacity

    let spawn (input: Alt<Task>) (capacity: int) =
        let resultCh = Ch()
        let getStateCh = Ch()

        Job.iterateServer { Capacity = capacity; Load = 0 } (fun state ->
        Job.tryWithDelay (fun _ ->
            let resultAlt = resultCh ^-> fun _ -> { state with Load = state.Load - 1 }
            let getStateAlt = getStateCh *<- state ^->. state

            if state.IsFull then
                getStateAlt <|> resultAlt
            else
                getStateAlt
                <|>
                (input ^-> fun task ->
                     let state = { state with Load = state.Load + 1 }
                     Job.tryFinallyJobDelay (fun _ ->
                        Job.tryInDelay
                            (fun _ -> 
                                 job {
                                     return 
                                        { ExtractedFiles = 
                                              if task.File.Depth < 5 then 
                                                  [ { Id = task.File.Id; Depth = task.File.Depth + 1 } 
                                                    { Id = task.File.Id; Depth = task.File.Depth + 1 } ] 
                                              else [] }
                                 })
                            (IVar.fill task.Result)
                            (IVar.fillFailure task.Result))
                         (resultCh *<- ())
                     |> queue 
                     state
                )
                <|>
                resultAlt
            )
            (fun e ->
                job { 
                    printfn "%O" e 
                    return state 
                })
        )
        |> queue
        getStateCh

type SlotTaskPool =
    { SlotId: SlotId
      Add: Task -> Job<unit>
      Get: Alt<Task> }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module SlotTaskPool =
    let create (slotId: SlotId) =
        let addTaskCh = Ch<Task>()
        let getTaskCh = Ch<Task>()

        Job.iterateServer (Map.empty, []) (fun (mbsByTaskId: Map<TaskId, Mailbox<_>>, mbs: Mailbox<_> list) ->
            (addTaskCh ^=> fun task ->
                 match mbsByTaskId |> Map.tryFind task.Id with
                 | Some mb -> mb *<<+ task >>-. (mbsByTaskId, mbs)
                 | None ->
                     let mb = Mailbox()
                     mb *<<+ task >>-. (mbsByTaskId |> Map.add task.Id mb, mb :: mbs)
            )
            <|>
            Alt.chooser mbs ^=> fun task -> getTaskCh *<- task >>-. (mbsByTaskId, mbs)
        )
        |> queue

        { SlotId = slotId
          Add = Ch.send addTaskCh
          Get = getTaskCh }

type TaskPool =
    { Add: Task -> Job<unit>
      CreateSource: unit -> Alt<Task> }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TaskPool =
    let create() = 
        let slotTaskPools = 
            [1; 2] |> Seq.map SlotId |> Seq.map (fun slot -> slot, SlotTaskPool.create slot) |> Map.ofSeq
        
        { Add = fun task -> 
            match slotTaskPools |> Map.tryFind task.SlotId with
            | Some taskPool -> taskPool.Add task
            | None -> 
                printfn "Slot with id = %A was not found, task has lost" task.SlotId
                Job.result()
          
          CreateSource = fun () ->
            slotTaskPools |> Map.toList |> List.map (fun (_, x) -> x.Get) |> Alt.choose }

type AsyncExtractor() =
    let taskPool = TaskPool.create()
    
    let workers = 
        [ Worker.spawn (taskPool.CreateSource()) 10
          Worker.spawn (taskPool.CreateSource()) 10 ]

    member __.Perform (task: Task) = taskPool.Add task >>=. task.Result
    member __.GetState() = workers |> Job.conCollect >>- Seq.toArray

module Unpacker =
    let unpackOneLevel (asyncExecutor: AsyncExtractor) (file: File) : UnpackResult Job =
        job {
            let task = 
                { Id = TaskId 1
                  SlotId = SlotId 1
                  File = file
                  Result = IVar() }

            let! result = asyncExecutor.Perform task
            let saveFile (_: File) : byte array Job = job { return [||] }
                
            let! children =
                    result.ExtractedFiles
                    |> Seq.Con.mapJob (fun file ->
                        saveFile file >>- fun hash ->
                            { File = file
                              Hash = hash })
            return 
                { File = file
                  Children = children }
        }
    
    let rec private recursionStep asyncExecutor (file: FileWithHash) resultCh : unit Job =
        job {
            let! result = unpackOneLevel asyncExecutor file.File
            do! resultCh *<+ Some result
    
            do! result.Children
                |> Seq.Con.iterJobIgnore (fun child -> 
                    recursionStep asyncExecutor child resultCh)
        }
    
    let unpack (file: FileWithHash) (asyncExecutor: AsyncExtractor) : UnpackResult option Ch Job =
        let resultCh = Ch()
    
        Job.tryFinallyJob
            (recursionStep asyncExecutor file resultCh)
            (resultCh *<+ None)
        |> Job.queue
        >>-. resultCh

module ReportBuilder =
    let sendReport (fileUnpackResults: UnpackResult option Ch): unit Job =
        let appendPart x = printfn "Part: %A" x
        let sendReport () = printfn "Report was sent."
        
        job {
            let rec loop() =
                job {
                    let! r = fileUnpackResults
                    match r with
                    | Some r -> 
                        appendPart r
                        return! loop()
                    | None -> ()
                }
    
            do! loop()
            sendReport ()
        }

module Top =
    let perform (asyncExecutor: AsyncExtractor) id =
        try
            Unpacker.unpack { File = { Id = id; Depth = 0 }; Hash = [||] } asyncExecutor
            >>= ReportBuilder.sendReport
            |> run
        with
        | e when e.InnerException <> null -> raise e.InnerException
        | _ -> reraise()

[<EntryPoint>]
let main _ = 
    let executor = AsyncExtractor()
    for id in 1..1000000 do
        printfn "Press a key to process a task with id = %d" id
        Console.ReadKey() |> ignore
        Top.perform executor id
    0