open Hopac
open Hopac.Infixes
open System

type State = { Count: int }

[<EntryPoint>]
let main _ = 
    let worker alt =
        Job.iterateServer { Count = 0 } (fun s -> 
            alt ^-> fun _ -> { s with Count = s.Count + 1 }
        ) |> queue

    let ch1, ch2 = Ch(), Ch()

    let _worker1 = worker <| Alt.choose [ch1; ch2]
    let _worker2 = worker <| Alt.choose [ch2; ch1]

    while true do
        printfn "Press a key to send a task"
        Console.ReadKey() |> ignore
        ch1 *<- () |> run
    0