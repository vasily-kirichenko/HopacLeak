#r @"..\packages\Hopac.0.2.1\lib\net45\Hopac.Platform.dll"
#r @"..\packages\Hopac.0.2.1\lib\net45\Hopac.Core.dll"
#r @"..\packages\Hopac.0.2.1\lib\net45\Hopac.dll"

open Hopac
open Hopac.Infixes

let mb1 = Mailbox<int>()
let mb2 = Mailbox<int>()
let mbs = [mb1; mb2]

let a, b = mbs |> List.partition ((=) mb1)

mb1 *<<+ 1 |> run

[1; 2; 1; 3] |> List.partition ((=) 1) |> snd