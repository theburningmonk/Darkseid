// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open System
open System.IO
open System.Text
open System.Threading
open Amazon
open Amazon.DynamoDBv2.DocumentModel
open Amazon.Kinesis.Model
open log4net
open log4net.Config
open Darkseid
open Darkseid.Model

[<EntryPoint>]
let main argv =  
    let appName     = "YC-TEST"
    let streamName  = "YC-test"
   
    BasicConfigurator.Configure()

    let kinesis    = Amazon.AWSClientFactory.CreateAmazonKinesisClient()
    let cloudWatch = Amazon.AWSClientFactory.CreateAmazonCloudWatchClient()
    
    let bgConfig = BackgroundProcessingConfig(HighWaterMarks = 100000, HighWaterMarksMode = HighWaterMarksMode.Block)
    let mode   = Background bgConfig
    let config = new DarkseidConfig(Mode = mode, LevelOfConcurrency = 100u)
    let producer = Producer.CreateNew(kinesis, cloudWatch, appName, streamName, config)

    let payload = [| 1..3 |] |> Array.map (fun _ -> "42") |> Array.reduce (+) |> System.Text.Encoding.UTF8.GetBytes

    let send () =
        let record = { Data = payload; PartitionKey = Guid.NewGuid().ToString() }
        producer.Send(record)

    let loop = 
        async {
            while true do
                send().Wait()
                do! Async.Sleep(1)
        }

    printfn "Starting 10 send loops..."

    for i = 1 to 10 do
        do Async.Start(loop)

    printfn "Started."

    printf "Press any key to stop..."
    Console.ReadKey() |> ignore

    0 // return an integer exit code