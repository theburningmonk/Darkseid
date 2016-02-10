#r "bin/Debug/AWSSDK.dll"
#r "bin/Debug/Darkseid.dll"
#r "bin/Debug/log4net.dll"

open System
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Amazon
open Amazon.DynamoDBv2.DocumentModel
open Amazon.Kinesis.Model
open log4net
open log4net.Config
open Darkseid
open Darkseid.Model

let awsKey      = "AKIAJ27RLPDDIA5Z2NLQ"
let awsSecret   = "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"
let region      = RegionEndpoint.USEast1
let streamName  = "YC-test"

BasicConfigurator.Configure()

let kinesis = Amazon.AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region) 
let cloudWatch = Amazon.AWSClientFactory.CreateAmazonCloudWatchClient(awsKey, awsSecret, region)

let config = new DarkseidConfig(LevelOfConcurrency = 100u)
let producer = Producer.CreateNew(kinesis, cloudWatch, "YC-Test", streamName, config)

let payload = [| 1..5000 |] |> Array.map (fun _ -> "42") |> Array.reduce (+) |> System.Text.Encoding.UTF8.GetBytes

let send () =
    let record = { Data = payload; PartitionKey = Guid.NewGuid().ToString() }
    producer.SendAsync(record)

[| 1..1000 |] |> Array.iter (fun _ -> Task.Run(fun () -> send()) |> ignore)