#r "bin/Debug/AWSSDK.dll"
#r "bin/Debug/Darkseid.dll"
#r "bin/Debug/log4net.dll"

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

let awsKey      = "AKIAIF4IYIIJKW6DURPQ"
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
    producer.Send(record)

[| 1..1000 |] |> Array.iter (fun _ -> send())