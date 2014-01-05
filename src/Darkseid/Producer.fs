namespace Darkseid

open System
open System.Collections.Generic
open System.Globalization
open System.IO
open System.Security.Cryptography
open System.Threading
open System.Timers

open Amazon.CloudWatch
open Amazon.CloudWatch.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model
open log4net

open Checked
open Darkseid.Model
open Darkseid.Utils

type internal VirmanVundabar (cloudWatch : IAmazonCloudWatch,
                              config     : DarkseidConfig,
                              appName    : string,
                              streamName : string,
                              cts        : CancellationTokenSource) =
    let loggerName = sprintf "VirmanVundabar[AppName:%s, Stream:%s]" appName streamName
    let logger     = LogManager.GetLogger loggerName

    // this metric name and dimensions would make our custom metrics go into the same place as the rest of the Kinesis metrics
    let metricNs, streamMetricDim, shardMetricDim = "AWS/Kinesis", "StreamName", "ShardId"
    let sendMetricName, throttledMetricName = "PutRecord.Success", "PutRecord.Throttled"

    let streamDimensions = [| new Dimension(Name = streamMetricDim, Value = streamName) |]

    let getShardDimensions =
        let getInternal shardId = [| new Dimension(Name = streamMetricDim, Value = streamName)
                                     new Dimension(Name = shardMetricDim,  Value = shardId) |]
        memoize getInternal

    let body (inbox : Agent<VirmanMessage>) =
        // since CloudWatch's lowest granularity is 1 minute, so let's batch up count metrics to the minute interval
        let getPeriodId (timestamp : DateTime) = uint64 <| timestamp.ToString("yyyyMMddHHmm")
        let getMinuteTimestamp (periodId : uint64) = DateTime.ParseExact(string periodId, "yyyyMMddHHmm", CultureInfo.InvariantCulture)

        let metricsData = new Dictionary<uint64 * string, Metric>()

        async {
            while true do
                let! msg = inbox.Receive()
                match msg with
                | IncrCount(timestamp, dims, metricName, count) ->
                    let periodId = getPeriodId timestamp
                    let key = periodId, metricName
                    match metricsData.TryGetValue key with
                    | true, metric -> metric.AddDatapoint(double count)
                    | _ -> let timestamp = getMinuteTimestamp periodId
                           metricsData.[key] <- Metric.Init(timestamp, dims, metricName, double count)
                | Flush(reply) ->
                    reply.Reply(metricsData.Values |> Seq.toArray)
                    metricsData.Clear()
        }
    let agent = Agent<VirmanMessage>.Start(body, cts.Token)
    do agent.Error.Add(fun exn -> logger.Warn("Encountered an unhandled error. Ignoring.", exn))
    
    let pushMetrics = 
        async {
            let! metrics = agent.PostAndAsyncReply Flush
            do! CloudWatchUtils.pushMetrics cloudWatch metricNs metrics
        }

    let timer = new Timer(TimeSpan.FromMinutes(1.0).TotalMilliseconds)
    do timer.Elapsed.Add(fun _ -> Async.Start(pushMetrics, cts.Token))
    do timer.Start()

    member this.TrackThrottledSend (partitionKey : string) = 
        agent.Post <| IncrCount(DateTime.UtcNow, streamDimensions, throttledMetricName, 1)

    member this.TrackSuccessfulSend (shardId : string, payloadSize : int) = 
        let dimensions = getShardDimensions shardId
        agent.Post <| IncrCount(DateTime.UtcNow, dimensions, sendMetricName, 1)

type internal AeroTrooper (kinesis    : IAmazonKinesis, 
                           appName    : string,
                           streamName : string,
                           cts        : CancellationTokenSource) =
    let loggerName = sprintf "AeroTrooper[AppName:%s, Stream:%s]" appName streamName
    let logger     = LogManager.GetLogger loggerName
    
    let successEvent   = new Event<string * int>()
    let errorEvent     = new Event<Exception>()
    let throttledEvent = new Event<string>()
    
    let rec send record attempts =
        async {
            try
                use memStream = new MemoryStream(record.Data)
                let  req = new PutRecordRequest(Data = memStream, PartitionKey = record.PartitionKey)
                let! res = kinesis.PutRecordAsync(req, cts.Token) |> Async.AwaitTask
                successEvent.Trigger(res.ShardId, record.Data.Length)
            with
            | Flatten (:? ProvisionedThroughputExceededException as exn) ->
                throttledEvent.Trigger(record.PartitionKey)
                logger.Warn(sprintf "PutRecord request attempt [%d] is throttled, retrying in 100ms..." attempts, exn)

                do! Async.Sleep 100
                do! send record (attempts + 1)
            | Flatten exn ->
                errorEvent.Trigger(exn)
                logger.Warn(sprintf "PutRecord request attempt [%d] encountered an error, retrying..." attempts, exn)

                do! send record (attempts + 1)
        }

    let body (inbox : Agent<AeroTrooperMessage>) =
        async {
            while true do
                let! msg = inbox.Receive()
                match msg with | PutRecord record -> do! send record 1
        }

    let agent = Agent<AeroTrooperMessage>.Start(body, cts.Token)
    do agent.Error.Add(fun exn -> logger.Warn("Encountered an unhandled error. Ignoring.", exn))

    member this.OnSuccess   = successEvent.Publish
    member this.OnError     = errorEvent.Publish
    member this.OnThrottled = throttledEvent.Publish

    member this.Send (record) = agent.Post <| PutRecord record

type internal GloriousGodfrey (kinesis    : IAmazonKinesis,
                               config     : DarkseidConfig,
                               appName    : string,
                               streamName : string,
                               cts        : CancellationTokenSource,
                               virman     : VirmanVundabar) = 
    let loggerName = sprintf "GloriousGodfrey[AppName:%s, Stream:%s]" appName streamName
    let logger     = LogManager.GetLogger loggerName

    let backlogSize = 0
    let incrBacklog () = Interlocked.Increment(ref backlogSize) |> ignore
    let decrBacklog () = Interlocked.Decrement(ref backlogSize) |> ignore

    /// A simple hash algorithm which uses the MD5 crypto service provider to hash the provided
    /// data and returns an uint value (4 bytes) from the first 4 bytes of the hashed data.
    let hash partitions (data : byte[]) =
        use md5  = new MD5CryptoServiceProvider()
        let hash = md5.ComputeHash(data)
        let n = (uint32 hash.[3] <<< 24) ||| (uint32 hash.[2] <<< 16) ||| (uint32 hash.[1] <<< 8) ||| (uint32 hash.[0])
        n % partitions

    let genTrooper _ = 
        let trooper = new AeroTrooper(kinesis, appName, streamName, cts)
        trooper.OnSuccess.Add(fun (shardId, payloadSize) -> 
            decrBacklog()
            virman.TrackSuccessfulSend(shardId, payloadSize))
        trooper.OnThrottled.Add(virman.TrackThrottledSend)

        trooper

    let body (inbox : Agent<GodfreyMessage>) =
        let troopers = [| 1u..config.LevelOfConcurrency |] |> Array.map genTrooper

        // employ a simple hashing algorithm to 
        let send ({ PartitionKey = partitionKey } as record) =
            let idx     = partitionKey |> System.Text.Encoding.UTF8.GetBytes |> hash config.LevelOfConcurrency |> int
            let trooper = troopers.[idx]
            trooper.Send(record)
            incrBacklog()

        async {
            while true do
                let! msg = inbox.Receive()
                match msg with
                | Send (record, reply) -> 
                    match backlogSize >= config.HighWaterMarks with
                    | true ->
                        match config.HighWaterMarksMode with
                        | HighWaterMarksMode.DropData -> 
                            reply.Reply()
                        | HighWaterMarksMode.Block    -> 
                            // block until we're below the high water mark
                            while backlogSize >= config.HighWaterMarks do
                                do! Async.Sleep(10)

                            send record
                            reply.Reply()
                        | x -> raise <| NotSupportedException(sprintf "Unknown HighWaterMarksMode [%O] is not supported." x)
                    | _ -> send record
                           reply.Reply()
        }

    let agent = Agent<GodfreyMessage>.Start(body, cts.Token)
    do agent.Error.Add(fun exn -> logger.Warn("Encountered an unhandled error. Ignoring.", exn))

    member this.Send (record) = agent.PostAndReply (fun reply -> Send(record, reply))

type Producer private (kinesis      : IAmazonKinesis,
                       cloudWatch   : IAmazonCloudWatch,
                       config       : DarkseidConfig,
                       appName      : string,
                       streamName   : string) =    
    let loggerName = sprintf "Darkseid[AppName:%s, Stream:%s]" appName streamName
    let logger     = LogManager.GetLogger loggerName

    let cts = new CancellationTokenSource()
 
    let virman  = new VirmanVundabar(cloudWatch, config, appName, streamName, cts)
    let godfrey = new GloriousGodfrey(kinesis, config, appName, streamName, cts, virman)

    member this.Send (record : Record) = godfrey.Send(record)