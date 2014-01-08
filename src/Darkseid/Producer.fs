namespace Darkseid

open System
open System.Collections.Generic
open System.Globalization
open System.IO
open System.Security.Cryptography
open System.Threading
open System.Timers

open Amazon
open Amazon.CloudWatch
open Amazon.CloudWatch.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model
open log4net

open Checked
open Darkseid.Model
open Darkseid.Utils

type internal VirmanVundabar (kinesis    : IAmazonKinesis,
                              cloudWatch : IAmazonCloudWatch,
                              config     : DarkseidConfig,
                              appName    : string,
                              streamName : string,
                              cts        : CancellationTokenSource) =
    let loggerName = sprintf "VirmanVundabar[AppName:%s, Stream:%s]" appName streamName
    let logger     = LogManager.GetLogger loggerName

    let streamDim  = new Dimension(Name = CloudWatchUtils.streamDimensionName, Value = streamName)
    let streamDims = [| streamDim |]

    let getShardDims =
        let getInternal shardId = [| new Dimension(Name = CloudWatchUtils.streamDimensionName, Value = streamName)
                                     new Dimension(Name = CloudWatchUtils.shardDimensionName,  Value = shardId) |]
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
                | IncrMetric(timestamp, dims, unit, metricName, n) ->
                    let periodId = getPeriodId timestamp
                    let key = periodId, metricName
                    match metricsData.TryGetValue key with
                    | true, metric -> metric.AddDatapoint(double n)
                    | _ -> let timestamp = getMinuteTimestamp periodId
                           metricsData.[key] <- Metric.Init(timestamp, dims, unit, metricName, double n)
                | Flush(reply) ->
                    reply.Reply(metricsData.Values |> Seq.toArray)
                    metricsData.Clear()
        }
    let agent = Agent<VirmanMessage>.Start(body, cts.Token)
    do agent.Error.Add(fun exn -> logger.Warn("Encountered an unhandled error. Ignoring.", exn))
    
    let pushMetrics = 
        async {
            let! metrics = agent.PostAndAsyncReply Flush
            do! CloudWatchUtils.pushMetrics cloudWatch metrics
        }

    let splitBusiestShard = 
        async {
            let! res = KinesisUtils.getShards kinesis streamName
            match res with
            | Success("ACTIVE", shards, shardIds) -> 
                // look at both the send request count and total bytes sent to determine the shard that is closest to the limits
                // imposed by Kinesis
                let! sendAvg = CloudWatchUtils.getAvgShardMetrics cloudWatch streamDim shardIds CloudWatchUtils.sendMetricName
                let! sizeAvg = CloudWatchUtils.getAvgShardMetrics cloudWatch streamDim shardIds CloudWatchUtils.sizeMetricName

                match sendAvg, sizeAvg with
                | Failure exn, _ | _, Failure exn -> 
                    logger.Error("Couldn't retrieve CloudWatch metrics for the shards. Cannot determine which shard to split right now.", exn)
                | Success sendMetrics, Success sizeMetrics -> 
                    let busiestShardId = shardIds |> Seq.maxBy (fun shardId -> 
                        // NOTE : the metrics are per minute but the limitations are per second, hence the need to divide by 60
                        let sendPerc = (sendMetrics |> Map.getOrDefault shardId 0.0) / 60.0 / KinesisUtils.maxPutRequestsPerSecond
                        let sizePerc = (sizeMetrics |> Map.getOrDefault shardId 0.0) / 60.0 / KinesisUtils.maxPutBytesPerSecond                        
                        max sendPerc sizePerc)

                    logger.DebugFormat("Shard [{0}] is the busiest shard at the moment, attempting to split it.", busiestShardId)
                    let shard = shards |> Seq.find (fun shard -> shard.ShardId = busiestShardId)
                    do! KinesisUtils.splitShard kinesis streamName shard
            | Success(status, _, _) 
                -> logger.WarnFormat("Stream is currenty in [{0}] status. Stream needs to be ACTIVE status to split shards.", status)
            | Failure exn 
                -> logger.Error("Couldn't retrieve current list of shards. Cannot determine which shard to split right now.", exn)
        }

    let checkThrottles =
        async {
            let! res = CloudWatchUtils.getAvgStreamMetric cloudWatch streamDim CloudWatchUtils.throttledMetricName
            match res with
            | Success avgThrottle -> 
                if avgThrottle > 100.0 then do! splitBusiestShard
            | Failure exn -> logger.Error(sprintf "Couldn't get throttle metrics for the stream [%s]" streamName, exn)
        }

    let startTimer onElapsed (interval : TimeSpan)  = 
        let timer = new Timer(interval.TotalMilliseconds)
        do timer.Elapsed.Add onElapsed
        do timer.Start() 
        timer

    let pushTimer  = TimeSpan.FromMinutes(1.0) |> startTimer (fun _ -> Async.Start(pushMetrics, cts.Token))
    let splitTimer = TimeSpan.FromMinutes(1.0) |> startTimer (fun _ -> Async.Start(checkThrottles, cts.Token))
    let mergeTimer = TimeSpan.FromMinutes(5.0) |> startTimer (fun _ -> ())

    member this.TrackThrottledSend (partitionKey : string) = 
        agent.Post <| IncrMetric(DateTime.UtcNow, streamDims, StandardUnit.Count, CloudWatchUtils.throttledMetricName, 1)

    member this.TrackSuccessfulSend (shardId : string, payloadSize : int) = 
        let shardDims = getShardDims shardId
        agent.Post <| IncrMetric(DateTime.UtcNow, shardDims, StandardUnit.Count, CloudWatchUtils.sendMetricName, 1)
        agent.Post <| IncrMetric(DateTime.UtcNow, shardDims, StandardUnit.Bytes, CloudWatchUtils.sizeMetricName, payloadSize)

type internal AeroTrooper (kinesis    : IAmazonKinesis,
                           config     : DarkseidConfig, 
                           appName    : string,
                           streamName : string,
                           cts        : CancellationTokenSource) =
    let loggerName = sprintf "AeroTrooper[AppName:%s, Stream:%s]" appName streamName
    let logger     = LogManager.GetLogger loggerName
    
    let successEvent   = new Event<string * int>()
    let errorEvent     = new Event<Record * Exception>()
    let failedEvent    = new Event<Record>()
    let throttledEvent = new Event<string>()
    
    let rec send record attempts =
        async {            
            if attempts >= config.MaxPutRecordAttempts 
            then failedEvent.Trigger(record)
            else
                try
                    use memStream = new MemoryStream(record.Data)
                    let  req = new PutRecordRequest(StreamName = streamName, Data = memStream, PartitionKey = record.PartitionKey)
                    let! res = kinesis.PutRecordAsync(req, cts.Token) |> Async.AwaitTask
                    successEvent.Trigger(res.ShardId, record.Data.Length)
                with
                | Flatten (:? ProvisionedThroughputExceededException as exn) ->
                    throttledEvent.Trigger(record.PartitionKey)
                    logger.Warn(sprintf "PutRecord request attempt [%d] is throttled, retrying in 100ms..." attempts, exn)

                    do! Async.Sleep 100
                    do! send record (attempts + 1)
                | Flatten exn ->
                    errorEvent.Trigger(record, exn)
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

    let errorEvent = new Event<Record * Exception>()

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
        let trooper = new AeroTrooper(kinesis, config, appName, streamName, cts)
        trooper.OnSuccess.Add(fun (shardId, payloadSize) -> 
            decrBacklog()
            virman.TrackSuccessfulSend(shardId, payloadSize))
        trooper.OnThrottled.Add(virman.TrackThrottledSend)
        trooper.OnError.Add(errorEvent.Trigger)

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

    let errorEvent = new Event<Record * Exception>()

    let cts = new CancellationTokenSource()
 
    let virman  = new VirmanVundabar(kinesis, cloudWatch, config, appName, streamName, cts)
    let godfrey = new GloriousGodfrey(kinesis, config, appName, streamName, cts, virman)

    member this.Send (record : Record) = godfrey.Send(record)
    
    static member CreateNew(awsKey    : string, 
                            awsSecret : string, 
                            region    : RegionEndpoint, 
                            appName, streamName) =
        let config     = new DarkseidConfig()
        let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
        let cloudWatch = AWSClientFactory.CreateAmazonCloudWatchClient(awsKey, awsSecret, region)
        new Producer(kinesis, cloudWatch, config, appName, streamName)

    static member CreateNew(awsKey    : string, 
                            awsSecret : string, 
                            region    : RegionEndpoint, 
                            appName, streamName, config) =
        let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
        let cloudWatch = AWSClientFactory.CreateAmazonCloudWatchClient(awsKey, awsSecret, region)
        new Producer(kinesis, cloudWatch, config, appName, streamName)

    static member CreateNew(kinesis, cloudWatch, appName, streamName) =
        let config = new DarkseidConfig()
        new Producer(kinesis, cloudWatch, config, appName, streamName)

    static member CreateNew(kinesis, cloudWatch, appName, streamName, config) =
        new Producer(kinesis, cloudWatch, config, appName, streamName)

    [<CLIEvent>] member this.OnError = errorEvent.Publish