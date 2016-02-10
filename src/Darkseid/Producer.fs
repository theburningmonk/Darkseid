namespace Darkseid

open System
open System.Collections.Generic
open System.Globalization
open System.IO
open System.Security.Cryptography
open System.Threading
open System.Threading.Tasks
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

/// Represents a producer of data for Amazon Kinesis
type IProducer =
    inherit IDisposable

    /// Fired when the application has been initialized
    [<CLIEvent>]
    abstract member OnError : IEvent<OnErrorDelegate, OnErrorEventArgs>

    /// Sends a data record to Kinesis, depending on the configured mode the task will complete:
    ///  1) Blocking mode   : when data is sent to Kinesis
    ///  2) Background mode : when data is accepted into the backlog
    abstract member SendAsync    : Record -> Task

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
        let getConsecutiveBreaches (dataPoints : List<Datapoint>) =
            dataPoints 
            |> Seq.sortBy (fun dp -> -dp.Timestamp.Ticks)
            |> Seq.takeWhile (fun dp -> dp.Sum >= float config.ThrottleThreshold.MaxThrottlePerMinute)
            |> Seq.length

        async {
            let! res = CloudWatchUtils.getStreamMetric cloudWatch streamDim CloudWatchUtils.throttledMetricName config
            match res with
            | Success dataPoints -> 
                if dataPoints.Count >= int config.ThrottleThreshold.ConsecutiveMinutes &&
                   getConsecutiveBreaches dataPoints >= int config.ThrottleThreshold.ConsecutiveMinutes 
                then 
                    logger.DebugFormat("Throttle threshold [{0}] for stream [{1}] is breached, splitting busiest shard.", config.ThrottleThreshold, streamName)
                    do! splitBusiestShard
            | Failure exn -> logger.Error(sprintf "Couldn't get throttle metrics for the stream [%s]" streamName, exn)
        }
        
    let startTimer onElapsed (interval : TimeSpan)  = 
        let timer = new Timer(interval.TotalMilliseconds)
        do timer.Elapsed.Add onElapsed
        do timer.Start() 
        timer

    let pushTimer  = TimeSpan.FromMinutes(1.0) |> startTimer (fun _ -> Async.Start(pushMetrics, cts.Token))
    let splitTimer = TimeSpan.FromMinutes(1.0) |> startTimer (fun _ -> Async.Start(checkThrottles, cts.Token))

    let disposeInvoked = ref 0
    let cleanup (disposing : bool) =
        if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
            logger.Debug("Disposing...")
            
            pushTimer.Dispose()
            splitTimer.Dispose()

            logger.Debug("Disposed.")

    member this.TrackThrottledSend (partitionKey : string) = 
        agent.Post <| IncrMetric(DateTime.UtcNow, streamDims, StandardUnit.Count, CloudWatchUtils.throttledMetricName, 1)

    member this.TrackSuccessfulSend (shardId : string, payloadSize : int) = 
        let shardDims = getShardDims shardId
        agent.Post <| IncrMetric(DateTime.UtcNow, shardDims, StandardUnit.Count, CloudWatchUtils.sendMetricName, 1)
        agent.Post <| IncrMetric(DateTime.UtcNow, shardDims, StandardUnit.Bytes, CloudWatchUtils.sizeMetricName, payloadSize)
    
    interface IDisposable with
        member this.Dispose () = 
            GC.SuppressFinalize(this)
            cleanup(true)

type internal GloriousGodfrey (kinesis    : IAmazonKinesis,
                               config     : DarkseidConfig,
                               appName    : string,
                               streamName : string,
                               cts        : CancellationTokenSource,
                               virman     : VirmanVundabar) = 
    let loggerName = sprintf "GloriousGodfrey[AppName:%s, Stream:%s]" appName streamName
    let logger     = LogManager.GetLogger loggerName

    let successEvent   = new Event<string * int>()
    let errorEvent     = new Event<Record * Exception>()
    let failedEvent    = new Event<Record>()
    let throttledEvent = new Event<string>()

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

    /// Attempts to send a record until the max number of attempts has been exceeded
    let rec send record attempts =
        async {
            let! res = KinesisUtils.putRecord kinesis streamName record cts
            match res with
            | Success res ->
                decrBacklog()
                virman.TrackSuccessfulSend(res.ShardId, record.Data.Length)
                successEvent.Trigger(res.ShardId, record.Data.Length)
                return Success()
            | Failure exn ->
                match exn with
                | :? ProvisionedThroughputExceededException ->
                    throttledEvent.Trigger(record.PartitionKey)
                    logger.Warn(sprintf "PutRecord request attempt [%d] is throttled, retrying in 100ms..." attempts, exn)
                    do! Async.Sleep 100                    
                | _ ->
                    errorEvent.Trigger(record, exn)
                    logger.Warn(sprintf "PutRecord request attempt [%d] encountered an error, retrying..." attempts, exn)
                                        
                if attempts >= config.MaxPutRecordAttempts 
                then failedEvent.Trigger(record)
                     return Failure exn
                else return! send record (attempts + 1)
        }

    /// Generates an AeroTrooper (background worker) for doing the work of actually sending records to Kinesis
    let genTrooper _ = 
        let body (inbox : Agent<AeroTrooperMessage>) =
            async {
                while true do
                    let! msg = inbox.Receive()
                    match msg with
                    | Put record -> do! send record 1 |> Async.Ignore
                    | BlockingPut (record, reply) -> 
                        let! res = send record 1
                        reply.Reply(res)
            }

        let trooper = Agent<AeroTrooperMessage>.Start(body, cts.Token)
        trooper.Error.Add(fun exn -> logger.Warn("Worker encountered an unhandled error. Ignoring.", exn))
        trooper

    /// agent for handling the background processing mode
    let getBackgroundAgent (inbox       : Agent<GodfreyMessage>) 
                           (bgConfig    : BackgroundProcessingConfig)
                           (getTrooper  : Record -> Agent<AeroTrooperMessage>) = 
        async {
            while true do
                let! msg = inbox.Receive()
                match msg with
                | Send (record, reply) -> 
                    match backlogSize >= bgConfig.HighWaterMarks with
                    | true ->
                        match bgConfig.HighWaterMarksMode with
                        | HighWaterMarksMode.DropData -> 
                            logger.WarnFormat("HighWaterMark [{0}] reached, dropping data...", bgConfig.HighWaterMarks)
                            reply.Reply(Success ())
                        | HighWaterMarksMode.Block    -> 
                            logger.WarnFormat("HighWaterMark [{0}] reached, blocking send request...", bgConfig.HighWaterMarks)

                            // block until we're below the high water mark
                            while backlogSize >= bgConfig.HighWaterMarks do
                                do! Async.Sleep(10)

                            getTrooper(record).Post <| Put record
                            incrBacklog()
                            reply.Reply(Success ())
                    | _ -> getTrooper(record).Post <| Put record
                           incrBacklog()
                           reply.Reply(Success ())
        }

    /// agent for handling the blocking processing mode
    let getBlockingAgent (inbox         : Agent<GodfreyMessage>) 
                         (getTrooper    : Record -> Agent<AeroTrooperMessage>) =
        async {
            while true do
                let! msg = inbox.Receive()
                match msg with
                | Send (record, reply) -> getTrooper(record).Post <| BlockingPut(record, reply)
        }

    let body (inbox : Agent<GodfreyMessage>) =
        let troopers = [| 1u..config.LevelOfConcurrency |] |> Array.map genTrooper

        // employ a simple hashing algorithm to find the trooper for this record
        let nextTrooper { PartitionKey = partitionKey } =
            let idx = partitionKey |> System.Text.Encoding.UTF8.GetBytes |> hash config.LevelOfConcurrency |> int
            troopers.[idx]

        match config.Mode with
        | Background backgroundConfig -> getBackgroundAgent inbox backgroundConfig nextTrooper
        | Blocking -> getBlockingAgent inbox nextTrooper

    let agent = Agent<GodfreyMessage>.Start(body, cts.Token)
    do agent.Error.Add(fun exn -> logger.Warn("Encountered an unhandled error. Ignoring.", exn))
    
    let disposeInvoked = ref 0
    let cleanup (disposing : bool) =
        if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
            logger.Debug("Disposing...waiting for all messages in circulation to be dealt with.")

            // backlog = sent to troopers to save
            // current queue = posted to top-level agent but not yet distributed to troopers
            while (agent.CurrentQueueLength + backlogSize) > 0 do Thread.Sleep(10)

            logger.Debug("Disposed...")

    member this.OnError = errorEvent.Publish

    member this.Send (record) = 
        if !disposeInvoked > 0 
        then raise ApplicationIsDisposing
        else agent.PostAndAsyncReply (fun reply -> Send(record, reply))

    interface IDisposable with
        member this.Dispose () = 
            GC.SuppressFinalize(this)
            cleanup(true)

    // provide a finalizer so that in the case the consumer forgets to dispose of the app the
    // finalizer will clean up
    override this.Finalize () =
        logger.Warn("Finalizer is invoked. Please ensure that the object is disposed in a deterministic manner instead.")
        cleanup(false)

type Producer private (kinesis      : IAmazonKinesis,
                       cloudWatch   : IAmazonCloudWatch,
                       config       : DarkseidConfig,
                       appName      : string,
                       streamName   : string) as this =
    let loggerName = sprintf "Darkseid[AppName:%s, Stream:%s]" appName streamName
    let logger     = LogManager.GetLogger loggerName

    do validateConfig config

    let errorEvent = new Event<OnErrorDelegate, OnErrorEventArgs>()

    let cts = new CancellationTokenSource()
 
    let virman  = new VirmanVundabar(kinesis, cloudWatch, config, appName, streamName, cts)
    let godfrey = new GloriousGodfrey(kinesis, config, appName, streamName, cts, virman)
    do godfrey.OnError.Add(fun (record, exn) -> errorEvent.Trigger(this, new OnErrorEventArgs(record, exn)))

    let disposeInvoked = ref 0
    let cleanup (disposing : bool) =
        if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
            logger.Debug("Disposing...")
            (godfrey :> IDisposable).Dispose()            
            logger.Debug("Disposed.")
    
    static member CreateNew(awsKey    : string, 
                            awsSecret : string, 
                            region    : RegionEndpoint, 
                            appName, streamName) =
        let config     = new DarkseidConfig()
        let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
        let cloudWatch = AWSClientFactory.CreateAmazonCloudWatchClient(awsKey, awsSecret, region)
        new Producer(kinesis, cloudWatch, config, appName, streamName) :> IProducer

    static member CreateNew(awsKey    : string, 
                            awsSecret : string, 
                            region    : RegionEndpoint, 
                            appName, streamName, config) =
        let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
        let cloudWatch = AWSClientFactory.CreateAmazonCloudWatchClient(awsKey, awsSecret, region)
        new Producer(kinesis, cloudWatch, config, appName, streamName) :> IProducer

    static member CreateNew(kinesis, cloudWatch, appName, streamName) =
        let config = new DarkseidConfig()
        new Producer(kinesis, cloudWatch, config, appName, streamName) :> IProducer

    static member CreateNew(kinesis, cloudWatch, appName, streamName, config) =
        new Producer(kinesis, cloudWatch, config, appName, streamName) :> IProducer
        
    interface IProducer with        
        [<CLIEvent>] member this.OnError = errorEvent.Publish

        member this.SendAsync (record : Record) = 
            async {
                let! res = godfrey.Send(record)
                match res with
                | Success _   -> ()
                | Failure exn -> raise exn
            }
            |> Async.StartAsPlainTask

    interface IDisposable with
        member this.Dispose () = 
            GC.SuppressFinalize(this)
            cleanup(true)

    // provide a finalizer so that in the case the consumer forgets to dispose of the app the
    // finalizer will clean up
    override this.Finalize () =
        logger.Warn("Finalizer is invoked. Please ensure that the object is disposed in a deterministic manner instead.")
        cleanup(false)