module Darkseid.Utils

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks

open Amazon.CloudWatch
open Amazon.CloudWatch.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open log4net

open Darkseid.Model

/// Type alias for F# mailbox processor type
type Agent<'T> = MailboxProcessor<'T>

[<AutoOpen>]
module internal Utils =
    let validateConfig (config : DarkseidConfig) =
        match config.Mode with
        | Background bgConfig -> 
            match bgConfig.HighWaterMarksMode with
            | HighWaterMarksMode.DropData | HighWaterMarksMode.Block -> ()
            | mode -> raise <| InvalidHighWaterMarksMode mode
        | _ -> ()

    // since the async methods from the AWSSDK excepts with AggregateException which is not all that useful, hence
    // this active pattern which unwraps any AggregateException
    let rec (|Flatten|) (exn : Exception) =
        match exn with
        | :? AggregateException as aggrExn -> Flatten aggrExn.InnerException
        | exn -> exn

    /// Applies memoization to the supplied function f
    let memoize (f : 'a -> 'b) =
        let cache = new Dictionary<'a, 'b>()

        let memoizedFunc (input : 'a) =
            // check if there is a cached result for this input
            match cache.TryGetValue(input) with
            | true, x   -> x
            | false, _  ->
                // evaluate and add result to cache
                let result = f input
                cache.Add(input, result)
                result

        // return the memoized version of f
        memoizedFunc

    /// Default function for calcuating delay (in milliseconds) between retries, based on (http://en.wikipedia.org/wiki/Exponential_backoff)
    /// TODO : can be memoized
    let private exponentialDelay =
        let calcInternal attempts = 
            let rec sum acc = function | 0 -> acc | n -> sum (acc + n) (n - 1)

            let n = pown 2 attempts - 1
            let slots = float (sum 0 n) / float (n + 1)
            int (10.0 * slots)
        memoize calcInternal

    let inline csv (arr : 'a[]) = String.Join(",", arr)

    type Async with
        /// Starts a computation as a plain task.
        static member StartAsPlainTask (work : Async<unit>) = Task.Factory.StartNew(fun () -> work |> Async.RunSynchronously)

        /// Retries the async computation up to specified number of times. Optionally accepts a function to calculate
        /// the delay in milliseconds between retries, default is exponential delay with a backoff slot of 500ms.
        static member WithRetry (computation : Async<'a>, maxRetries, ?calcDelay) =
            let calcDelay = defaultArg calcDelay exponentialDelay

            let rec loop retryCount =
                async {
                    let! res = computation |> Async.Catch
                    match res with
                    | Choice1Of2 x -> return Choice1Of2 x
                    | Choice2Of2 _ when retryCount <= maxRetries -> 
                        do! calcDelay (retryCount + 1) |> Async.Sleep
                        return! loop (retryCount + 1)
                    | Choice2Of2 exn -> return Choice2Of2 exn
                }
            loop 0

    module Seq =
        // originaly from http://fssnip.net/1o
        let groupsOfAtMost (size: int) (s: seq<'v>) =
            seq {
                let en = s.GetEnumerator ()
                let more = ref true
                while !more do
                    let group =
                        [|
                            let i = ref 0
                            while !i < size && en.MoveNext () do
                                yield en.Current
                                i := !i + 1
                        |]
                    if group.Length = 0 
                    then more := false
                    else yield group
            }

    module Map =
        let getOrDefault key defaultVal (map : Map<'a, 'b>) =
            match map.TryFind key with | Some value -> value | _ -> defaultVal

module internal KinesisUtils =
    let private logger   = LogManager.GetLogger("KinesisUtils")

    let maxPutRequestsPerSecond = 1000.0
    let maxPutBytesPerSecond    = 1024.0 * 1024.0
    
    /// Sends a record to a stream
    let putRecord (kinesis : IAmazonKinesis) streamName (record : Record) (cts : CancellationTokenSource) =
        async {
            use memStream = new MemoryStream(record.Data)
            let  req = new PutRecordRequest(StreamName = streamName, Data = memStream, PartitionKey = record.PartitionKey)
            let! res = kinesis.PutRecordAsync(req, cts.Token) |> Async.AwaitTask |> Async.Catch
            match res with
            | Choice1Of2 res -> return Success res
            | Choice2Of2 (Flatten exn) -> return Failure exn
        }

    /// Returns the shards that are part of the stream
    let getShards (kinesis : IAmazonKinesis) streamName =
        async {
            let req = new DescribeStreamRequest(StreamName = streamName)
            let! res = Async.WithRetry(kinesis.DescribeStreamAsync(req) |> Async.AwaitTask, 2)
               
            match res with
            | Choice1Of2 res ->
                let shardIds = res.StreamDescription.Shards |> Seq.map (fun shard -> shard.ShardId) |> Seq.toArray
                logger.DebugFormat("Stream [{0}] has [{1}] shards: [{2}]", streamName, shardIds.Length, csv shardIds)
                return Success(res.StreamDescription.StreamStatus.Value, res.StreamDescription.Shards, shardIds)
            | Choice2Of2 (Flatten exn) -> 
                logger.Error(sprintf "Failed to get shards for stream [%s]" streamName, exn)
                return Failure exn
        }

    /// Splits the specified shard in the middle of its hash key range
    let splitShard (kinesis : IAmazonKinesis) streamName (shard : Shard) =
        async {
            let req = new SplitShardRequest(StreamName = streamName, ShardToSplit = shard.ShardId)
            let startHashKey = bigint.Parse shard.HashKeyRange.StartingHashKey
            let endHashKey   = bigint.Parse shard.HashKeyRange.EndingHashKey
            let newHashKey   = (startHashKey + endHashKey) / 2I
            req.NewStartingHashKey <- string newHashKey

            let! res = kinesis.SplitShardAsync(req) |> Async.AwaitTask |> Async.Catch
            match res with
            | Choice1Of2 _   -> logger.DebugFormat("Successfuly split shard [{0}] in stream [{1}] with NewStartingHashKey [{2}]", shard.ShardId, streamName, newHashKey)
            | Choice2Of2 exn -> logger.Error(sprintf "Failed to split shard [%s] in stream [%s]" shard.ShardId streamName, exn)
        }

module internal CloudWatchUtils =
    let private logger = LogManager.GetLogger("CloudWatchUtils")
    
    // this metric name and dimensions would make our custom metrics go into the same place as the rest of the Kinesis metrics
    let metricNamespace, streamDimensionName, shardDimensionName = "Darkseid", "StreamName", "ShardId"
    let sendMetricName, sizeMetricName, throttledMetricName = "PutRecord.Success", "PutRecord.Bytes", "PutRecord.Throttled"

    // batch up metrics into groups of 20
    let private batchSize = 20

    /// Push a bunch of metrics
    let pushMetrics (cloudWatch : IAmazonCloudWatch) (metrics : Metric[]) =
        let groups   = metrics |> Seq.groupsOfAtMost batchSize |> Seq.toArray
        let requests = groups |> Array.map (fun metrics -> 
            let req  = new PutMetricDataRequest(Namespace = metricNamespace)
            let data = metrics |> Seq.map (fun m -> 
                        let stats = new StatisticSet(Minimum = m.Min, Maximum = m.Max, Sum = m.Sum, SampleCount = m.Count)
                        let datum = new MetricDatum(MetricName      = m.MetricName, 
                                                    Timestamp       = m.Timestamp,
                                                    Unit            = m.Unit,
                                                    StatisticValues = stats)
                        datum.Dimensions.AddRange(m.Dimensions)
                        datum)
            req.MetricData.AddRange(data)
            req)

        let pushMetricsInternal (cloudWatch : IAmazonCloudWatch) req =
            async {
                let! res = Async.WithRetry(cloudWatch.PutMetricDataAsync(req) |> Async.AwaitTask, 2)
                match res with
                | Choice1Of2 _  -> logger.DebugFormat("Successfully pushed [{0}] metrics.", req.MetricData.Count)
                | Choice2Of2 (Flatten exn) -> logger.Warn(sprintf "Failed to push [%d] metrics." req.MetricData.Count, exn)
            }

        async {
            logger.DebugFormat("Pushing [{0}] metrics in [{1}] batches.", metrics.Length, requests.Length)
            do! requests |> Seq.map (pushMetricsInternal cloudWatch) |> Async.Parallel |> Async.Ignore
        }

    /// Get metrics for a particular shard
    let private getAvgShardMetric (cloudWatch : IAmazonCloudWatch) metricName startTime endTime (streamDim : Dimension) shardId =
        async {
            let req = new GetMetricStatisticsRequest(Namespace  = metricNamespace, 
                                                     EndTime    = endTime,
                                                     StartTime  = startTime,
                                                     Period     = 60, // 60 seconds is the lowest granularity supported by CloudWatch
                                                     MetricName = metricName)
            req.Statistics.Add("Sum")
            req.Dimensions.Add(streamDim)
            req.Dimensions.Add(new Dimension(Name = shardDimensionName, Value = shardId))

            let! res = Async.WithRetry(cloudWatch.GetMetricStatisticsAsync(req) |> Async.AwaitTask, 2)
            match res with
            | Choice1Of2 res -> 
                let avg = res.Datapoints |> Seq.map (fun dp -> dp.Sum) |> Seq.average
                return Success (shardId, avg)
            | Choice2Of2 (Flatten exn) -> return Failure exn
        }

    /// Get the averages of CloudWatch metrics for a batch of shards
    let getAvgShardMetrics (cloudWatch : IAmazonCloudWatch) (streamDim : Dimension) (shardIds : string[]) metricName =
        let endTime    = DateTime.UtcNow
        let startTime  = endTime.AddMinutes(-5.0)
        let streamName = streamDim.Value

        async {
            logger.DebugFormat("Getting metrics [{0}] for [{1}] shards in stream [{2}], shard IDs [{3}].", metricName, shardIds.Length, streamName, csv shardIds)
            let! averages = shardIds
                            |> Seq.map (getAvgShardMetric cloudWatch metricName startTime endTime streamDim)
                            |> Async.Parallel

            let error = averages |> Array.tryPick (function | Success _ -> None | Failure exn -> Some exn)
            match error with
            | Some exn -> return Failure exn
            | _ ->
                logger.DebugFormat("Received [{0}] shard metrics for stream [{1}].", averages.Length, streamName)
                let result = averages 
                             |> Seq.map (function (Success(shardId, avg)) -> shardId, avg)
                             |> Map.ofSeq
                return Success result
        }

    /// Get the average of a CloudWatch metric for a stream
    let getAvgStreamMetric (cloudWatch : IAmazonCloudWatch) (streamDim : Dimension) metricName =
        let endTime    = DateTime.UtcNow
        let startTime  = endTime.AddMinutes(-5.0)
        let streamName = streamDim.Value

        async {
            logger.DebugFormat("Getting metric [{0}] for stream [{1}]", metricName, streamName)

            let req = new GetMetricStatisticsRequest(Namespace  = metricNamespace, 
                                                     EndTime    = endTime,
                                                     StartTime  = startTime,
                                                     Period     = 60, // 60 seconds is the lowest granularity supported by CloudWatch
                                                     MetricName = metricName)
            req.Statistics.Add("Sum")
            req.Dimensions.Add(streamDim)
            let! res = Async.WithRetry(cloudWatch.GetMetricStatisticsAsync(req) |> Async.AwaitTask, 2)
            match res with
            | Choice1Of2 res -> 
                let avg = res.Datapoints |> Seq.map (fun dp -> dp.Sum) |> Seq.average
                logger.DebugFormat("Received [{0}] datapoints for stream [{1}], average is [{2}]", res.Datapoints.Count, streamName, avg)
                return Success avg
            | Choice2Of2 (Flatten exn) -> return Failure exn
        }