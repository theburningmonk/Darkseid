module Darkseid.Utils

open System
open System.Collections.Generic

open Amazon.CloudWatch
open Amazon.CloudWatch.Model

open log4net

open Darkseid.Model

/// Type alias for F# mailbox processor type
type Agent<'T> = MailboxProcessor<'T>

[<AutoOpen>]
module internal Utils =
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

    type Async with
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

module internal CloudWatchUtils =
    let private logger = LogManager.GetLogger("CloudWatchUtils")

    // batch up metrics into groups of 20
    let private batchSize = 20

    let private putMetricData (cloudWatch : IAmazonCloudWatch) req =
        async {
            let! work = Async.WithRetry(cloudWatch.PutMetricDataAsync(req) |> Async.AwaitTask, 2)
            match work with
            | Choice1Of2 _   -> logger.Debug("Successfully pushed metrics.")
            | Choice2Of2 (Flatten exn) -> logger.Warn("Failed to push metrics.", exn)
        }

    let pushMetrics (cloudWatch : IAmazonCloudWatch) ns (metrics : Metric[]) =
        let groups   = metrics |> Seq.mapi (fun i m -> i / batchSize, m) |> Seq.groupBy fst
        let requests = groups |> Seq.map (fun (_, group) -> 
            let req   = new PutMetricDataRequest(Namespace = ns)
            let data  = group 
                        |> Seq.map snd 
                        |> Seq.map (fun m -> 
                            let datum = new MetricDatum(MetricName = m.MetricName, 
                                                        Timestamp  = m.Timestamp,
                                                        Unit       = StandardUnit.Count,
                                                        Value      = m.Count)
                            datum.Dimensions.AddRange(m.Dimensions)
                            datum)
            req.MetricData.AddRange(data)
            req)

        async {
            do! requests |> Seq.map (putMetricData cloudWatch) |> Async.Parallel |> Async.Ignore
        }

        