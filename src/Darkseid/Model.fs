module Darkseid.Model

open System

open Amazon.Kinesis.Model
open Amazon.CloudWatch
open Amazon.CloudWatch.Model

type HighWaterMarksMode =
    | Block     = 1
    | DropData  = 2

type BackgroundProcessingConfig () =
    /// Maximum amount of backlog allowed to build up before we take preventative actions. Default is 1000.
    member val HighWaterMarks       = 1000 with get, set

    /// What to do when the high water marks has been reached. Default is to drop data.
    member val HighWaterMarksMode   = HighWaterMarksMode.DropData with get, set

type ProcessingMode = 
    | Background    of BackgroundProcessingConfig
    | Blocking

type ThrottleThreshold =
    {
        MaxThrottlePerMinute    : uint32   // max number of throttled puts allowed per minute
        ConsecutiveMinutes      : uint32   // the number of consecutive minutes where the threshold has been breached
    }
    static member Default = 
        {
            MaxThrottlePerMinute    = 100u
            ConsecutiveMinutes      = 3u
        }

    override this.ToString() =
        sprintf ">= %d throttled calls per minute for %d consecutive minutes" this.MaxThrottlePerMinute this.ConsecutiveMinutes

type DarkseidConfig () =
    /// How to process the send requests, the available modes are
    ///  - Background : records are saved into Kinesis by a number of background workers, the send request
    ///                 returns as soon as it's submitted into a managed backlog.
    ///  - Blocking : records are saved into Kinesis straight away, and the send request returns as soon as
    ///               the Kinesis request has completed.
    /// The default is the background mode which ensures better throughput.
    member val Mode = Background(new BackgroundProcessingConfig()) with get, set    
    
    /// The amount of concurrency for writing to the Kinesis stream. Default is 10.
    member val LevelOfConcurrency   = 10u with get, set

    /// The max number of attempts allowed for putting a record into the stream. Default is 3.
    member val MaxPutRecordAttempts = 3 with get, set

    /// Threshold for the throttled calls which will trigger splitting shards. Default is 100 throttles for 3 consecutive minutes.
    member val ThrottleThreshold    = ThrottleThreshold.Default with get, set

type Record =
    {
        Data         : byte[]
        PartitionKey : string
    }

[<AutoOpen>]
module Exceptions =
    exception InvalidHighWaterMarksMode of HighWaterMarksMode
    exception ApplicationIsDisposing
    
[<AutoOpen>]
module internal InternalModel =    
    type Result<'Success, 'Failure> =
        | Success   of 'Success
        | Failure   of 'Failure

    type GodfreyMessage =
        | Send          of Record * AsyncReplyChannel<Result<unit, Exception>>

    type AeroTrooperMessage =
        | Put           of Record
        | BlockingPut   of Record * AsyncReplyChannel<Result<unit, Exception>>

    type Metric = 
        {
            Dimensions      : Dimension[]
            MetricName      : string
            Timestamp       : DateTime
            Unit            : StandardUnit
            mutable Average : double
            mutable Sum     : double
            mutable Max     : double
            mutable Min     : double
            mutable Count   : double
        }

        static member Init (timestamp : DateTime, dimensions : Dimension[], unit, metricName, n) =
            { 
                Dimensions  = dimensions
                MetricName  = metricName
                Timestamp   = timestamp
                Unit        = unit
                Average     = n
                Sum         = n
                Max         = n
                Min         = n
                Count       = 1.0
            }

        member metric.AddDatapoint (n) =
            match metric.Count with
            | 0.0 -> metric.Max <- n
                     metric.Min <- n
            | _   -> metric.Max <- max metric.Max n
                     metric.Min <- min metric.Min n

            metric.Sum     <- metric.Sum + n
            metric.Count   <- metric.Count + 1.0
            metric.Average <- metric.Sum / metric.Count

    type VirmanMessage =
        | IncrMetric    of DateTime * Dimension[] * StandardUnit * string * int
        | Flush         of AsyncReplyChannel<Metric[]>