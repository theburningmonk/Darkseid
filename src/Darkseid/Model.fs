module Darkseid.Model

open System

open Amazon.Kinesis.Model
open Amazon.CloudWatch
open Amazon.CloudWatch.Model

type OptimizationMode =
    | Throughput       = 1
    | PreserveOrdering = 2

type HighWaterMarksMode =
    | Block     = 1
    | DropData  = 2

type DarkseidConfig () =
    /// Maximum amount of backlog allowed to build up before we take preventative actions. Default is 1000.
    member val HighWaterMarks       = 1000 with get, set

    /// What to do when the high water marks has been reached. Default is to drop data.
    member val HighWaterMarksMode   = HighWaterMarksMode.DropData with get, set

    /// The amount of concurrency for writing to the Kinesis stream. Default is 10.
    member val LevelOfConcurrency   = 10u with get, set

    /// The max number of attempts allowed for putting a record into the stream. Default is 3.
    member val MaxPutRecordAttempts = 3 with get, set

type Record =
    {
        Data         : byte[]
        PartitionKey : string
    }

[<AutoOpen>]
module Exceptions =
    do ()
    
[<AutoOpen>]
module internal InternalModel =    
    type Result<'Success, 'Failure> =
        | Success   of 'Success
        | Failure   of 'Failure

    type GodfreyMessage =
        | Send of Record * AsyncReplyChannel<unit>

    type AeroTrooperMessage =
        | PutRecord of Record

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