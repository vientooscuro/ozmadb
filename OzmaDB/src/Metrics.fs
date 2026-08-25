module OzmaDB.Metrics

open System.Diagnostics
open System.Threading.Tasks
open Prometheus

// Handling one user view request is a chain of distinct steps - fetching the cached state,
// restricting the query to a role, talking to PostgreSQL, serializing the response - and until
// now none of them were measured separately. Without that split, "the page takes 400 ms" gives
// no clue which step to look at.

// Sub-millisecond work up to multi-second queries.
let private durationBuckets =
    [| 0.0005
       0.001
       0.0025
       0.005
       0.01
       0.025
       0.05
       0.1
       0.25
       0.5
       1.0
       2.5
       5.0
       10.0 |]

let private stageDuration =
    Metrics.CreateHistogram(
        "ozmadb_request_stage_duration_seconds",
        "Time spent in one stage of handling a request.",
        HistogramConfiguration(LabelNames = [| "stage" |], Buckets = durationBuckets)
    )

let private sqlDuration =
    Metrics.CreateHistogram(
        "ozmadb_sql_duration_seconds",
        "Time spent executing a single SQL statement, including the round trip to PostgreSQL.",
        HistogramConfiguration(LabelNames = [| "kind" |], Buckets = durationBuckets)
    )

let private roleViewCacheEvents =
    Metrics.CreateCounter(
        "ozmadb_role_view_cache_total",
        "Lookups of role-restricted compiled views, by outcome.",
        CounterConfiguration(LabelNames = [| "result" |])
    )

let private anonymousViewCacheEvents =
    Metrics.CreateCounter(
        "ozmadb_anonymous_view_cache_total",
        "Lookups of compiled anonymous user views, by outcome.",
        CounterConfiguration(LabelNames = [| "result" |])
    )

let private observe (histogram: Histogram) (label: string) (start: int64) =
    histogram
        .WithLabels(label)
        .Observe(Stopwatch.GetElapsedTime(start).TotalSeconds)

/// Measure a synchronous stage of request handling.
let measureStage (stage: string) (f: unit -> 'a) : 'a =
    let start = Stopwatch.GetTimestamp()

    try
        f ()
    finally
        observe stageDuration stage start

/// Measure an asynchronous stage of request handling.
let measureStageTask (stage: string) (f: unit -> Task<'a>) : Task<'a> =
    task {
        let start = Stopwatch.GetTimestamp()

        try
            return! f ()
        finally
            observe stageDuration stage start
    }

/// Measure a single SQL statement. `kind` distinguishes reads from writes and DDL.
let measureSqlTask (kind: string) (f: unit -> Task<'a>) : Task<'a> =
    task {
        let start = Stopwatch.GetTimestamp()

        try
            return! f ()
        finally
            observe sqlDuration kind start
    }

let roleViewCacheHit () =
    roleViewCacheEvents.WithLabels("hit").Inc()

let roleViewCacheMiss () =
    roleViewCacheEvents.WithLabels("miss").Inc()

let anonymousViewCacheHit () =
    anonymousViewCacheEvents.WithLabels("hit").Inc()

let anonymousViewCacheMiss () =
    anonymousViewCacheEvents.WithLabels("miss").Inc()
