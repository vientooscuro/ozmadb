module OzmaDB.TimeTriggersWorker

open System
open System.Data
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Npgsql

open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.Connection
open OzmaDB.SQL.Query
open OzmaDB.API.Types
open OzmaDB.API.Request
open OzmaDB.API.API
open OzmaDB.API.InstancesCache
open OzmaDB.Actions.Schedule
open OzmaDB.Triggers.Time
open OzmaDB.HTTP.Utils

type TimeTriggersWorker
    (loggerFactory: ILoggerFactory, instancesCache: InstancesCacheStore, instancesSource: IInstancesSource) =
    inherit BackgroundService()

    let logger = loggerFactory.CreateLogger<TimeTriggersWorker>()
    let pollDelay = TimeSpan.FromSeconds(1.0)
    let maxBatchPerConnection = 64

    let instanceConnectionString (instance: IInstance) =
        let builder = NpgsqlConnectionStringBuilder()
        builder.Host <- instance.Host
        builder.Port <- instance.Port
        builder.Database <- instance.Database
        builder.Username <- instance.Username
        builder.Password <- instance.Password
        builder.Enlist <- false
        instancesSource.SetExtraConnectionOptions(builder)
        builder.ConnectionString

    let discoverConnectionStrings (cancellationToken: CancellationToken) : Task<Set<string>> =
        task {
            let fromCache = instancesCache.KnownConnectionStrings |> Set.ofSeq
            let! sourceInstances = instancesSource.GetAllInstances(cancellationToken)
            let instances = sourceInstances |> Seq.toArray

            let mutable discovered = fromCache

            try
                for instance in instances do
                    discovered <- Set.add (instanceConnectionString instance) discovered
            finally
                for instance in instances do
                    instance.Dispose()

            return discovered
        }

    let maxSerializableRetries = 5
    let baseRetryDelayMs = 100
    let maxRetryDelayMs = 2000
    let retryJitterMs = 50

    // Phase A: claim a due time trigger or action schedule in a short ReadCommitted transaction.
    // SSI is not needed here — FOR UPDATE SKIP LOCKED already guarantees exclusive claim semantics,
    // and running the claim at ReadCommitted removes predicate reads that previously caused SSI
    // pivot cancellations when the long-running scheduler tx overlapped with app transactions.
    let claimNextDue
        (connectionString: string)
        (cancellationToken: CancellationToken)
        : Task<ClaimedTimeTriggerTask option * ClaimedActionSchedule option> =
        openAndCheckTransaction loggerFactory connectionString IsolationLevel.ReadCommitted cancellationToken
        <| fun trans ->
            task {
                try
                    let! maybeClaimedTimeTask = tryClaimDueTimeTrigger trans.Connection.Query cancellationToken

                    let! maybeClaimedActionSchedule =
                        match maybeClaimedTimeTask with
                        | Some _ -> Task.result None
                        | None -> tryClaimDueActionSchedule trans.Connection.Query cancellationToken

                    let! _ = trans.Commit(cancellationToken)
                    return (maybeClaimedTimeTask, maybeClaimedActionSchedule)
                finally
                    (trans.Connection :> IDisposable).Dispose()
            }

    // Phase C: persist a terminal failure of a claimed task in a short ReadCommitted transaction,
    // independent from the serializable tx that may have rolled back.
    let persistTimeTriggerFailure
        (connectionString: string)
        (taskId: int)
        (attempts: int)
        (err: string)
        (cancellationToken: CancellationToken)
        : Task<unit> =
        openAndCheckTransaction loggerFactory connectionString IsolationLevel.ReadCommitted cancellationToken
        <| fun trans ->
            task {
                try
                    do! failClaimedTimeTrigger trans.Connection.Query taskId attempts err cancellationToken
                    let! _ = trans.Commit(cancellationToken)
                    return ()
                finally
                    (trans.Connection :> IDisposable).Dispose()
            }

    let persistActionScheduleFailure
        (connectionString: string)
        (scheduleId: int)
        (attempts: int)
        (err: string)
        (cancellationToken: CancellationToken)
        : Task<unit> =
        openAndCheckTransaction loggerFactory connectionString IsolationLevel.ReadCommitted cancellationToken
        <| fun trans ->
            task {
                try
                    do! failClaimedActionSchedule trans.Connection.Query scheduleId attempts err cancellationToken
                    let! _ = trans.Commit(cancellationToken)
                    return ()
                finally
                    (trans.Connection :> IDisposable).Dispose()
            }

    // Phase B: run the claimed time trigger inside the SERIALIZABLE ctx obtained from the cache.
    // Retries on ConcurrentUpdateException (SSI / row-level concurrent update) with exponential
    // backoff. If the user's JS trigger code itself throws, we return a terminal Error — do NOT
    // retry a user-visible failure. Same semantics as runWithApi in HTTP/Utils.fs.
    //
    // Returns:
    //   Ok () — trigger ran and completion was committed
    //   Error err — trigger failed terminally (either JS error or SSI retries exhausted);
    //               caller must persist the failure via Phase C.
    let runClaimedTimeTrigger
        (connectionString: string)
        (claimedTask: ClaimedTimeTriggerTask)
        (cancellationToken: CancellationToken)
        : Task<Result<unit, string>> =
        task {
            let mutable attempt = 0
            let mutable result: Result<unit, string> option = None

            while Option.isNone result do
                try
                    let! cache = instancesCache.GetContextCache(connectionString)
                    use! ctx = cache.GetCache(cancellationToken)

                    let reqParams =
                        { Context = ctx
                          UserName = "__time_trigger_worker"
                          IsRoot = true
                          CanRead = true
                          Language = "en"
                          Theme = "default"
                          Quota = { MaxSize = None; MaxUsers = None } }

                    let! rctx = RequestContext.Create(reqParams)
                    let _api = OzmaDBAPI(rctx)

                    match ctx.FindTrigger claimedTask.Trigger with
                    | None ->
                        logger.LogWarning(
                            "Time trigger {trigger} (task {id}) no longer exists, removing stale task",
                            claimedTask.Trigger,
                            claimedTask.Id
                        )

                        do! completeClaimedTimeTrigger ctx.Transaction.Connection.Query claimedTask.Id cancellationToken

                        let! commitResult = ctx.Commit()

                        match commitResult with
                        | Ok() -> result <- Some(Ok())
                        | Error err ->
                            logger.LogError("Failed to commit stale time-trigger removal: {error}", err.LogMessage)

                            result <- Some(Error(err.Message))
                    | Some preparedTrigger ->
                        let! runResult =
                            task {
                                try
                                    do!
                                        rctx.RunWithSource(ESTrigger claimedTask.Trigger)
                                        <| fun () ->
                                            task {
                                                do!
                                                    preparedTrigger.Script.RunTimeTrigger
                                                        claimedTask.EventEntity
                                                        claimedTask.RowId
                                                        claimedTask.FieldName
                                                        claimedTask.DueAt
                                                        claimedTask.OffsetValue
                                                        claimedTask.OffsetUnit
                                                        cancellationToken

                                                return ()
                                            }

                                    return Ok()
                                with e ->
                                    return Error(fullUserMessage e)
                            }

                        match runResult with
                        | Ok() ->
                            do!
                                completeClaimedTimeTrigger
                                    ctx.Transaction.Connection.Query
                                    claimedTask.Id
                                    cancellationToken

                            let! commitResult = ctx.Commit()

                            match commitResult with
                            | Ok() -> result <- Some(Ok())
                            | Error err ->
                                logger.LogError("Failed to commit time-trigger transaction: {error}", err.LogMessage)

                                result <- Some(Error(err.Message))
                        | Error err ->
                            logger.LogError(
                                "Time trigger execution failed for {trigger} (task {id}): {error}",
                                claimedTask.Trigger,
                                claimedTask.Id,
                                err
                            )

                            result <- Some(Error err)
                with :? ConcurrentUpdateException as e ->
                    if attempt >= maxSerializableRetries then
                        logger.LogError(
                            e,
                            "Concurrent update on time trigger {trigger} (task {id}); giving up after {attempt} retries",
                            claimedTask.Trigger,
                            claimedTask.Id,
                            attempt
                        )

                        result <- Some(Error(fullUserMessage e))
                    else
                        let jitter = Random.Shared.Next(retryJitterMs)
                        let delay = min maxRetryDelayMs ((baseRetryDelayMs * (1 <<< attempt)) + jitter)

                        logger.LogWarning(
                            e,
                            "Concurrent update on time trigger {trigger} (task {id}) attempt {attempt}/{max}, retrying in {delay}ms",
                            claimedTask.Trigger,
                            claimedTask.Id,
                            attempt + 1,
                            maxSerializableRetries,
                            delay
                        )

                        do! Task.Delay(delay, cancellationToken)
                        attempt <- attempt + 1

            return Option.get result
        }

    let runClaimedActionSchedule
        (connectionString: string)
        (claimedSchedule: ClaimedActionSchedule)
        (cancellationToken: CancellationToken)
        : Task<Result<unit, string>> =
        task {
            let mutable attempt = 0
            let mutable result: Result<unit, string> option = None

            while Option.isNone result do
                try
                    let! cache = instancesCache.GetContextCache(connectionString)
                    use! ctx = cache.GetCache(cancellationToken)

                    let reqParams =
                        { Context = ctx
                          UserName = "__time_trigger_worker"
                          IsRoot = true
                          CanRead = true
                          Language = "en"
                          Theme = "default"
                          Quota = { MaxSize = None; MaxUsers = None } }

                    let! rctx = RequestContext.Create(reqParams)
                    let _api = OzmaDBAPI(rctx)

                    match ctx.FindAction claimedSchedule.Action with
                    | None ->
                        logger.LogWarning(
                            "Scheduled action {action} (schedule {id}) no longer exists, removing stale schedule",
                            claimedSchedule.Action,
                            claimedSchedule.Id
                        )

                        do!
                            completeClaimedActionSchedule
                                ctx.Transaction.Connection.Query
                                claimedSchedule.Id
                                claimedSchedule.DueAt
                                cancellationToken

                        let! commitResult = ctx.Commit()

                        match commitResult with
                        | Ok() -> result <- Some(Ok())
                        | Error err ->
                            logger.LogError("Failed to commit stale action-schedule removal: {error}", err.LogMessage)

                            result <- Some(Error(err.Message))
                    | Some actionResult ->
                        let! runResult =
                            task {
                                try
                                    match actionResult with
                                    | Error e ->
                                        let message =
                                            sprintf "Action %O is broken: %s" claimedSchedule.Action (fullUserMessage e)

                                        return Error message
                                    | Ok action ->
                                        do!
                                            rctx.RunWithSource(ESAction claimedSchedule.Action)
                                            <| fun () ->
                                                task {
                                                    let! _ = action.Run(claimedSchedule.Args, cancellationToken)
                                                    return ()
                                                }

                                        return Ok()
                                with e ->
                                    return Error(fullUserMessage e)
                            }

                        match runResult with
                        | Ok() ->
                            do!
                                completeClaimedActionSchedule
                                    ctx.Transaction.Connection.Query
                                    claimedSchedule.Id
                                    claimedSchedule.DueAt
                                    cancellationToken

                            let! commitResult = ctx.Commit()

                            match commitResult with
                            | Ok() -> result <- Some(Ok())
                            | Error err ->
                                logger.LogError("Failed to commit action-schedule transaction: {error}", err.LogMessage)

                                result <- Some(Error(err.Message))
                        | Error err ->
                            logger.LogError(
                                "Scheduled action execution failed for {action} (schedule {id}): {error}",
                                claimedSchedule.Action,
                                claimedSchedule.Id,
                                err
                            )

                            result <- Some(Error err)
                with :? ConcurrentUpdateException as e ->
                    if attempt >= maxSerializableRetries then
                        logger.LogError(
                            e,
                            "Concurrent update on action schedule {action} (id {id}); giving up after {attempt} retries",
                            claimedSchedule.Action,
                            claimedSchedule.Id,
                            attempt
                        )

                        result <- Some(Error(fullUserMessage e))
                    else
                        let jitter = Random.Shared.Next(retryJitterMs)
                        let delay = min maxRetryDelayMs ((baseRetryDelayMs * (1 <<< attempt)) + jitter)

                        logger.LogWarning(
                            e,
                            "Concurrent update on action schedule {action} (id {id}) attempt {attempt}/{max}, retrying in {delay}ms",
                            claimedSchedule.Action,
                            claimedSchedule.Id,
                            attempt + 1,
                            maxSerializableRetries,
                            delay
                        )

                        do! Task.Delay(delay, cancellationToken)
                        attempt <- attempt + 1

            return Option.get result
        }

    let processOneConnection (connectionString: string) (cancellationToken: CancellationToken) : Task<int> =
        task {
            let mutable processed = 0
            let mutable shouldContinue = true

            while shouldContinue
                  && processed < maxBatchPerConnection
                  && not cancellationToken.IsCancellationRequested do
                let! maybeClaimedTimeTask, maybeClaimedActionSchedule = claimNextDue connectionString cancellationToken

                match maybeClaimedTimeTask, maybeClaimedActionSchedule with
                | None, None -> shouldContinue <- false
                | Some claimedTask, _ ->
                    let! runResult = runClaimedTimeTrigger connectionString claimedTask cancellationToken

                    match runResult with
                    | Ok() -> processed <- processed + 1
                    | Error err ->
                        try
                            do!
                                persistTimeTriggerFailure
                                    connectionString
                                    claimedTask.Id
                                    claimedTask.Attempts
                                    err
                                    cancellationToken

                            processed <- processed + 1
                        with e ->
                            logger.LogError(
                                e,
                                "Failed to persist time-trigger failure for {trigger} (task {id})",
                                claimedTask.Trigger,
                                claimedTask.Id
                            )

                            shouldContinue <- false
                | None, Some claimedSchedule ->
                    let! runResult = runClaimedActionSchedule connectionString claimedSchedule cancellationToken

                    match runResult with
                    | Ok() -> processed <- processed + 1
                    | Error err ->
                        try
                            do!
                                persistActionScheduleFailure
                                    connectionString
                                    claimedSchedule.Id
                                    claimedSchedule.Attempts
                                    err
                                    cancellationToken

                            processed <- processed + 1
                        with e ->
                            logger.LogError(
                                e,
                                "Failed to persist action-schedule failure for {action} (id {id})",
                                claimedSchedule.Action,
                                claimedSchedule.Id
                            )

                            shouldContinue <- false

            return processed
        }

    override this.ExecuteAsync(cancellationToken: CancellationToken) : Task =
        task {
            while not cancellationToken.IsCancellationRequested do
                try
                    let mutable totalProcessed = 0

                    let! connectionStrings = discoverConnectionStrings cancellationToken

                    for connectionString in connectionStrings do
                        let! processed = processOneConnection connectionString cancellationToken
                        totalProcessed <- totalProcessed + processed

                    if totalProcessed = 0 then
                        do! Task.Delay(pollDelay, cancellationToken)
                with
                | :? OperationCanceledException -> ()
                | e ->
                    logger.LogError(e, "Unhandled exception in time trigger worker")
                    do! Task.Delay(pollDelay, cancellationToken)
        }
