module OzmaDB.TimeTriggersWorker

open System
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Npgsql

open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.API.Types
open OzmaDB.API.Request
open OzmaDB.API.API
open OzmaDB.API.InstancesCache
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

    let processOneConnection (connectionString: string) (cancellationToken: CancellationToken) : Task<int> =
        task {
            let mutable processed = 0
            let mutable shouldContinue = true

            while shouldContinue
                  && processed < maxBatchPerConnection
                  && not cancellationToken.IsCancellationRequested do
                let! cache = instancesCache.GetContextCache(connectionString)

                use! ctx = cache.GetCache(cancellationToken)

                let reqParams =
                    { Context = ctx
                      UserName = "__time_trigger_worker"
                      IsRoot = true
                      CanRead = true
                      Language = "en"
                      Quota = { MaxSize = None; MaxUsers = None } }

                let! rctx = RequestContext.Create(reqParams)
                let _api = OzmaDBAPI(rctx)

                match! tryClaimDueTimeTrigger ctx.Transaction.Connection.Query cancellationToken with
                | None ->
                    let! commitResult = ctx.Commit()

                    match commitResult with
                    | Ok() -> shouldContinue <- false
                    | Error err ->
                        logger.LogError("Failed to commit empty time-trigger transaction: {error}", err.LogMessage)
                        shouldContinue <- false
                | Some claimedTask ->
                    let! runResult =
                        task {
                            try
                                match ctx.FindTrigger claimedTask.Trigger with
                                | None ->
                                    let message = sprintf "Trigger %O not found" claimedTask.Trigger
                                    return Error message
                                | Some preparedTrigger ->
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
                                                        cancellationToken

                                                return ()
                                            }

                                    return Ok()
                            with e ->
                                return Error(fullUserMessage e)
                        }

                    match runResult with
                    | Ok() ->
                        do! completeClaimedTimeTrigger ctx.Transaction.Connection.Query claimedTask.Id cancellationToken
                    | Error err ->
                        logger.LogError(
                            "Time trigger execution failed for {trigger} (task {id}): {error}",
                            claimedTask.Trigger,
                            claimedTask.Id,
                            err
                        )

                        do!
                            failClaimedTimeTrigger
                                ctx.Transaction.Connection.Query
                                claimedTask.Id
                                claimedTask.Attempts
                                err
                                cancellationToken

                    let! commitResult = ctx.Commit()

                    match commitResult with
                    | Ok() -> processed <- processed + 1
                    | Error err ->
                        logger.LogError("Failed to commit time-trigger transaction: {error}", err.LogMessage)
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
