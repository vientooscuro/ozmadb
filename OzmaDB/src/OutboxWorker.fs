module OzmaDB.OutboxWorker

open System
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Npgsql

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.API.InstancesCache
open OzmaDB.HTTP.Utils
open OzmaDB.Outbox.HTTP
open OzmaDB.Outbox.Queue

[<NoComparison; NoEquality>]
type OutboxWorkerSettings =
    { PollDelayMs: int
      MaxBatchPerConnection: int }

let defaultOutboxWorkerSettings =
    { PollDelayMs = 1000
      MaxBatchPerConnection = 64 }

type OutboxWorker
    (
        loggerFactory: ILoggerFactory,
        instancesCache: InstancesCacheStore,
        instancesSource: IInstancesSource,
        httpPolicy: OutboundHttpPolicy,
        settings: OutboxWorkerSettings
    ) =
    inherit BackgroundService()

    let logger = loggerFactory.CreateLogger<OutboxWorker>()
    let pollDelay = TimeSpan.FromMilliseconds(max 100 settings.PollDelayMs)
    let maxBatchPerConnection = max 1 settings.MaxBatchPerConnection
    let httpPolicy = normalizePolicy httpPolicy

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

                let! maybeClaimed = tryClaimDueOutboxMessage ctx.Transaction.Connection.Query cancellationToken

                match maybeClaimed with
                | None ->
                    let! commitResult = ctx.Commit()

                    match commitResult with
                    | Ok() -> shouldContinue <- false
                    | Error err ->
                        logger.LogError("Failed to commit empty outbox transaction: {error}", err.LogMessage)
                        shouldContinue <- false
                | Some claimed ->
                    let request =
                        { Method = claimed.Method
                          Url = claimed.Url
                          Headers = claimed.Headers
                          Body = claimed.Body
                          TimeoutMs = claimed.TimeoutMs
                          Retries = Some 0
                          RetryBaseDelayMs = Some claimed.RetryBaseDelayMs }

                    let! sendResult =
                        task {
                            try
                                let! response = dispatchHttp httpPolicy request cancellationToken
                                return Ok response
                            with e ->
                                return Error(fullUserMessage e)
                        }

                    match sendResult with
                    | Ok response when response.Status >= 200 && response.Status < 300 ->
                        do!
                            completeClaimedOutboxMessage
                                ctx.Transaction.Connection.Query
                                claimed.Id
                                response.Status
                                cancellationToken
                    | Ok response ->
                        let bodyPreview =
                            if String.IsNullOrEmpty(response.Body) then ""
                            elif response.Body.Length <= 600 then response.Body
                            else response.Body.Substring(0, 600)

                        let error = sprintf "HTTP %d from %s: %s" response.Status response.Url bodyPreview

                        do!
                            failClaimedOutboxMessage
                                ctx.Transaction.Connection.Query
                                claimed
                                (Some response.Status)
                                error
                                cancellationToken

                        logger.LogWarning(
                            "Outbox delivery failed (HTTP status) for message {id}: {status}",
                            claimed.Id,
                            response.Status
                        )
                    | Error err ->
                        do! failClaimedOutboxMessage ctx.Transaction.Connection.Query claimed None err cancellationToken

                        logger.LogWarning("Outbox delivery error for message {id}: {error}", claimed.Id, err)

                    let! commitResult = ctx.Commit()

                    match commitResult with
                    | Ok() -> processed <- processed + 1
                    | Error err ->
                        logger.LogError("Failed to commit outbox transaction: {error}", err.LogMessage)
                        shouldContinue <- false

            return processed
        }

    override this.ExecuteAsync(cancellationToken: CancellationToken) : Task =
        task {
            if not httpPolicy.Enabled then
                logger.LogWarning("Outbox worker is started while outbound HTTP is disabled")

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
                    logger.LogError(e, "Unhandled exception in outbox worker")
                    do! Task.Delay(pollDelay, cancellationToken)
        }
