module OzmaDB.OutboxWorker

open System
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Npgsql

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.API.Types
open OzmaDB.API.Request
open OzmaDB.API.API
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

    let pollDelay: TimeSpan =
        TimeSpan.FromMilliseconds(int64 (max 100 settings.PollDelayMs))

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

    // Notifies the action named in `onResponse` about the delivery outcome.
    //
    // Runs in its own transaction, deliberately: delivery is already committed by now.
    // Doing this inside the delivery transaction would mean a failing callback rolls the
    // delivery back and the worker sends the request again — for something like a fiscal
    // receipt a duplicate is far worse than a missing notification.
    let runDeliveryCallback
        (connectionString: string)
        (claimed: ClaimedOutboxMessage)
        (status: int option)
        (body: string)
        (deliveryError: string option)
        (cancellationToken: CancellationToken)
        : Task =
        task {
            match claimed.CallbackSchema, claimed.CallbackName with
            | Some schemaName, Some actionName ->
                try
                    let! cache = instancesCache.GetContextCache(connectionString)
                    use! ctx = cache.GetCache(cancellationToken)

                    let reqParams =
                        { Context = ctx
                          UserName = "__outbox_worker"
                          IsRoot = true
                          CanRead = true
                          Language = "en"
                          Theme = "default"
                          Quota = { MaxSize = None; MaxUsers = None } }

                    let! rctx = RequestContext.Create(reqParams)
                    let api = OzmaDBAPI(rctx)

                    let args =
                        match claimed.CallbackArgs with
                        | Some raw when not (String.IsNullOrWhiteSpace raw) ->
                            try
                                Newtonsoft.Json.Linq.JObject.Parse raw
                            with _ ->
                                Newtonsoft.Json.Linq.JObject()
                        | _ -> Newtonsoft.Json.Linq.JObject()

                    // The answer itself: parsed JSON when the body is JSON, raw text otherwise.
                    let bodyToken: Newtonsoft.Json.Linq.JToken =
                        if String.IsNullOrWhiteSpace body then
                            Newtonsoft.Json.Linq.JValue.CreateNull() :> Newtonsoft.Json.Linq.JToken
                        else
                            try
                                Newtonsoft.Json.Linq.JToken.Parse body
                            with _ ->
                                Newtonsoft.Json.Linq.JValue(body) :> Newtonsoft.Json.Linq.JToken

                    args.["ok"] <- Newtonsoft.Json.Linq.JValue(Option.isNone deliveryError)
                    args.["messageId"] <- Newtonsoft.Json.Linq.JValue(claimed.Id)
                    args.["url"] <- Newtonsoft.Json.Linq.JValue(claimed.Url)
                    args.["body"] <- bodyToken

                    args.["status"] <-
                        match status with
                        | Some code -> Newtonsoft.Json.Linq.JValue(code) :> Newtonsoft.Json.Linq.JToken
                        | None -> Newtonsoft.Json.Linq.JValue.CreateNull() :> Newtonsoft.Json.Linq.JToken

                    args.["error"] <-
                        match deliveryError with
                        | Some err -> Newtonsoft.Json.Linq.JValue(err) :> Newtonsoft.Json.Linq.JToken
                        | None -> Newtonsoft.Json.Linq.JValue.CreateNull() :> Newtonsoft.Json.Linq.JToken

                    let actionRef =
                        { Schema = OzmaQLName schemaName
                          Name = OzmaQLName actionName }
                        : ResolvedEntityRef

                    let! actionResult = api.Actions.RunAction { Action = actionRef; Args = Some args }

                    let callbackError =
                        match actionResult with
                        | Ok _ -> None
                        | Error err ->
                            let message = sprintf "%O" err

                            logger.LogWarning(
                                "Outbox callback {schema}.{name} failed for message {id}: {error}",
                                schemaName,
                                actionName,
                                claimed.Id,
                                message
                            )

                            Some message

                    do!
                        recordCallbackOutcome
                            ctx.Transaction.Connection.Query
                            claimed.Id
                            callbackError
                            cancellationToken

                    let! commitResult = ctx.Commit()

                    match commitResult with
                    | Ok() -> ()
                    | Error err ->
                        logger.LogError(
                            "Failed to commit outbox callback for message {id}: {error}",
                            claimed.Id,
                            err.LogMessage
                        )
                with e ->
                    logger.LogError(e, "Unhandled exception in outbox callback for message {id}", claimed.Id)
            | _ -> ()
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

                    // What to hand over to the callback once the delivery transaction commits.
                    let mutable callbackOutcome: (int option * string * string option) option = None

                    match sendResult with
                    | Ok response when response.Status >= 200 && response.Status < 300 ->
                        do!
                            completeClaimedOutboxMessage
                                ctx.Transaction.Connection.Query
                                claimed.Id
                                response.Status
                                response.Body
                                cancellationToken

                        callbackOutcome <- Some(Some response.Status, response.Body, None)
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

                        // Only report a failure the caller can act on: while retries remain,
                        // the request is still on its way and nothing is decided yet.
                        if claimed.Attempts > claimed.MaxRetries then
                            callbackOutcome <- Some(Some response.Status, response.Body, Some error)
                    | Error err ->
                        do! failClaimedOutboxMessage ctx.Transaction.Connection.Query claimed None err cancellationToken

                        logger.LogWarning("Outbox delivery error for message {id}: {error}", claimed.Id, err)

                        if claimed.Attempts > claimed.MaxRetries then
                            callbackOutcome <- Some(None, "", Some err)

                    let! commitResult = ctx.Commit()

                    match commitResult with
                    | Ok() ->
                        processed <- processed + 1

                        match callbackOutcome with
                        | Some(status, body, deliveryError) ->
                            do! runDeliveryCallback connectionString claimed status body deliveryError cancellationToken
                        | None -> ()
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
