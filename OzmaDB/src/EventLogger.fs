module OzmaDB.EventLogger

open System
open System.Data
open System.Collections.Generic
open System.Reflection
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

open OzmaDB.OzmaUtils
open OzmaDBSchema.System
open OzmaDB.Connection

type EventLoggerSettings =
    { QueueCapacity: int
      MaxFieldLength: int
      WriteEventSampleRate: float }

let defaultEventLoggerSettings =
    { QueueCapacity = 10000
      MaxFieldLength = 16384
      WriteEventSampleRate = 1.0 }

type EventLogger(loggerFactory: ILoggerFactory, settings: EventLoggerSettings) =
    inherit BackgroundService()

    let queueCapacity = max 1 settings.QueueCapacity
    let boundedOptions = BoundedChannelOptions(queueCapacity)

    do
        boundedOptions.SingleReader <- true
        boundedOptions.SingleWriter <- false
        boundedOptions.FullMode <- BoundedChannelFullMode.DropOldest

    let chan = Channel.CreateBounded<string * EventEntry>(boundedOptions)
    let logger = loggerFactory.CreateLogger<EventLogger>()

    let sampleRate =
        if
            Double.IsNaN(settings.WriteEventSampleRate)
            || Double.IsInfinity(settings.WriteEventSampleRate)
        then
            1.0
        else
            min 1.0 (max 0.0 settings.WriteEventSampleRate)

    let truncateString (value: string) =
        if
            settings.MaxFieldLength <= 0
            || String.IsNullOrEmpty(value)
            || value.Length <= settings.MaxFieldLength
        then
            value
        else
            value.Substring(0, settings.MaxFieldLength) + "...[truncated]"

    let eventEntryType = typeof<EventEntry>

    let tryGetProperty (name: string) : PropertyInfo option =
        eventEntryType.GetProperty(name, BindingFlags.Public ||| BindingFlags.Instance)
        |> Option.ofObj

    let requestProp = tryGetProperty "Request"
    let responseProp = tryGetProperty "Response"
    let errorProp = tryGetProperty "Error"
    let detailsProp = tryGetProperty "Details"
    let typeProp = tryGetProperty "Type"

    let truncateField (prop: PropertyInfo option) (entry: EventEntry) =
        match prop with
        | None -> ()
        | Some p ->
            match p.GetValue(entry) with
            | :? string as v when not <| String.IsNullOrEmpty(v) ->
                let next = truncateString v

                if not <| obj.ReferenceEquals(v, next) then
                    p.SetValue(entry, next)
            | _ -> ()

    let sanitizeEntry (entry: EventEntry) =
        truncateField requestProp entry
        truncateField responseProp entry
        truncateField errorProp entry
        truncateField detailsProp entry
        entry

    let isWriteEvent (entry: EventEntry) =
        match typeProp with
        | Some p ->
            match p.GetValue(entry) with
            | :? string as v -> String.Equals(v, "writeEvent", StringComparison.Ordinal)
            | _ -> false
        | None -> false

    let shouldKeep (entry: EventEntry) =
        not (isWriteEvent entry)
        || sampleRate >= 1.0
        || Random.Shared.NextDouble() <= sampleRate

    override this.ExecuteAsync(cancellationToken: CancellationToken) : Task =
        task {
            while not cancellationToken.IsCancellationRequested do
                match! chan.Reader.WaitToReadAsync cancellationToken with
                | false -> ()
                | true ->
                    let databaseConnections = Dictionary<string, DatabaseTransaction>()

                    use _ =
                        Task.toDisposable
                        <| fun () ->
                            task {
                                for KeyValue(connectionString, transaction) in databaseConnections do
                                    do! (transaction :> IAsyncDisposable).DisposeAsync()
                                    do! (transaction.Connection :> IAsyncDisposable).DisposeAsync()
                            }

                    do!
                        Task.loop ()
                        <| fun () ->
                            task {
                                match chan.Reader.TryRead() with
                                | (false, _) -> return Task.StopLoop()
                                | (true, (connectionString, entry)) ->
                                    try
                                        match databaseConnections.TryGetValue(connectionString) with
                                        | (false, _) ->
                                            let! transaction =
                                                openAndCheckTransaction
                                                    loggerFactory
                                                    connectionString
                                                    IsolationLevel.ReadCommitted
                                                    cancellationToken
                                                <| fun transaction ->
                                                    task {
                                                        let! _ = transaction.System.Events.AddAsync(entry)
                                                        // Check that connection works the first time.
                                                        let! _ = transaction.System.SaveChangesAsync(cancellationToken)

                                                        return transaction
                                                    }

                                            databaseConnections.Add(connectionString, transaction)
                                        | (true, transaction) ->
                                            let! _ = transaction.System.Events.AddAsync(entry)
                                            ()
                                    with ex ->
                                        logger.LogError(ex, "Exception while logging event")

                                    return Task.NextLoop()
                            }

                    let mutable totalChanged = 0

                    for KeyValue(connectionString, transaction) in databaseConnections do
                        try
                            let! changed = transaction.Commit(cancellationToken)
                            totalChanged <- totalChanged + changed
                        with ex ->
                            logger.LogError(ex, "Exception while commiting logged event")

                    if totalChanged > 0 then
                        logger.LogInformation("Logged {count} events into databases", totalChanged)
        }

    member this.WriteEvent(connectionString: string, entry: EventEntry) =
        if shouldKeep entry then
            let sanitized = sanitizeEntry entry

            if not <| chan.Writer.TryWrite((connectionString, sanitized)) then
                logger.LogWarning("Event queue is full; dropping event entry")
