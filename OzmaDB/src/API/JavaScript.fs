module OzmaDB.API.JavaScript

open Printf
open System
open System.Linq
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open Microsoft.Extensions.Logging
open Microsoft.ClearScript
open Microsoft.ClearScript.JavaScript
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open NodaTime

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Serialization.Utils
open OzmaDB.Exception
open OzmaDB.OzmaQL.Utils
open OzmaDB.API.Types
open OzmaDB.JavaScript.Json
open OzmaDB.JavaScript.Runtime
open OzmaDB.Outbox.HTTP
open OzmaDBSchema.System

[<NoComparison; NoEquality>]
type JSOutboxSettings =
    { Enabled: bool
      DefaultTimeoutMs: int
      MaxTimeoutMs: int
      MaxRetries: int
      RetryBaseDelayMs: int
      MaxBodyBytes: int }

[<NoComparison; NoEquality>]
type JSHostSettings =
    { HttpPolicy: OutboundHttpPolicy
      Outbox: JSOutboxSettings }

let defaultJSOutboxSettings =
    { Enabled = true
      DefaultTimeoutMs = 5000
      MaxTimeoutMs = 15000
      MaxRetries = 5
      RetryBaseDelayMs = 500
      MaxBodyBytes = 256 * 1024 }

let defaultJSHostSettings =
    { HttpPolicy = defaultOutboundHttpPolicy
      Outbox = defaultJSOutboxSettings }

[<SerializeAsObject("error")>]
type APICallErrorInfo =
    | [<CaseKey("call")>] ACECall of Details: string

    [<DataMember>]
    member this.Message =
        match this with
        | ACECall msg -> msg

    member this.ShouldLog = false

    static member private LookupKey = prepareLookupCaseKey<APICallErrorInfo>
    member this.Error = APICallErrorInfo.LookupKey this |> Option.get

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog
        member this.Details = Map.empty

    interface IErrorDetails with
        member this.Message = this.Message
        member this.LogMessage = this.Message
        member this.HTTPResponseCode = 500
        member this.Error = this.Error

// We don't declare these private because JSON serialization then breaks.
// See https://stackoverflow.com/questions/54169707/f-internal-visibility-changes-record-constructor-behavior
type WriteEventRequest = { Details: JToken }

type HttpRequestBody =
    { Url: string
      Method: string option
      Headers: JObject option
      Body: JToken option
      TimeoutMs: int option
      Retries: int option
      RetryBaseDelayMs: int option }

type HttpRequestResponse =
    { Status: int
      Url: string
      Headers: Map<string, string[]>
      Body: string
      Json: JToken option }

type EnqueueOutboxRequest =
    { Url: string
      Method: string option
      Headers: JObject option
      Body: JToken option
      TimeoutMs: int option
      MaxRetries: int option
      RetryBaseDelayMs: int option
      DelayMs: int option }

type EnqueueOutboxResponse = { Id: int }

type private APIHandle(api: IOzmaDBAPI) =
    let logger = api.Request.Context.LoggerFactory.CreateLogger<APIHandle>()

    member this.API = api
    member this.Logger = logger

let private preludeSource =
    """
    const unwrappedApiProxy = globalThis.unwrappedApiProxy;
    delete globalThis.unwrappedApiProxy;
    delete globalThis.unwrapHostResult;
    delete globalThis.wrapHostFunction;
    const wrapHostObject = globalThis.wrapHostObject;
    delete globalThis.wrapHostObject;

    const apiProxy = wrapHostObject(unwrappedApiProxy);

    const objectValues = Object.values;
    const objectAssign = Object.assign;
    const objectFreeze = Object.freeze;
    const arrayMap = Array.prototype.map;

    const findInnerByKey = (object, key) => {
        for (const value of objectValues(object)) {
            if (typeof value === 'object' && value) {
                const currValue = object[key];
                if (currValue !== undefined) {
                    return currValue;
                }
                const nestedValue = findInnerByKey(value, key);
                if (nestedValue !== undefined) {
                    return nestedValue;
                }
            }
        }
    };

    class OzmaDBError extends Error {
        constructor(body) {
            super(body.message);
            objectAssign(this, body);
            // Find `userData` and bring it to the top-level.
            if (!('userData' in body)) {
                const userData = findInnerByKey(body, 'userData');
                if (userData !== undefined) {
                    this.userData = userData;
                }
            }
        }
    };

    globalThis.OzmaDBError = OzmaDBError;
    // DEPRECATED
    globalThis.FunDBError = OzmaDBError;

    globalThis.formatDate = (date) => date.toISOString().split('T')[0];
    globalThis.formatOzmaQLName = apiProxy.FormatOzmaQLName;
    globalThis.formatOzmaQLValue = apiProxy.FormatOzmaQLValue;

    // DEPRECATED
    globalThis.renderDate = globalThis.formatDate;
    globalThis.renderFunQLName = globalThis.formatOzmaQLName;
    globalThis.renderFunQLValue = globalThis.formatOzmaQLValue;
    globalThis.formatFunQLName = globalThis.formatOzmaQLName;
    globalThis.formatFunQLValue = globalThis.formatOzmaQLValue;

    const normalizeSource = (source) => {
        if (source.ref) {
            return { ...source, ...source.ref };
        } else {
            return source;
        }
    };

    class OzmaDBCurrent {
        constructor() {
            objectFreeze(this);
        }

        getUserView(source, args, chunk) {
            return apiProxy.GetUserView({ source: normalizeSource(source), args, chunk });
        };

        getUserViewInfo(source) {
            return apiProxy.GetUserViewInfo({ source: normalizeSource(source) });
        };

        getEntityInfo(entity) {
            return apiProxy.GetEntityInfo({ entity });
        };

        async insertEntry(entity, fields) {
            try {
                const ret = await this.insertEntries(entity, [fields]);
                return ret.entries[0];
            } catch (e) {
                if (e.error === 'transaction') {
                    // We want to keep the stack trace, so we mutate the exception.
                    const inner = e.inner;
                    delete e.operation;
                    delete e.inner;
                    objectAssign(e, inner);
                }
                throw e;
            }
        };

        // DEPRECATED
        async insertEntities(entity, entries) {
            const ret = await this.insertEntries(entity, entries);
            return arrayMap.call(ret.entries, entry => entry.id);
        }

        insertEntries(entity, entries) {
            return apiProxy.InsertEntries({ entity, entries });
        };

        // DEPRECATED
        async updateEntity(entity, id, fields) {
            const ret = await this.updateEntry(entity, id, fields);
            return ret.id;
        };

        updateEntry(entity, id, fields) {
            return apiProxy.UpdateEntry({ entity, id, fields });
        };

        // DEPRECATED
        deleteEntity(entity, id) {
            return this.deleteEntry(entity, id);
        };

        deleteEntry(entity, id) {
            return apiProxy.DeleteEntry({ entity, id });
        };

        // DEPRECATED
        getRelatedEntities(entity, id) {
            return this.getRelatedEntries(entity, id);
        };

        getRelatedEntries(entity, id) {
            return apiProxy.GetRelatedEntries({ entity, id });
        };

        // DEPRECATED
        recursiveDeleteEntity(entity, id) {
            return this.recursiveDeleteEntry(entity, id);
        };

        recursiveDeleteEntry(entity, id) {
            return apiProxy.RecursiveDeleteEntry({ entity, id });
        };

        runCommand(command, args) {
            return apiProxy.RunCommand({ command, args });
        };

        deferConstraints(func) {
            return apiProxy.DeferConstraints(func);
        };

        pretendRole(asRole, func) {
            return apiProxy.PretendRole({ asRole }, func);
        };

        getDomainValues(entity, id, chunk) {
            return apiProxy.GetDomainValues({ entity, id, chunk });
        };

        writeEvent(details) {
            return apiProxy.WriteEvent({ details });
        };

        writeEventSync(details) {
            return apiProxy.WriteEventSync({ details });
        };

        httpRequest(req) {
            return apiProxy.HttpRequest(req);
        };

        enqueueHttpRequest(req) {
            return apiProxy.EnqueueOutboxHttpRequest(req);
        };

        cancelWith(userData, message) {
            throw new OzmaDBError({ message, userData });
        };
    };

    class OzmaDB1 extends OzmaDBCurrent {
        // DEPRECATED
        async insertEntity(entity, fields) {
            const ret = await this.insertEntry(entity, fields);
            return ret.id;
        }

        // DEPRECATED
        async insertEntities(entity, entries) {
            const ret = await this.insertEntries(entity, entries);
            return arrayMap.call(ret.entries, entry => entry.id);
        }

        // DEPRECATED
        async updateEntity(entity, id, fields) {
            const ret = await this.updateEntry(entity, id, fields);
            return ret.id;
        };

        // DEPRECATED
        deleteEntity(entity, id) {
            return this.deleteEntry(entity, id);
        };

        // DEPRECATED
        getRelatedEntities(entity, id) {
            return this.getRelatedEntries(entity, id);
        };

        // DEPRECATED
        recursiveDeleteEntity(entity, id) {
            return this.recursiveDeleteEntry(entity, id);
        };
    };

    globalThis.OzmaDB = new OzmaDB1();
    // DEPRECATED
    globalThis.FunDB = globalThis.OzmaDB;
"""

let private preludeDoc =
    let info = DocumentInfo("ozmadb_prelude.js", Category = ModuleCategory.Standard)
    RuntimeLocal(fun runtime -> runtime.Runtime.Compile(info, preludeSource))

[<DefaultScriptUsage(ScriptAccess.None)>]
type OzmaJSEngine(runtime: JSRuntime, env: JSEnvironment, settings: JSHostSettings) as this =
    inherit SchedulerJSEngine<Task.SerializingTrackingTaskScheduler>(runtime, env)

    let mutable topLevelAPI = None: IOzmaDBAPI option
    let apiHandle = AsyncLocal<APIHandle>()
    let httpPolicy = normalizePolicy settings.HttpPolicy
    let outboxSettings = settings.Outbox

    do
        this.Engine.AddHostObject("unwrappedApiProxy", this)
        ignore <| this.Engine.Evaluate(preludeDoc.GetValue(this.Runtime))

    let errorConstructor = this.Engine.Global.["OzmaDBError"] :?> IJavaScriptObject

    member inline private this.ThrowErrorWithInner (e: #IErrorDetails) (innerException: exn) : 'b =
        let body = this.Json.Serialize(e)
        let exc = errorConstructor.Invoke(true, body) :?> IJavaScriptObject
        raise <| JSException(e.Message, exc, innerException)

    member inline private this.ThrowError(e: #IErrorDetails) : 'b = this.ThrowErrorWithInner e null

    member private this.FormatErrorWithInner (innerException: exn) format =
        let thenRaise str =
            this.ThrowErrorWithInner (ACECall str) innerException

        ksprintf thenRaise format

    member private this.FormatError format = this.FormatErrorWithInner null format

    member private this.ClampOrDefault (value: int option) (defaultValue: int) (minValue: int) (maxValue: int) =
        value |> Option.defaultValue defaultValue |> min maxValue |> max minValue

    member private this.HeadersMap (headers: JObject option) : Map<string, string> =
        match headers with
        | None -> Map.empty
        | Some raw ->
            raw.Properties()
            |> Seq.choose (fun p ->
                match p.Value with
                | :? JValue as v when v.Type = JTokenType.String -> Some(p.Name, string v.Value)
                | _ -> None)
            |> Map.ofSeq

    member private this.WithSerializedBody (headers: Map<string, string>) (body: JToken option) : Map<string, string> * string option =
        match body with
        | None -> (headers, None)
        | Some token when token.Type = JTokenType.Null -> (headers, None)
        | Some token when token.Type = JTokenType.String ->
            let txt = token.Value<string>()
            (headers, Some txt)
        | Some token ->
            let hasContentType =
                headers
                |> Seq.exists (fun (KeyValue(k, _)) -> String.Equals(k, "content-type", StringComparison.OrdinalIgnoreCase))

            let headers =
                if hasContentType then headers else Map.add "content-type" "application/json" headers

            let txt = JsonConvert.SerializeObject(token, Formatting.None)
            (headers, Some txt)

    member private this.ParseResponseJson(body: string) : JToken option =
        if String.IsNullOrWhiteSpace(body) then
            None
        else
            try
                Some <| JToken.Parse(body)
            with _ ->
                None

    member private this.GetCurrentSchemaId() : int option = None

    member private this.Deserialize(v: obj) : 'a =
        let ret =
            try
                V8JsonReader.Deserialize<'a>(v)
            with :? JsonException as e ->
                this.FormatErrorWithInner e "Failed to parse value: %s" e.Message

        if isRefNull ret then
            this.FormatError "Value must not be null"

        ret

    member private this.GetHandle() =
        let handle = apiHandle.Value

        if isRefNull handle then
            this.FormatError "This API call cannot be used in this context"
        else
            handle

    member inline private this.WrapApiCall<'Result, 'Error when 'Error :> IErrorDetails>
        ([<InlineIfLambda>] wrap: APIHandle -> (unit -> Task<'Result>) -> Task<Result<'Result, 'Error>>)
        ([<InlineIfLambda>] f: APIHandle -> Task<'Result>)
        : Task<'Result> =
        task {
            let handle = this.GetHandle()
            let! ret = wrap handle <| fun () -> f handle

            match ret with
            | Ok r -> return r
            | Error e -> return this.ThrowError e
        }

    member inline private this.RunVoidApiCall([<InlineIfLambda>] f: APIHandle -> Task) : Task =
        task {
            let handle = this.GetHandle()
            do! f handle
        }

    member inline private this.RunResultApiCall<'a, 'e when 'e :> IErrorDetails>
        ([<InlineIfLambda>] f: APIHandle -> Task<Result<'a, 'e>>)
        : Task<obj> =
        task {
            let handle = this.GetHandle()
            let! res = f handle

            match res with
            | Ok r -> return this.Json.Serialize(r)
            | Error e -> return this.ThrowError e
        }

    member inline private this.RunVoidResultApiCall<'e when 'e :> IErrorDetails>
        ([<InlineIfLambda>] f: APIHandle -> Task<Result<unit, 'e>>)
        : Task =
        task {
            let handle = this.GetHandle()
            let! res = f handle

            match res with
            | Ok r -> ()
            | Error e -> this.ThrowError e
        }

    member inline private this.SimpleApiCall
        (arg: obj)
        ([<InlineIfLambda>] f: APIHandle -> 'Request -> Task<Result<'Response, 'Error>>)
        =
        let req = this.Deserialize arg: 'Request

        this.WrapAsyncHostFunction
        <| fun () -> this.RunResultApiCall <| fun handle -> f handle req

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.FormatOzmaQLName(name: string) =
        this.WrapHostFunction <| fun () -> renderOzmaQLName name

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.FormatOzmaQLValue(arg: obj) =
        this.WrapHostFunction
        <| fun () ->
            use reader = new V8JsonReader(arg)

            let source =
                try
                    JToken.Load(reader)
                with :? JsonReaderException as e ->
                    this.FormatError "Failed to parse value: %s" e.Message

            try
                renderOzmaQLJson source
            with Failure msg ->
                this.FormatError "%s" msg

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetUserView arg =
        this.SimpleApiCall arg (fun handle -> handle.API.UserViews.GetUserView)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetUserViewInfo arg =
        this.SimpleApiCall arg (fun handle -> handle.API.UserViews.GetUserViewInfo)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetEntityInfo arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.GetEntityInfo)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.InsertEntries arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.InsertEntries)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.UpdateEntry arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.UpdateEntry)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.DeleteEntry arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.DeleteEntry)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetRelatedEntries arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.GetRelatedEntries)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.RecursiveDeleteEntry arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.RecursiveDeleteEntry)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.RunCommand arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Entities.RunCommand)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.DeferConstraints(f: IJavaScriptObject) =
        this.WrapAsyncHostFunction
        <| fun () ->
            this.WrapApiCall(fun handle -> handle.API.Entities.DeferConstraints)
            <| fun handle -> this.RunAsyncJSFunction(f, [||])

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.PretendRole (req: obj) (f: IJavaScriptObject) =
        this.WrapAsyncHostFunction
        <| fun () ->
            let req = this.Deserialize req: PretendRoleRequest

            this.WrapApiCall(fun handle -> handle.API.Request.PretendRole req)
            <| fun handle -> this.RunAsyncJSFunction(f, [||])

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.GetDomainValues arg =
        this.SimpleApiCall arg (fun handle -> handle.API.Domains.GetDomainValues)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.WriteEvent(arg: obj) =
        this.WrapHostFunction
        <| fun () ->
            let req = this.Deserialize arg: WriteEventRequest
            let handle = this.GetHandle()

            handle.Logger.LogInformation(
                "Source {source} wrote event from JavaScript: {details}",
                handle.API.Request.Source,
                req.Details.ToString()
            )

            handle.API.Request.WriteEvent(fun event ->
                event.Type <- "writeEvent"
                event.Request <- JsonConvert.SerializeObject req)

            Undefined.Value

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.WriteEventSync(arg: obj) =
        let req = this.Deserialize arg: WriteEventRequest

        this.RunVoidApiCall
        <| fun handle ->
            handle.Logger.LogInformation(
                "Source {source} wrote sync event from JavaScript: {details}",
                handle.API.Request.Source,
                req.Details.ToString()
            )

            handle.API.Request.WriteEventSync(fun event ->
                event.Type <- "writeEvent"
                event.Request <- JsonConvert.SerializeObject req)

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.HttpRequest(arg: obj) =
        this.WrapAsyncHostFunction
        <| fun () ->
            task {
                let req = this.Deserialize arg: HttpRequestBody
                let headers = this.HeadersMap req.Headers
                let (headers, body) = this.WithSerializedBody headers req.Body

                let timeoutMs =
                    this.ClampOrDefault req.TimeoutMs httpPolicy.DefaultTimeoutMs 1 httpPolicy.MaxTimeoutMs

                let retries = this.ClampOrDefault req.Retries httpPolicy.MaxRetries 0 httpPolicy.MaxRetries
                let retryBaseDelayMs = this.ClampOrDefault req.RetryBaseDelayMs httpPolicy.RetryBaseDelayMs 1 30000

                let method =
                    req.Method
                    |> Option.defaultValue "GET"
                    |> fun m -> m.Trim().ToUpperInvariant()

                let request: HttpDispatchRequest =
                    { Method = method
                      Url = req.Url
                      Headers = headers
                      Body = body
                      TimeoutMs = Some timeoutMs
                      Retries = Some retries
                      RetryBaseDelayMs = Some retryBaseDelayMs }

                let cancellationToken = this.GetHandle().API.Request.Context.CancellationToken
                let! response = dispatchHttp httpPolicy request cancellationToken

                let parsed =
                    { Status = response.Status
                      Url = response.Url
                      Headers = response.Headers
                      Body = response.Body
                      Json = this.ParseResponseJson response.Body }

                return this.Json.Serialize(parsed)
            }

    [<ScriptUsage(ScriptAccess.Full)>]
    member this.EnqueueOutboxHttpRequest(arg: obj) =
        this.WrapAsyncHostFunction
        <| fun () ->
            task {
                if not outboxSettings.Enabled then
                    this.FormatError "Outbox is disabled"

                let req = this.Deserialize arg: EnqueueOutboxRequest
                let headers = this.HeadersMap req.Headers
                let (headers, body) = this.WithSerializedBody headers req.Body

                match body with
                | Some b when Encoding.UTF8.GetByteCount(b) > outboxSettings.MaxBodyBytes ->
                    this.FormatError "Outbox body exceeds max configured size"
                | _ -> ()

                match validateUrlAgainstPolicy httpPolicy req.Url with
                | Error err -> this.FormatError "%s" err
                | Ok _ -> ()

                let timeoutMs =
                    this.ClampOrDefault req.TimeoutMs outboxSettings.DefaultTimeoutMs 1 outboxSettings.MaxTimeoutMs

                let maxRetries = this.ClampOrDefault req.MaxRetries outboxSettings.MaxRetries 0 1000

                let retryBaseDelayMs =
                    this.ClampOrDefault req.RetryBaseDelayMs outboxSettings.RetryBaseDelayMs 1 30000

                let method =
                    req.Method
                    |> Option.defaultValue "POST"
                    |> fun m -> m.Trim().ToUpperInvariant()

                let dueAt =
                    let delayMs = req.DelayMs |> Option.defaultValue 0 |> max 0
                    SystemClock.Instance.GetCurrentInstant() + Duration.FromMilliseconds(float delayMs)

                let schemaId = this.GetCurrentSchemaId()
                let handle = this.GetHandle()
                let tx = handle.API.Request.Context.Transaction
                let headersJson = JsonConvert.SerializeObject(headers, Formatting.None)
                let bodyObj = body |> Option.toObj
                let timeoutMsObj = timeoutMs |> Nullable
                let schemaIdObj = schemaId |> Option.toNullable

                let outbox = OutboxMessage()
                outbox.SchemaId <- schemaIdObj
                outbox.Method <- method
                outbox.Url <- req.Url
                outbox.Headers <- headersJson
                outbox.Body <- bodyObj
                outbox.TimeoutMs <- timeoutMsObj
                outbox.MaxRetries <- maxRetries
                outbox.RetryBaseDelayMs <- retryBaseDelayMs
                outbox.DueAt <- dueAt
                outbox.Attempts <- 0

                ignore <| tx.System.OutboxMessages.Add(outbox)
                let! _ = tx.SystemSaveChangesAsync(handle.API.Request.Context.CancellationToken)

                return this.Json.Serialize({ Id = outbox.Id }: EnqueueOutboxResponse)
            }

    override this.CreateScheduler() = Task.SerializingTrackingTaskScheduler()

    member this.SetAPI(api: IOzmaDBAPI) =
        assert (Option.isNone topLevelAPI)
        topLevelAPI <- Some api

    member this.ResetAPI api = topLevelAPI <- None

    // Ugh. Workarounds.
    member private this.BaseRunAsyncJSFunction
        (func: IJavaScriptObject, args: obj[], whenPromiseFinished: (unit -> unit), cancellationToken: CancellationToken) =
        base.RunAsyncJSFunction(func, args, whenPromiseFinished, cancellationToken)

    override this.RunAsyncJSFunction
        (func: IJavaScriptObject, args: obj[], whenPromiseFinished: (unit -> unit), cancellationToken: CancellationToken) : Task<
                                                                                                                                obj
                                                                                                                             >
        =
        task {
            if isRefNull apiHandle.Value then
                let ozma =
                    match topLevelAPI with
                    | None -> failwith "API handle is not set"
                    | Some api -> api

                apiHandle.Value <- APIHandle(ozma)

            return! this.BaseRunAsyncJSFunction(func, args, whenPromiseFinished, cancellationToken)
        }
