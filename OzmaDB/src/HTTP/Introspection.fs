module OzmaDB.HTTP.Introspection

open System
open System.Numerics
open System.Runtime.Serialization
open System.Text
open System.Threading.Tasks
open Giraffe
open Microsoft.AspNetCore.Http
open Giraffe.EndpointRouting
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Newtonsoft.Json.Linq

open OzmaDB.OzmaUtils.Serialization.Utils
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.API.Types
open OzmaDB.HTTP.Utils
open OzmaDB.HTTP.Entities

// TODO: make it private after we fix JSON serialization for the private F# types.
[<SerializeAsObject("error")>]
type IntrospectionErrorInfo =
    | [<CaseKey("accessDenied")>] IEAccessDenied
    | [<CaseKey("entityNotFound")>] IEEntityNotFound of Entity: string
    | [<CaseKey("fieldNotFound")>] IEFieldNotFound of Field: string
    | [<CaseKey("fullDryRunDisabled")>] IEFullDryRunDisabled
    | [<CaseKey("rawSqlDisabled")>] IERawSqlDisabled
    | [<CaseKey("rawSqlUnavailable")>] IERawSqlUnavailable
    | [<CaseKey("reportNotFound")>] IEReportNotFound of Report: string

    member this.LogMessage =
        match this with
        | IEAccessDenied -> "Introspection is available only to root"
        | IEEntityNotFound entity -> sprintf "Entity not found: %s" entity
        | IEFieldNotFound field -> sprintf "Field not found: %s" field
        | IEFullDryRunDisabled -> "full dry-run is disabled for this instance"
        | IERawSqlDisabled -> "raw SQL is disabled for this instance"
        | IERawSqlUnavailable -> "raw SQL is unavailable: database has dblink/postgres_fdw or a privileged role"
        | IEReportNotFound report -> sprintf "Report not found: %s" report

    [<DataMember>]
    member this.Message = this.LogMessage

    member this.ShouldLog = false
    member this.Details = Map.empty

    member this.HTTPResponseCode =
        match this with
        | IEAccessDenied -> 403
        | IEEntityNotFound _ -> 404
        | IEFieldNotFound _ -> 404
        | IEFullDryRunDisabled -> 403
        | IERawSqlDisabled -> 403
        | IERawSqlUnavailable -> 403
        | IEReportNotFound _ -> 404

    static member private LookupKey = prepareLookupCaseKey<IntrospectionErrorInfo>
    member this.Error = IntrospectionErrorInfo.LookupKey this |> Option.get

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog
        member this.Details = this.Details

    interface IErrorDetails with
        member this.LogMessage = this.LogMessage
        member this.Message = this.Message
        member this.HTTPResponseCode = this.HTTPResponseCode
        member this.Error = this.Error

// TODO: make it private after we fix JSON serialization for the private F# types.
type CapabilitiesResponse = { Version: int; Features: string[] }

// Same shape as the `/transaction` body plus an optional `mode`.
// TODO: make it private after we fix JSON serialization for the private F# types.
[<NoEquality; NoComparison>]
type DryRunHTTPRequest =
    { Operations: TransactionOp[]
      DeferConstraints: bool
      ForceAllowBroken: bool
      [<DataMember(EmitDefaultValue = false)>]
      Mode: string option }

// TODO: make it private after we fix JSON serialization for the private F# types.
[<NoEquality; NoComparison>]
type DryRunResponse =
    { Ok: bool
      Errors: ValidateErrorItem[]
      Results: TransactionOpResponse[]
      Ddl: string[]
      BrokenObjects: string[] }

// TODO: make it private after we fix JSON serialization for the private F# types.
[<NoEquality; NoComparison>]
type SqlHTTPRequest =
    { Sql: string
      [<DataMember(EmitDefaultValue = false)>]
      Params: JToken[] option
      [<DataMember(EmitDefaultValue = false)>]
      TimeoutMs: int option
      [<DataMember(EmitDefaultValue = false)>]
      MaxRows: int option }

// `ozmaDB:introspection` configuration section.
type IntrospectionSettings =
    { // A full dry run holds the migration locks and locks on altered tables while it runs.
      AllowFullDryRun: bool
      // Runs in a read-only transaction that is never committed, and is refused when the database has
      // dblink/postgres_fdw or the database role is privileged. Still, root can read anything the role can and
      // call side-effecting functions allowed in read-only mode, e.g. pg_terminate_backend/pg_cancel_backend on
      // other sessions of the same role.
      AllowRawSql: bool }

let readIntrospectionSettings (config: IConfiguration) : IntrospectionSettings =
    let section = config.GetSection("OzmaDB").GetSection("Introspection")

    { AllowFullDryRun = section.GetValue("AllowFullDryRun", false)
      AllowRawSql = section.GetValue("AllowRawSql", false) }

let private capabilities (settings: IntrospectionSettings) =
    let sql = if settings.AllowRawSql then [| "sql" |] else [||]

    { Version = 1
      Features =
        Array.concat
            [ [| "validate"; "schema"; "dependencies"; "search"; "code_check"; "dry_run" |]
              sql
              [| "pg_reports" |] ] }

let private sqlParam (token: JToken) : Result<SqlParam, string> =
    if isNull token then
        Ok SPNull
    else
        match token.Type with
        | JTokenType.Null -> Ok SPNull
        | JTokenType.String -> Ok(SPString(token.Value<string>()))
        | JTokenType.Boolean -> Ok(SPBool(token.Value<bool>()))
        | JTokenType.Integer ->
            match (token :?> JValue).Value with
            | :? BigInteger as i when i < BigInteger(Decimal.MinValue) || i > BigInteger(Decimal.MaxValue) ->
                Error "Numeric parameter is out of range"
            | :? BigInteger as i -> Ok(SPDecimal(decimal i))
            | v -> Ok(SPInt(Convert.ToInt64 v))
        | JTokenType.Float ->
            try
                Ok(SPDecimal(Convert.ToDecimal((token :?> JValue).Value)))
            with :? OverflowException ->
                Error "Numeric parameter is out of range"
        | _ -> Error "Parameters must be strings, numbers, booleans or nulls"

let private validateSqlRequest (req: SqlHTTPRequest) : Result<SqlQueryRequest, string> =
    let query =
        if isNull req.Sql then
            ""
        else
            let mutable length = req.Sql.Length

            while length > 0
                  && (req.Sql.[length - 1] = ';' || Char.IsWhiteSpace req.Sql.[length - 1]) do
                length <- length - 1

            req.Sql.Substring(0, length)

    let timeoutMs = Option.defaultValue 5000 req.TimeoutMs
    let maxRows = Option.defaultValue 500 req.MaxRows

    let pars =
        req.Params
        |> Option.defaultValue [||]
        |> Array.fold
            (fun acc token ->
                match acc, sqlParam token with
                | Ok pars, Ok par -> Ok(par :: pars)
                | (Error _ as e), _ -> e
                | _, Error e -> Error e)
            (Ok [])

    if query = "" then
        Error "Field sql is required and must not be empty"
    else if timeoutMs < 1 || timeoutMs > 30000 then
        Error "Field timeoutMs must be between 1 and 30000"
    else if maxRows < 1 || maxRows > 5000 then
        Error "Field maxRows must be between 1 and 5000"
    else
        match pars with
        | Error e -> Error e
        | Ok pars ->
            Ok
                { Sql = query
                  Params = pars |> List.rev |> List.toArray
                  TimeoutMs = timeoutMs
                  MaxRows = maxRows }

let private maxCodeCheckSourceSize = 1024 * 1024

let private requireRoot (api: IOzmaDBAPI) (f: unit -> Task<HttpJobResponse>) : Task<HttpJobResponse> =
    match api.Request.User.Effective.Type with
    | RTRoot -> f ()
    | RTRole _ -> Task.FromResult(jobError IEAccessDenied)

let introspectionApi (serviceProvider: IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let settings =
        readIntrospectionSettings (serviceProvider.GetRequiredService<IConfiguration>())

    let getCapabilities =
        utils.PerformReadJob
        <| fun api -> requireRoot api (fun () -> Task.FromResult(jobJson (capabilities settings)))

    let validateQuery =
        safeBindJson
        <| fun (req: ValidateQueryRequest) ->
            utils.PerformReadJob
            <| fun api ->
                requireRoot api
                <| fun () ->
                    task {
                        let! ret = api.Introspection.ValidateQuery req
                        return jobJson ret
                    }

    let getSchemas =
        utils.PerformReadJob
        <| fun api ->
            requireRoot api
            <| fun () ->
                task {
                    let! ret = api.Introspection.GetSchemas()
                    return jobJson ret
                }

    let getEntity (schema: string, name: string) : HttpHandler =
        utils.PerformReadJob
        <| fun api ->
            requireRoot api
            <| fun () ->
                task {
                    let ref =
                        { Schema = OzmaQLName schema
                          Name = OzmaQLName name }

                    match! api.Introspection.GetEntity ref with
                    | Some ret -> return jobJson ret
                    | None -> return jobError (IEEntityNotFound(string ref))
                }

    let getDependencies (next: HttpFunc) (ctx: HttpContext) =
        match ctx.TryGetQueryStringValue "schema", ctx.TryGetQueryStringValue "entity" with
        | Some schema, Some entity ->
            let ref =
                { Schema = OzmaQLName schema
                  Name = OzmaQLName entity }

            let field = ctx.TryGetQueryStringValue "field" |> Option.map OzmaQLName

            let handler =
                utils.PerformReadJob
                <| fun api ->
                    requireRoot api
                    <| fun () ->
                        task {
                            match! api.Introspection.GetDependencies ref field with
                            | Ok ret -> return jobJson ret
                            | Error DEEntityNotFound -> return jobError (IEEntityNotFound(string ref))
                            | Error DEFieldNotFound ->
                                let fieldName = field |> Option.map string |> Option.defaultValue ""
                                return jobError (IEFieldNotFound(sprintf "%O.%s" ref fieldName))
                        }

            handler next ctx
        | _ -> requestError (RIRequest "Query parameters schema and entity are required") next ctx

    let search (next: HttpFunc) (ctx: HttpContext) =
        let boolParam name =
            match ctx.TryGetQueryStringValue name with
            | None -> Ok false
            | Some v ->
                match Boolean.TryParse v with
                | (true, b) -> Ok b
                | _ -> Error(sprintf "Query parameter %s must be true or false" name)

        let kinds =
            match ctx.TryGetQueryStringValue "kinds" with
            | None
            | Some "" -> Ok(searchKinds |> Map.keys |> Seq.toArray)
            | Some v ->
                let names =
                    v.Split(',', StringSplitOptions.RemoveEmptyEntries ||| StringSplitOptions.TrimEntries)

                match names |> Array.tryFind (fun k -> not (Map.containsKey k searchKinds)) with
                | Some unknown -> Error(sprintf "Unknown kind: %s" unknown)
                | None -> Ok names

        let limit =
            match ctx.TryGetQueryStringValue "limit" with
            | None -> Ok 200
            | Some v ->
                match Int32.TryParse v with
                | (true, n) when n > 0 -> Ok(min n 2000)
                | _ -> Error "Query parameter limit must be a positive integer"

        let query =
            match ctx.TryGetQueryStringValue "q" with
            | Some q when q <> "" -> Ok q
            | _ -> Error "Query parameter q is required and must not be empty"

        match query, boolParam "regex", boolParam "caseSensitive", kinds, limit with
        | Ok q, Ok isRegex, Ok caseSensitive, Ok kinds, Ok limit ->
            let req =
                { Query = q
                  IsRegex = isRegex
                  CaseSensitive = caseSensitive
                  Kinds = kinds
                  Limit = limit }

            let handler =
                utils.PerformReadJob
                <| fun api ->
                    requireRoot api
                    <| fun () ->
                        task {
                            let! ret = api.Introspection.Search req
                            return jobJson ret
                        }

            handler next ctx
        | Error e, _, _, _, _
        | _, Error e, _, _, _
        | _, _, Error e, _, _
        | _, _, _, Error e, _
        | _, _, _, _, Error e -> requestError (RIRequest e) next ctx

    let checkCode =
        safeBindJson
        <| fun (req: CodeCheckRequest) ->
            if isNull req.Kind || not (Set.contains req.Kind codeCheckKinds) then
                requestError (RIRequest "Field kind must be one of: action, trigger, module")
            else if isNull req.Source then
                requestError (RIRequest "Field source is required")
            else if Encoding.UTF8.GetByteCount req.Source > maxCodeCheckSourceSize then
                requestError (RIRequest(sprintf "Field source must not exceed %i bytes" maxCodeCheckSourceSize))
            else
                utils.PerformReadJob
                <| fun api ->
                    requireRoot api
                    <| fun () ->
                        task {
                            let! ret = api.Introspection.CheckCode req
                            return jobJson ret
                        }

    let dryRun =
        safeBindJson
        <| fun (req: DryRunHTTPRequest) ->
            let mode =
                match req.Mode with
                | None
                | Some "plan" -> Some DRPlan
                | Some "full" -> Some DRFull
                | _ -> None

            match mode with
            | None -> requestError (RIRequest "Field mode must be one of: plan, full")
            | Some DRFull when not settings.AllowFullDryRun ->
                utils.PerformWriteJob
                <| fun api -> requireRoot api (fun () -> Task.FromResult(jobError IEFullDryRunDisabled))
            | Some mode ->
                utils.PerformWriteJob
                <| fun api ->
                    requireRoot api
                    <| fun () ->
                        let transaction =
                            { Operations = req.Operations
                              DeferConstraints = req.DeferConstraints
                              ForceAllowBroken = req.ForceAllowBroken }

                        api.Request.IsDryRun <- true

                        // Nothing is committed: the request transaction is rolled back when the job ends.
                        runHTTPTransaction api transaction
                        <| fun ret ->
                            task {
                                let! validation = api.Request.Context.ValidateMigration mode

                                return
                                    jobJson
                                        { Ok = validation.Ok
                                          Errors = List.toArray validation.Errors
                                          Results = ret.Results
                                          Ddl = List.toArray validation.Ddl
                                          BrokenObjects = List.toArray validation.BrokenObjects }
                            }

    let runSql =
        safeBindJson
        <| fun (req: SqlHTTPRequest) ->
            utils.PerformReadJob
            <| fun api ->
                requireRoot api
                <| fun () ->
                    if not settings.AllowRawSql then
                        Task.FromResult(jobError IERawSqlDisabled)
                    else
                        match validateSqlRequest req with
                        | Error e -> Task.FromResult(jobError (RIRequest e))
                        | Ok req ->
                            task {
                                // Nothing is committed: the request transaction is rolled back when the job ends.
                                match! api.Introspection.RunSql req with
                                | Ok ret -> return jobJson ret
                                | Error RSEUnsafeDatabase -> return jobError IERawSqlUnavailable
                            }

    let pgReport (name: string) : HttpHandler =
        utils.PerformReadJob
        <| fun api ->
            requireRoot api
            <| fun () ->
                task {
                    match! api.Introspection.PgReport name with
                    | Some ret -> return jobJson ret
                    | None -> return jobError (IEReportNotFound name)
                }

    [ subRoute
          "/introspection"
          [ GET [ route "/capabilities" getCapabilities ]
            GET [ route "/schema" getSchemas ]
            GET [ routef "/schema/%s/%s" getEntity ]
            GET [ route "/dependencies" getDependencies ]
            GET [ route "/search" search ]
            POST [ route "/funql/validate" validateQuery ]
            POST [ route "/code/check" checkCode ]
            POST [ route "/dry_run" dryRun ]
            POST [ route "/sql" runSql ]
            GET [ routef "/pg/%s" pgReport ] ] ]
