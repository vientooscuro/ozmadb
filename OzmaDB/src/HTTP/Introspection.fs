module OzmaDB.HTTP.Introspection

open System
open System.Runtime.Serialization
open System.Threading.Tasks
open Giraffe
open Microsoft.AspNetCore.Http
open Giraffe.EndpointRouting
open Microsoft.Extensions.DependencyInjection

open OzmaDB.OzmaUtils.Serialization.Utils
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.API.Types
open OzmaDB.HTTP.Utils

// TODO: make it private after we fix JSON serialization for the private F# types.
[<SerializeAsObject("error")>]
type IntrospectionErrorInfo =
    | [<CaseKey("accessDenied")>] IEAccessDenied
    | [<CaseKey("entityNotFound")>] IEEntityNotFound of Entity: string
    | [<CaseKey("fieldNotFound")>] IEFieldNotFound of Field: string

    member this.LogMessage =
        match this with
        | IEAccessDenied -> "Introspection is available only to root"
        | IEEntityNotFound entity -> sprintf "Entity not found: %s" entity
        | IEFieldNotFound field -> sprintf "Field not found: %s" field

    [<DataMember>]
    member this.Message = this.LogMessage

    member this.ShouldLog = false
    member this.Details = Map.empty

    member this.HTTPResponseCode =
        match this with
        | IEAccessDenied -> 403
        | IEEntityNotFound _ -> 404
        | IEFieldNotFound _ -> 404

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

let private capabilities =
    { Version = 1
      Features = [| "validate"; "schema"; "dependencies"; "search" |] }

let private requireRoot (api: IOzmaDBAPI) (f: unit -> Task<HttpJobResponse>) : Task<HttpJobResponse> =
    match api.Request.User.Effective.Type with
    | RTRoot -> f ()
    | RTRole _ -> Task.FromResult(jobError IEAccessDenied)

let introspectionApi (serviceProvider: IServiceProvider) : Endpoint list =
    let utils = serviceProvider.GetRequiredService<HttpJobUtils>()

    let getCapabilities =
        utils.PerformReadJob
        <| fun api -> requireRoot api (fun () -> Task.FromResult(jobJson capabilities))

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

    [ subRoute
          "/introspection"
          [ GET [ route "/capabilities" getCapabilities ]
            GET [ route "/schema" getSchemas ]
            GET [ routef "/schema/%s/%s" getEntity ]
            GET [ route "/dependencies" getDependencies ]
            GET [ route "/search" search ]
            POST [ route "/funql/validate" validateQuery ] ] ]
