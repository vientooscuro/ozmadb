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
      Features = [| "validate"; "schema"; "dependencies" |] }

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

    [ subRoute
          "/introspection"
          [ GET [ route "/capabilities" getCapabilities ]
            GET [ route "/schema" getSchemas ]
            GET [ routef "/schema/%s/%s" getEntity ]
            GET [ route "/dependencies" getDependencies ]
            POST [ route "/funql/validate" validateQuery ] ] ]
