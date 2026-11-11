module OzmaDB.HTTP.Introspection

open System
open System.Runtime.Serialization
open System.Threading.Tasks
open Giraffe.EndpointRouting
open Microsoft.Extensions.DependencyInjection

open OzmaDB.OzmaUtils.Serialization.Utils
open OzmaDB.Exception
open OzmaDB.API.Types
open OzmaDB.HTTP.Utils

// TODO: make it private after we fix JSON serialization for the private F# types.
[<SerializeAsObject("error")>]
type IntrospectionErrorInfo =
    | [<CaseKey("accessDenied")>] IEAccessDenied

    member this.LogMessage =
        match this with
        | IEAccessDenied -> "Introspection is available only to root"

    [<DataMember>]
    member this.Message = this.LogMessage

    member this.ShouldLog = false
    member this.Details = Map.empty

    member this.HTTPResponseCode =
        match this with
        | IEAccessDenied -> 403

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
      Features = [| "validate" |] }

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

    [ subRoute
          "/introspection"
          [ GET [ route "/capabilities" getCapabilities ]
            POST [ route "/funql/validate" validateQuery ] ] ]
