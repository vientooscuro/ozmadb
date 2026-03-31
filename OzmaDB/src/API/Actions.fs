module OzmaDB.API.Actions

open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Newtonsoft.Json.Linq

open OzmaDB.OzmaQL.AST
open OzmaDB.Actions.Types
open OzmaDB.Actions.Run
open OzmaDB.Permissions.Source
open OzmaDB.Permissions.Types
open OzmaDB.API.Types
open OzmaDB.Exception

let private isActionPrivileged (actionRef: ActionRef) (role: ResolvedRole) : bool =
    role.PrivilegedActions
    |> Set.exists (function
        | SPAAll -> true
        | SPASchema schema -> schema = actionRef.Schema
        | SPAAction ref -> ref = actionRef)

type ActionsAPI(api: IOzmaDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context
    let logger = ctx.LoggerFactory.CreateLogger<ActionsAPI>()

    member this.RunAction(req: RunActionRequest) : Task<Result<ActionResponse, ActionErrorInfo>> =
        wrapAPIResult rctx "runAction" req
        <| fun () ->
            task {
                match ctx.FindAction(req.Action) with
                | None ->
                    let msg = sprintf "Action %O not found" req.Action
                    return Error <| AERequest msg
                | Some(Ok action) ->
                    try
                        let shouldElevate =
                            match rctx.User.Effective.Type with
                            | RTRoot -> false
                            | RTRole roleInfo ->
                                match roleInfo.Role with
                                | Some role -> isActionPrivileged req.Action role
                                | None -> false

                        let runAction () =
                            rctx.RunWithSource(ESAction req.Action)
                            <| fun () ->
                                task {
                                    let args = Option.defaultWith (fun () -> JObject()) req.Args
                                    let! res = action.Run(args, ctx.CancellationToken)
                                    return Ok { Result = res }
                                }

                        if shouldElevate then
                            let! result = rctx.PretendRole { AsRole = PRRoot } <| fun () -> runAction ()

                            return
                                match result with
                                | Ok r -> r
                                | Error _ -> failwith "Unexpected PretendRole error when elevating action"
                        else
                            return! runAction ()
                    with :? ActionRunException as e when e.IsUserException ->
                        logger.LogError(e, "Exception in action {action}", req.Action)
                        return Error(AEException(fullUserMessage e, e.UserData))
                | Some(Error e) ->
                    logger.LogError(e, "Requested action {action} is broken", req.Action)
                    let msg = sprintf "Requested action %O is broken: %s" req.Action (fullUserMessage e)
                    return Error <| AEOther msg
            }

    interface IActionsAPI with
        member this.RunAction req = this.RunAction req
