module OzmaDB.API.Introspection

open System.Text.RegularExpressions
open System.Threading.Tasks

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.UsedReferences
open OzmaDB.Layout.Types
open OzmaDB.UserViews.Types
open OzmaDB.UserViews.Resolve
open OzmaDB.API.Types

// Matches both position formats produced by `OzmaDB.Parsing.parse`.
let private parsePositionRegex =
    Regex(@"Error at (?:line (\d+), position (\d+)|position (\d+)\.(\d+))", RegexOptions.Compiled)

let private parsePosition (message: string) : int option * int option =
    let m = parsePositionRegex.Match message

    if not m.Success then
        (None, None)
    else if m.Groups.[1].Success then
        (Some(int m.Groups.[1].Value), Some(int m.Groups.[2].Value))
    else
        (Some(int m.Groups.[3].Value), Some(int m.Groups.[4].Value))

let private errorItem (kind: ValidateErrorKind) (e: exn) : ValidateErrorItem =
    let message = fullUserMessage e
    let (line, column) = parsePosition message

    { Message = message
      Kind = kind
      Line = line
      Column = column
      Path = None }

let private failedResult (error: ValidateErrorItem) : ValidateResult =
    { Ok = false
      Errors = [| error |]
      Columns = [||]
      Arguments = [||]
      Used = Map.empty
      Sql = None }

let private usedToMap (used: UsedDatabase) : Map<string, Map<string, string[]>> =
    let mapEntity (entity: UsedEntity) =
        entity.Fields |> Map.keys |> Seq.map string |> Seq.sort |> Seq.toArray

    let mapSchema (schema: UsedSchema) =
        schema.Entities
        |> Map.toSeq
        |> Seq.map (fun (name, entity) -> (string name, mapEntity entity))
        |> Map.ofSeq

    used.Schemas
    |> Map.toSeq
    |> Seq.map (fun (name, schema) -> (string name, mapSchema schema))
    |> Map.ofSeq

let private successResult (layout: Layout) (uv: ResolvedUserView) : ValidateResult =
    let getColumn (column: CompiledColumnInfo) =
        match column.Type with
        | CTColumn name ->
            Some
                { Name = string name
                  ValueType = column.Info.ValueType |> Option.map (fun t -> t.ToSQLString()) }
        | _ -> None

    let getArgument (ref: ArgumentRef, arg: ResolvedArgument) =
        match ref with
        | PLocal name ->
            Some
                { Name = string name
                  ArgType = arg.ArgType.ToOzmaQLString()
                  Optional = arg.Optional }
        | PGlobal _ -> None

    let used = selectExprUsedReferences layout uv.Resolved.Select

    { Ok = true
      Errors = [||]
      Columns = uv.Compiled.Columns |> Seq.mapMaybe getColumn |> Seq.toArray
      Arguments =
        uv.Resolved.Arguments
        |> OrderedMap.toSeq
        |> Seq.mapMaybe getArgument
        |> Seq.toArray
      Used = usedToMap used.UsedDatabase
      Sql = Some(uv.Compiled.Query.Expression.ToSQLString()) }

type IntrospectionAPI(api: IOzmaDBAPI) =
    let rctx = api.Request
    let ctx = rctx.Context

    // Resolves and compiles without the anonymous views cache or a dry run, so nothing touches the database.
    member this.ValidateQuery(req: ValidateQueryRequest) : Task<ValidateResult> =
        task {
            let findExistingView =
                ctx.UserViews.Find >> Option.map (Result.map (fun pref -> pref.UserView))

            try
                let uv =
                    resolveAnonymousUserView
                        ctx.Layout
                        true
                        ctx.DefaultAttrs
                        findExistingView
                        ctx.PgFunctions
                        req.HomeSchema
                        req.Query

                return successResult ctx.Layout uv
            with
            | :? UserViewResolveException as e when (e.InnerException :? QueryResolveException) ->
                return failedResult (errorItem VEKResolve e)
            | :? UserViewResolveException as e -> return failedResult (errorItem VEKParse e)
            | :? UserException as e -> return failedResult (errorItem VEKCompile e)
        }

    interface IIntrospectionAPI with
        member this.ValidateQuery req = this.ValidateQuery req
