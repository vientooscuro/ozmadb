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

let private entityRefString (ref: ResolvedEntityRef) =
    sprintf "%s.%s" (string ref.Schema) (string ref.Name)

// The stock printer quotes every name, so references are rendered by hand.
let private scalarTypeString (t: ScalarFieldType<ResolvedEntityRef>) =
    match t with
    | SFTReference(target, onDelete) ->
        let onDeleteStr =
            match onDelete with
            | None -> ""
            | Some action -> sprintf " ON DELETE %s" (action.ToOzmaQLString())

        sprintf "reference(%s)%s" (entityRefString target) onDeleteStr
    | _ -> t.ToOzmaQLString()

let private fieldTypeString (t: ResolvedFieldType) =
    match t with
    | FTScalar s -> scalarTypeString s
    | FTArray s -> sprintf "array(%s)" (scalarTypeString s)

let private entityDetails (ref: ResolvedEntityRef) (entity: ResolvedEntity) : EntityDetails =
    let sortedNames (names: seq<string>) = names |> Seq.sort |> Seq.toArray

    let columnField (name: FieldName, field: ResolvedColumnField) =
        { Name = string name
          Type = fieldTypeString field.FieldType
          Default = field.DefaultValue |> Option.map (fun v -> v.ToOzmaQLString())
          IsNullable = field.IsNullable
          IsImmutable = field.IsImmutable
          InheritedFrom = field.InheritedFrom |> Option.map entityRefString }

    let computedField (name: FieldName, field: ResolvedComputedField) =
        { Name = string name
          Expression = field.Expression.ToOzmaQLString()
          IsVirtual = Option.isSome field.Virtual
          IsMaterialized = field.IsMaterialized
          InheritedFrom = field.InheritedFrom |> Option.map entityRefString }

    let referenceTarget (field: ResolvedColumnField) =
        match field.FieldType with
        | FTScalar(SFTReference(target, _))
        | FTArray(SFTReference(target, _)) -> Some(entityRefString target)
        | _ -> None

    { Name = string ref.Name
      Parent = entity.Parent |> Option.map entityRefString
      Children = entity.Children |> Map.keys |> Seq.map entityRefString |> sortedNames
      MainField = string entity.MainField
      ColumnFields = entity.ColumnFields |> Map.toSeq |> Seq.map columnField |> Seq.toArray
      ComputedFields =
        entity.ComputedFields
        |> Map.toSeq
        |> Seq.mapMaybe (fun (name, field) -> Result.getOption field |> Option.map (fun f -> computedField (name, f)))
        |> Seq.toArray
      CheckConstraints =
        entity.CheckConstraints
        |> Map.toSeq
        |> Seq.map (fun (name, c) ->
            { Name = string name
              Expression = c.Expression.ToOzmaQLString() })
        |> Seq.toArray
      UniqueConstraints =
        entity.UniqueConstraints
        |> Map.toSeq
        |> Seq.map (fun (name, c) ->
            { Name = string name
              Columns = c.Columns |> Array.map string })
        |> Seq.toArray
      Indexes =
        entity.Indexes
        |> Map.toSeq
        |> Seq.map (fun (name, i) ->
            { Name = string name
              Expressions = i.Expressions |> Array.map (fun e -> e.ToOzmaQLString())
              IsUnique = i.IsUnique })
        |> Seq.toArray
      References =
        entity.ColumnFields
        |> Map.toSeq
        |> Seq.mapMaybe (fun (name, field) ->
            referenceTarget field
            |> Option.map (fun target -> { Field = string name; Target = target }))
        |> Seq.toArray }

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

    member this.GetSchemas() : Task<SchemaSummary[]> =
        task {
            return
                ctx.Layout.Schemas
                |> Map.toSeq
                |> Seq.map (fun (schemaName, schema) ->
                    { Schema = string schemaName
                      Entities =
                        schema.Entities
                        |> Map.toSeq
                        |> Seq.map (fun (name, entity) ->
                            { Name = string name
                              Parent = entity.Parent |> Option.map entityRefString
                              IsAbstract = entity.IsAbstract
                              IsFrozen = entity.IsFrozen })
                        |> Seq.sortBy (fun e -> e.Name)
                        |> Seq.toArray })
                |> Seq.sortBy (fun s -> s.Schema)
                |> Seq.toArray
        }

    member this.GetEntity(ref: ResolvedEntityRef) : Task<EntityDetails option> =
        task { return ctx.Layout.FindEntity ref |> Option.map (entityDetails ref) }

    interface IIntrospectionAPI with
        member this.ValidateQuery req = this.ValidateQuery req
        member this.GetSchemas() = this.GetSchemas()
        member this.GetEntity ref = this.GetEntity ref
