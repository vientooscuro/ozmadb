module OzmaDB.API.Introspection

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Data
open System.IO
open System.Text
open System.Text.RegularExpressions
open System.Threading.Tasks
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open Npgsql
open NpgsqlTypes

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.Objects.Types
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.UsedReferences
open OzmaDB.OzmaQL.Optimize
open OzmaDB.Layout.Source
open OzmaDB.Layout.Types
open OzmaDB.UserViews.Types
open OzmaDB.UserViews.Resolve
open OzmaDB.JavaScript.Runtime
open OzmaDB.API.Types

type private MutableHashSet<'a> = System.Collections.Generic.HashSet<'a>

module PermissionsTypes = OzmaDB.Permissions.Types
module DryRun = OzmaDB.UserViews.DryRun
module ActionsSchema = OzmaDB.Actions.Schema
module TriggersSchema = OzmaDB.Triggers.Schema
module ModulesSchema = OzmaDB.Modules.Schema
module LayoutSchema = OzmaDB.Layout.Schema
module UserViewsSchema = OzmaDB.UserViews.Schema
module PermissionsSchema = OzmaDB.Permissions.Schema

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

// Stored (unresolved) layout. Every state version change produces a new layout, so it is loaded once per layout.
let private sourceLayoutCache = ConditionalWeakTable<Layout, SourceLayout>()

let private getSourceLayout (ctx: IContext) : Task<SourceLayout> =
    task {
        match sourceLayoutCache.TryGetValue ctx.Layout with
        | (true, source) -> return source
        | (false, _) ->
            let! source = LayoutSchema.buildSchemaLayout ctx.Transaction.System None ctx.CancellationToken
            sourceLayoutCache.AddOrUpdate(ctx.Layout, source)
            return source
    }

// Finds a stored object in the entity or the ancestor that declares it.
let rec private findStored
    (source: SourceLayout)
    (ref: ResolvedEntityRef)
    (get: SourceEntity -> 'a option)
    : (ResolvedEntityRef * 'a) option =
    match source.FindEntity ref with
    | None -> None
    | Some entity ->
        match get entity with
        | Some value -> Some(ref, value)
        | None -> entity.Parent |> Option.bind (fun parent -> findStored source parent get)

let private entityDetails (source: SourceLayout) (ref: ResolvedEntityRef) (entity: ResolvedEntity) : EntityDetails =
    let sortedNames (names: seq<string>) = names |> Seq.sort |> Seq.toArray

    let columnField (name: FieldName, field: ResolvedColumnField) =
        { Name = string name
          Type = fieldTypeString field.FieldType
          Default = field.DefaultValue |> Option.map (fun v -> v.ToOzmaQLString())
          IsNullable = field.IsNullable
          IsImmutable = field.IsImmutable
          InheritedFrom = field.InheritedFrom |> Option.map entityRefString }

    let computedField (name: FieldName, field: PossiblyBroken<ResolvedComputedField>) =
        let stored = findStored source ref (fun e -> Map.tryFind name e.ComputedFields)

        match field with
        | Ok field ->
            let resolved = field.Expression.ToOzmaQLString()

            { Name = string name
              Expression =
                stored
                |> Option.map (fun (_, f) -> f.Expression)
                |> Option.defaultValue resolved
              IsVirtual = Option.isSome field.Virtual
              IsMaterialized = field.IsMaterialized
              InheritedFrom = field.InheritedFrom |> Option.map entityRefString
              IsBroken = false
              Error = None
              ResolvedExpression = Some resolved }
        | Error broken ->
            { Name = string name
              Expression = stored |> Option.map (fun (_, f) -> f.Expression) |> Option.defaultValue ""
              IsVirtual = stored |> Option.exists (fun (_, f) -> f.IsVirtual)
              IsMaterialized = stored |> Option.exists (fun (_, f) -> f.IsMaterialized)
              InheritedFrom =
                stored
                |> Option.map fst
                |> Option.filter (fun declaring -> declaring <> ref)
                |> Option.map entityRefString
              IsBroken = true
              Error = Some(fullUserMessage broken.Error)
              ResolvedExpression = None }

    let checkConstraint (name: ConstraintName, constr: ResolvedCheckConstraint) =
        let resolved = constr.Expression.ToOzmaQLString()

        { Name = string name
          Expression =
            findStored source ref (fun e -> Map.tryFind name e.CheckConstraints)
            |> Option.map (fun (_, c) -> c.Expression)
            |> Option.defaultValue resolved
          ResolvedExpression = resolved }

    let referenceTarget (field: ResolvedColumnField) =
        match field.FieldType with
        | FTScalar(SFTReference(target, _))
        | FTArray(SFTReference(target, _)) -> Some(entityRefString target)
        | _ -> None

    { Name = string ref.Name
      Parent = entity.Parent |> Option.map entityRefString
      Children =
        entity.Children
        |> Map.toSeq
        |> Seq.filter (fun (_, c) -> c.Direct)
        |> Seq.map (fst >> entityRefString)
        |> sortedNames
      MainField = string entity.MainField
      ColumnFields = entity.ColumnFields |> Map.toSeq |> Seq.map columnField |> Seq.toArray
      ComputedFields = entity.ComputedFields |> Map.toSeq |> Seq.map computedField |> Seq.toArray
      CheckConstraints = entity.CheckConstraints |> Map.toSeq |> Seq.map checkConstraint |> Seq.toArray
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

[<NoEquality; NoComparison>]
type private JSSource =
    { Item: DependencyItem; Source: string }

// Built once per layout and never mutated afterwards.
[<NoEquality; NoComparison>]
type private DependencyIndex =
    { Fields: Dictionary<ResolvedFieldRef, MutableHashSet<DependencyItem>>
      Entities: Dictionary<ResolvedEntityRef, MutableHashSet<DependencyItem>>
      JSSources: JSSource[] }

// Every state version change produces a new layout, so the layout instance identifies the indexed state.
let private dependencyIndexes = ConditionalWeakTable<Layout, DependencyIndex>()

let private restrictionKindString =
    function
    | PermissionsTypes.RKSelect -> "select"
    | PermissionsTypes.RKUpdate -> "update"
    | PermissionsTypes.RKDelete -> "delete"
    | PermissionsTypes.RKCheck -> "check"

// Field references through a child entity are attributed to the entity that declares the field.
let private declaringEntity (layout: Layout) (entityRef: ResolvedEntityRef) (fieldName: FieldName) =
    match layout.FindEntity entityRef with
    | None -> entityRef
    | Some entity when fieldName = funId || fieldName = funSubEntity -> entity.Root
    | Some entity ->
        match Map.tryFind fieldName entity.ColumnFields with
        | Some col -> Option.defaultValue entityRef col.InheritedFrom
        | None ->
            match Map.tryFind fieldName entity.ComputedFields with
            | Some(Ok comp) -> Option.defaultValue entityRef comp.InheritedFrom
            | _ -> entityRef

let private resolvedItem (kind: string) (schema: SchemaName) (name: string) (entity: ResolvedEntityRef option) =
    { Kind = kind
      Schema = string schema
      Name = name
      Entity = entity |> Option.map entityRefString
      Detail = None
      Confidence = "resolved" }

let private buildDependencyIndex
    (layout: Layout)
    (userViews: DryRun.PrefetchedUserViews)
    (permissions: PermissionsTypes.Permissions)
    (jsSources: JSSource[])
    : DependencyIndex =
    let fields = Dictionary<ResolvedFieldRef, MutableHashSet<DependencyItem>>()
    let entities = Dictionary<ResolvedEntityRef, MutableHashSet<DependencyItem>>()

    let addTo (dict: Dictionary<'k, MutableHashSet<DependencyItem>>) (key: 'k) (item: DependencyItem) =
        match dict.TryGetValue key with
        | (true, items) -> ignore <| items.Add item
        | (false, _) ->
            let items = MutableHashSet<DependencyItem>()
            ignore <| items.Add item
            dict.[key] <- items

    let addField (entityRef: ResolvedEntityRef) (fieldName: FieldName) (item: DependencyItem) =
        let declRef = declaringEntity layout entityRef fieldName
        addTo fields { Entity = declRef; Name = fieldName } item
        addTo entities declRef item

    let addUsed (item: DependencyItem) (used: UsedDatabase) =
        for KeyValue(schemaName, usedSchema) in used.Schemas do
            for KeyValue(entityName, usedEntity) in usedSchema.Entities do
                let entityRef =
                    { Schema = schemaName
                      Name = entityName }

                addTo entities entityRef item

                for KeyValue(fieldName, _) in usedEntity.Fields do
                    addField entityRef fieldName item

    for KeyValue(schemaName, schema) in layout.Schemas do
        for KeyValue(entityName, entity) in schema.Entities do
            let entityRef =
                { Schema = schemaName
                  Name = entityName }

            let item kind name =
                resolvedItem kind schemaName name (Some entityRef)

            for KeyValue(fieldName, field) in entity.ComputedFields do
                match field with
                | Ok comp when Option.isNone comp.InheritedFrom ->
                    addUsed (item "computed_field" (string fieldName)) comp.UsedDatabase
                | _ -> ()

            for KeyValue(constrName, constr) in entity.CheckConstraints do
                addUsed (item "check_constraint" (string constrName)) constr.UsedDatabase

            for KeyValue(indexName, index) in entity.Indexes do
                let exprs =
                    Seq.concat
                        [ index.Expressions |> Seq.map (fun col -> col.Expr)
                          Seq.ofArray index.IncludedExpressions
                          Option.toSeq index.Predicate ]

                let indexItem = item "index" (string indexName)

                for expr in exprs do
                    addUsed indexItem (fieldExprUsedReferences layout expr).UsedDatabase

            if entity.MainField <> funId then
                addField entityRef entity.MainField (item "main_field" (string entityName))

    for KeyValue(schemaName, viewsSchema) in userViews.Schemas do
        match viewsSchema with
        | Error _ -> ()
        | Ok viewsSchema ->
            for KeyValue(viewName, view) in viewsSchema.UserViews do
                match view with
                | Error _ -> ()
                | Ok pref ->
                    let used = selectExprUsedReferences layout pref.UserView.Resolved.Select
                    addUsed (resolvedItem "user_view" schemaName (string viewName) None) used.UsedDatabase

    for KeyValue(schemaName, permissionsSchema) in permissions.Schemas do
        for KeyValue(roleName, role) in permissionsSchema.Roles do
            match role with
            | Error _ -> ()
            | Ok role ->
                for KeyValue(allowedSchemaName, allowedSchema) in role.Permissions.Schemas do
                    for KeyValue(allowedEntityName, allowedEntity) in allowedSchema.Entities do
                        match allowedEntity with
                        | Error _ -> ()
                        | Ok allowedEntity ->
                            let entityRef =
                                { Schema = allowedSchemaName
                                  Name = allowedEntityName }

                            for KeyValue(kind, used) in allowedEntity.UsedDatabases do
                                let item =
                                    { resolvedItem "role_rule" schemaName (string roleName) (Some entityRef) with
                                        Detail = Some(restrictionKindString kind) }

                                addUsed item used

    { Fields = fields
      Entities = entities
      JSSources = jsSources }

let private loadJSSources (ctx: IContext) : Task<JSSource[]> =
    task {
        let system = ctx.Transaction.System
        let! actions = ActionsSchema.buildSchemaActions system None ctx.CancellationToken
        let! triggers = TriggersSchema.buildSchemaTriggers system None ctx.CancellationToken
        let! modules = ModulesSchema.buildSchemaModules system None ctx.CancellationToken

        let source kind (schemaName: SchemaName) name entity text =
            { Item =
                { Kind = kind
                  Schema = string schemaName
                  Name = name
                  Entity = entity
                  Detail = None
                  Confidence = "text" }
              Source = text }

        let actionSources =
            seq {
                for KeyValue(schemaName, schema) in actions.Schemas do
                    for KeyValue(name, action) in schema.Actions do
                        yield source "action" schemaName (string name) None action.Function
            }

        let triggerSources =
            seq {
                for KeyValue(schemaName, triggersDb) in triggers.Schemas do
                    for KeyValue(entitySchemaName, triggersSchema) in triggersDb.Schemas do
                        for KeyValue(entityName, triggersEntity) in triggersSchema.Entities do
                            let entityRef =
                                { Schema = entitySchemaName
                                  Name = entityName }

                            for KeyValue(name, trigger) in triggersEntity.Triggers do
                                yield
                                    source
                                        "trigger"
                                        schemaName
                                        (string name)
                                        (Some(entityRefString entityRef))
                                        trigger.Procedure
            }

        let moduleSources =
            seq {
                for KeyValue(schemaName, schema) in modules.Schemas do
                    for KeyValue(path, modul) in schema.Modules do
                        yield source "module" schemaName path None modul.Source
            }

        return Seq.concat [ actionSources; triggerSources; moduleSources ] |> Seq.toArray
    }

let private jsMatches (sources: JSSource[]) (word: string) : DependencyItem seq =
    let regex = Regex(@"\b" + Regex.Escape word + @"\b")

    sources
    |> Seq.filter (fun s -> s.Source.Contains(word, StringComparison.Ordinal))
    |> Seq.collect (fun s ->
        s.Source.Split('\n')
        |> Seq.indexed
        |> Seq.filter (fun (_, line) -> regex.IsMatch line)
        |> Seq.map (fun (i, _) ->
            { s.Item with
                Detail = Some(sprintf "line %i" (i + 1)) }))

let private dependencyLine (item: DependencyItem) =
    match item.Detail with
    | Some detail when item.Confidence = "text" -> int (detail.Substring("line ".Length))
    | _ -> 0

let private sortDependencies (items: DependencyItem seq) : DependencyItem[] =
    items
    |> Set.ofSeq
    |> Seq.sortBy (fun i -> (i.Kind, i.Schema, i.Name, dependencyLine i, i.Detail, i.Entity))
    |> Seq.toArray

let private searchSourcesCache = ConditionalWeakTable<Layout, JSSource[]>()

// Stored source texts of metadata objects, so match positions can be used to patch them.
let private loadMetadataSources (ctx: IContext) : Task<JSSource[]> =
    task {
        let system = ctx.Transaction.System
        let! layout = getSourceLayout ctx
        let! userViews = UserViewsSchema.buildSchemaUserViews system None ctx.CancellationToken
        let! permissions = PermissionsSchema.buildSchemaPermissions system None ctx.CancellationToken

        let source kind (schemaName: SchemaName) name entity part text =
            { Item =
                { Kind = kind
                  Schema = string schemaName
                  Name = name
                  Entity = entity
                  Detail = part
                  Confidence = "resolved" }
              Source = text }

        let layoutSources =
            seq {
                for KeyValue(schemaName, schema) in layout.Schemas do
                    for KeyValue(entityName, entity) in schema.Entities do
                        let entityStr =
                            Some(
                                entityRefString
                                    { Schema = schemaName
                                      Name = entityName }
                            )

                        for KeyValue(fieldName, field) in entity.ComputedFields do
                            yield
                                source
                                    "computed_field"
                                    schemaName
                                    (sprintf "%s.%s" (string entityName) (string fieldName))
                                    entityStr
                                    None
                                    field.Expression

                        for KeyValue(constrName, constr) in entity.CheckConstraints do
                            yield
                                source
                                    "check_constraint"
                                    schemaName
                                    (string constrName)
                                    entityStr
                                    None
                                    constr.Expression
            }

        let viewSources =
            seq {
                for KeyValue(schemaName, schema) in userViews.Schemas do
                    for KeyValue(viewName, view) in schema.UserViews do
                        yield source "user_view" schemaName (string viewName) None None view.Query
            }

        let ruleSources =
            seq {
                for KeyValue(schemaName, schema) in permissions.Schemas do
                    for KeyValue(roleName, role) in schema.Roles do
                        for KeyValue(allowedSchemaName, allowedSchema) in role.Permissions.Schemas do
                            for KeyValue(allowedEntityName, allowed) in allowedSchema.Entities do
                                let entityStr =
                                    Some(
                                        entityRefString
                                            { Schema = allowedSchemaName
                                              Name = allowedEntityName }
                                    )

                                let parts =
                                    [ ("select", allowed.Select)
                                      ("update", allowed.Update)
                                      ("delete", allowed.Delete)
                                      ("check", allowed.Check) ]

                                for (part, text) in parts do
                                    match text with
                                    | Some text ->
                                        yield source "role_rule" schemaName (string roleName) entityStr (Some part) text
                                    | None -> ()
            }

        return Seq.concat [ layoutSources; viewSources; ruleSources ] |> Seq.toArray
    }

let private maxExcerptLength = 240
let private regexTimeout = TimeSpan.FromMilliseconds 500.0

exception private SearchTimeout

let private lineStarts (text: string) : int[] =
    seq {
        yield 0

        for i in 0 .. text.Length - 1 do
            if text.[i] = '\n' then
                yield i + 1
    }
    |> Seq.toArray

// Returns (index, length) of every occurrence.
let private findOccurrences (regex: Regex option) (query: string) (comparison: StringComparison) (text: string) =
    match regex with
    | Some regex ->
        try
            regex.Matches text
            |> Seq.filter (fun m -> m.Length > 0)
            |> Seq.map (fun m -> (m.Index, m.Length))
            |> Seq.toArray
        with :? RegexMatchTimeoutException ->
            raise SearchTimeout
    | None ->
        let result = List<int * int>()
        let mutable idx = text.IndexOf(query, comparison)

        while idx >= 0 do
            result.Add((idx, query.Length))
            idx <- text.IndexOf(query, idx + query.Length, comparison)

        result.ToArray()

let private sourceMatches (regex: Regex option) (req: SearchRequest) (src: JSSource) : SearchMatch[] =
    let comparison =
        if req.CaseSensitive then
            StringComparison.Ordinal
        else
            StringComparison.OrdinalIgnoreCase

    let occurrences = findOccurrences regex req.Query comparison src.Source

    if Array.isEmpty occurrences then
        [||]
    else
        let text = src.Source
        let starts = lineStarts text

        occurrences
        |> Array.map (fun (index, _) ->
            let lineIdx =
                let pos = Array.BinarySearch(starts, index)
                if pos >= 0 then pos else (~~~pos) - 1

            let lineStart = starts.[lineIdx]

            let lineEnd =
                if lineIdx + 1 < starts.Length then
                    starts.[lineIdx + 1] - 1
                else
                    text.Length

            let line = text.Substring(lineStart, lineEnd - lineStart).TrimEnd('\r')
            let col = index - lineStart

            // Long lines are windowed around the match; the column still refers to the full stored line.
            let excerpt =
                if line.Length <= maxExcerptLength then
                    line
                else
                    let start =
                        max 0 (min (col - maxExcerptLength / 4) (line.Length - maxExcerptLength))

                    line.Substring(start, maxExcerptLength)

            { Kind = src.Item.Kind
              Schema = src.Item.Schema
              Name = src.Item.Name
              Entity = src.Item.Entity
              Part =
                (if src.Item.Kind = "role_rule" then
                     src.Item.Detail
                 else
                     None)
              Line = lineIdx + 1
              Column = col + 1
              Excerpt = excerpt })

let private sqlWrapperPrefix = "SELECT * FROM ("

// Text read from PostgreSQL, in characters: per value and per response.
let private maxSqlValueLength = 1024 * 1024
let private maxSqlResultLength = 16 * 1024 * 1024
// Charged per cell, NULLs included, so that many tiny cells still count toward the response limit.
let private sqlCellCost = 16
// Larger json/jsonb values are returned as strings instead of being parsed into a token tree.
let private maxParsedJsonLength = 64 * 1024
let private truncatedValueMarker = "…[truncated]"

// Membership, not just inheritance: `set_config('role', ...)` can switch to any role the user is a member of.
let private unsafeDatabaseQuery =
    """SELECT EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname IN ('dblink', 'postgres_fdw'))
OR EXISTS (
  SELECT FROM pg_catalog.pg_roles r
  WHERE (r.rolsuper OR r.rolreplication OR r.rolbypassrls
         OR r.rolname IN ('pg_read_server_files', 'pg_write_server_files', 'pg_execute_server_program',
                          'pg_signal_backend'))
    AND pg_catalog.pg_has_role(current_user, r.oid, 'MEMBER'))"""

let private pgReportTimeoutMs = 10000
let private pgReportMaxRows = 5000

let private tableSizesReport =
    """SELECT n.nspname AS schema, c.relname AS "table", pg_total_relation_size(c.oid) AS total_bytes, pg_table_size(c.oid) AS table_bytes, pg_indexes_size(c.oid) AS index_bytes
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_toast%' AND n.nspname NOT LIKE 'pg\_temp\_%'
ORDER BY total_bytes DESC, 1, 2
LIMIT 200"""

// Only sessions of this database: cluster-wide locks (virtualxid, transactionid) would expose other databases' queries.
let private locksReport =
    """SELECT l.pid, a.datname, l.locktype, l.mode, l.granted, l.relation::regclass::text AS relation, a.query, a.state, a.wait_event
FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
WHERE a.datname = current_database()
ORDER BY l.granted, l.pid
LIMIT 1000"""

let private activityReport =
    """SELECT pid, usename, state, query_start, wait_event_type, wait_event, left(query, 1000) AS query
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY query_start NULLS LAST
LIMIT 1000"""

let private statementsReport =
    """SELECT query, calls, total_exec_time, mean_exec_time, rows
FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
ORDER BY total_exec_time DESC
LIMIT 50"""

let private indexesUnusedReport =
    """SELECT s.schemaname AS schema, s.relname AS "table", s.indexrelname AS index, pg_relation_size(s.indexrelid) AS index_bytes
FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.idx_scan = 0 AND NOT i.indisunique
ORDER BY index_bytes DESC, 1, 2, 3
LIMIT 200"""

let private pgReports =
    Map.ofList
        [ ("table_sizes", tableSizesReport)
          ("locks", locksReport)
          ("activity", activityReport)
          ("statements", statementsReport)
          ("indexes_unused", indexesUnusedReport) ]

let private sqlErrorKind (sqlState: string) =
    match sqlState with
    | "57014" -> VEKTimeout // query_canceled, raised by statement_timeout
    | "25006" -> VEKReadOnly // read_only_sql_transaction
    | "42601" -> VEKSyntax // syntax_error
    | "55P03" -> VEKLockTimeout // lock_not_available
    | _ -> VEKSql

let private sqlFailure (kind: ValidateErrorKind) (message: string) (position: (int * int) option) : SqlQueryResult =
    { Ok = false
      Errors =
        [| { Message = message
             Kind = kind
             Line = position |> Option.map fst
             Column = position |> Option.map snd
             Path = None } |]
      Columns = [||]
      Rows = [||]
      Truncated = false }

// Maps a 1-based character position in the wrapped query back to a line and column of the user query.
let private userSqlPosition (query: string) (position: int) : (int * int) option =
    let offset = position - sqlWrapperPrefix.Length - 1

    if position <= 0 || offset < 0 || offset > query.Length then
        None
    else
        let before = query.Substring(0, offset)
        let line = before |> Seq.filter (fun c -> c = '\n') |> Seq.length
        let column = offset - (before.LastIndexOf('\n') + 1)
        Some(line + 1, column + 1)

let private jsonNumberRegex =
    Regex(@"^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$", RegexOptions.Compiled)

let private timestampTzRegex =
    Regex(@"^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d(?:\.\d+)?)([+-]\d\d)(?::(\d\d))?$", RegexOptions.Compiled)

let private timestampRegex =
    Regex(@"^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d(?:\.\d+)?)$", RegexOptions.Compiled)

let private parseJsonValue (text: string) : JToken =
    use reader = new JsonTextReader(new StringReader(text))
    reader.DateParseHandling <- DateParseHandling.None
    reader.FloatParseHandling <- FloatParseHandling.Decimal
    reader.MaxDepth <- Nullable(64)
    JToken.ReadFrom reader

// Values are read in PostgreSQL text format (with `DateStyle = ISO`), so every type is readable.
let private sqlTextValue (pgType: string) (text: string) : JToken =
    let fallback () = JValue(text) :> JToken

    match pgType with
    | "boolean" -> JValue((text = "t")) :> JToken
    | "smallint"
    | "integer"
    | "bigint"
    | "oid"
    | "numeric"
    | "real"
    | "double precision" when jsonNumberRegex.IsMatch text -> JRaw(text) :> JToken
    | "json"
    | "jsonb" ->
        try
            parseJsonValue text
        with :? JsonException ->
            fallback ()
    | "bytea" when text.StartsWith(@"\x") ->
        JValue(Convert.ToBase64String(Convert.FromHexString(text.Substring 2))) :> JToken
    | "timestamp with time zone" ->
        let m = timestampTzRegex.Match text

        if m.Success then
            let minutes = if m.Groups.[4].Success then m.Groups.[4].Value else "00"
            JValue(sprintf "%sT%s%s:%s" m.Groups.[1].Value m.Groups.[2].Value m.Groups.[3].Value minutes) :> JToken
        else
            fallback ()
    | "timestamp without time zone" ->
        let m = timestampRegex.Match text

        if m.Success then
            JValue(sprintf "%sT%s" m.Groups.[1].Value m.Groups.[2].Value) :> JToken
        else
            fallback ()
    | _ -> fallback ()

let private npgsqlParam (param: SqlParam) : NpgsqlParameter =
    match param with
    | SPString s -> NpgsqlParameter(Value = s, NpgsqlDbType = NpgsqlDbType.Text)
    | SPInt i -> NpgsqlParameter(Value = i, NpgsqlDbType = NpgsqlDbType.Bigint)
    | SPDecimal d -> NpgsqlParameter(Value = d, NpgsqlDbType = NpgsqlDbType.Numeric)
    | SPBool b -> NpgsqlParameter(Value = b, NpgsqlDbType = NpgsqlDbType.Boolean)
    | SPNull -> NpgsqlParameter(Value = DBNull.Value)

// Makes the request transaction read-only. Nothing is ever committed, so all `SET LOCAL`s are discarded with it.
let private enterReadOnly (ctx: IContext) (timeoutMs: int) : Task =
    task {
        let query =
            sprintf
                "SET TRANSACTION READ ONLY; SET LOCAL statement_timeout = '%ims'; SET LOCAL lock_timeout = '1s'; SET LOCAL DateStyle = 'ISO'; SET LOCAL bytea_output = 'hex'"
                timeoutMs

        let! _ = ctx.Transaction.Connection.Query.ExecuteNonQuery query Map.empty ctx.CancellationToken
        ()
    }

// Must run after `enterReadOnly`. A PostgreSQL error aborts the transaction, so nothing may run after it.
let private runWrappedQuery (ctx: IContext) (query: string) (pars: SqlParam[]) (maxRows: int) : Task<SqlQueryResult> =
    task {
        // At least one parameter (the limit) is always bound, so Npgsql sends the text as is in a single
        // extended-protocol Parse message instead of splitting it on semicolons, and PostgreSQL rejects
        // multiple statements. The newline stops a trailing line comment from swallowing the wrapper.
        let commandText =
            sprintf "%s%s\n) AS __q LIMIT $%i" sqlWrapperPrefix query (Array.length pars + 1)

        use command = new NpgsqlCommand(commandText, ctx.Transaction.Connection.Connection)
        let buffer = Array.zeroCreate<char> 8192
        command.AllResultTypesAreUnknown <- true

        for par in pars do
            ignore <| command.Parameters.Add(npgsqlParam par)

        ignore
        <| command.Parameters.Add(NpgsqlParameter(Value = int64 maxRows + 1L, NpgsqlDbType = NpgsqlDbType.Bigint))

        try
            // Sequential access streams values instead of buffering whole rows, so huge values are never held in memory.
            use! reader = command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ctx.CancellationToken)

            let columns =
                Array.init reader.FieldCount (fun i ->
                    { Name = reader.GetName i
                      PgType = reader.GetDataTypeName i
                      JsonAsText = None })

            let isJsonColumn =
                columns |> Array.map (fun c -> c.PgType = "json" || c.PgType = "jsonb")

            let jsonAsText = Array.create columns.Length false

            // Reads at most `maxSqlValueLength + 1` characters; the rest of the value is skipped by the reader.
            let readText (i: int) : Task<string * bool> =
                task {
                    use textReader = reader.GetTextReader i
                    let text = StringBuilder()
                    let mutable finished = false

                    while not finished do
                        let toRead = min buffer.Length (maxSqlValueLength + 1 - text.Length)
                        let! read = textReader.ReadBlockAsync(buffer, 0, toRead)
                        ignore <| text.Append(buffer, 0, read)
                        finished <- read < toRead || text.Length > maxSqlValueLength

                    if text.Length > maxSqlValueLength then
                        return (text.ToString(0, maxSqlValueLength), true)
                    else
                        return (text.ToString(), false)
                }

            let rows = List<JToken[]>()
            let mutable totalLength = 0L
            let mutable truncated = false
            let mutable tooLarge = false
            let mutable reading = true

            while reading do
                let! hasRow = reader.ReadAsync(ctx.CancellationToken)

                if not hasRow then
                    reading <- false
                else if rows.Count >= maxRows then
                    truncated <- true
                    reading <- false
                else
                    let row = Array.zeroCreate<JToken> columns.Length
                    let mutable i = 0

                    while not tooLarge && i < columns.Length do
                        let! isNull = reader.IsDBNullAsync(i, ctx.CancellationToken)
                        totalLength <- totalLength + int64 sqlCellCost

                        if isNull then
                            row.[i] <- JValue.CreateNull()
                        else
                            let! (text, cut) = readText i
                            totalLength <- totalLength + int64 text.Length

                            if isJsonColumn.[i] && (cut || text.Length > maxParsedJsonLength) then
                                jsonAsText.[i] <- true

                            row.[i] <-
                                if cut then
                                    JValue(text + truncatedValueMarker) :> JToken
                                else if isJsonColumn.[i] && text.Length > maxParsedJsonLength then
                                    JValue(text) :> JToken
                                else
                                    sqlTextValue columns.[i].PgType text

                        tooLarge <- totalLength > int64 maxSqlResultLength
                        i <- i + 1

                    // A row crossing the response limit is dropped as a whole.
                    if tooLarge then
                        truncated <- true
                        reading <- false
                    else
                        rows.Add row

            return
                { Ok = true
                  Errors =
                    if tooLarge then
                        [| { Message =
                               sprintf
                                   "Result exceeds %i characters (each cell also counts as %i), the rest is omitted"
                                   maxSqlResultLength
                                   sqlCellCost
                             Kind = VEKResultTooLarge
                             Line = None
                             Column = None
                             Path = None } |]
                    else
                        [||]
                  Columns =
                    columns
                    |> Array.mapi (fun i c ->
                        if jsonAsText.[i] then
                            { c with JsonAsText = Some true }
                        else
                            c)
                  Rows = rows.ToArray()
                  Truncated = truncated }
        with :? PostgresException as e ->
            let message = sprintf "%s (SQLSTATE %s)" e.MessageText e.SqlState
            return sqlFailure (sqlErrorKind e.SqlState) message (userSqlPosition query e.Position)
    }

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
        task {
            match ctx.Layout.FindEntity ref with
            | None -> return None
            | Some entity ->
                let! source = getSourceLayout ctx
                return Some(entityDetails source ref entity)
        }

    member this.GetDependencies
        (entityRef: ResolvedEntityRef)
        (fieldName: FieldName option)
        : Task<Result<DependenciesResult, DependenciesError>> =
        task {
            match ctx.Layout.FindEntity entityRef with
            | None -> return Error DEEntityNotFound
            | Some entity ->
                let target =
                    match fieldName with
                    | None -> Ok None
                    | Some name ->
                        match entity.FindField name with
                        | None -> Error DEFieldNotFound
                        | Some info -> Ok(Some info.Name)

                match target with
                | Error e -> return Error e
                | Ok target ->
                    let! index = this.GetIndex()

                    let lookup (dict: Dictionary<'k, MutableHashSet<DependencyItem>>) (key: 'k) =
                        match dict.TryGetValue key with
                        | (true, items) -> items :> DependencyItem seq
                        | (false, _) -> Seq.empty

                    let (resolved, word) =
                        match target with
                        | None -> (lookup index.Entities entityRef, string entityRef.Name)
                        | Some name ->
                            let declRef = declaringEntity ctx.Layout entityRef name
                            (lookup index.Fields { Entity = declRef; Name = name }, string name)

                    let items = Seq.append resolved (jsMatches index.JSSources word)

                    return
                        Ok
                            { Ok = true
                              Items = sortDependencies items }
        }

    member private this.GetIndex() : Task<DependencyIndex> =
        task {
            match dependencyIndexes.TryGetValue ctx.Layout with
            | (true, index) -> return index
            | (false, _) ->
                let! jsSources = loadJSSources ctx
                let index = buildDependencyIndex ctx.Layout ctx.UserViews ctx.Permissions jsSources
                dependencyIndexes.AddOrUpdate(ctx.Layout, index)
                return index
        }

    member this.Search(req: SearchRequest) : Task<SearchResult> =
        task {
            let! index = this.GetIndex()

            let! metadata =
                task {
                    match searchSourcesCache.TryGetValue ctx.Layout with
                    | (true, sources) -> return sources
                    | (false, _) ->
                        let! sources = loadMetadataSources ctx
                        searchSourcesCache.AddOrUpdate(ctx.Layout, sources)
                        return sources
                }

            let kinds = req.Kinds |> Array.map (fun k -> Map.find k searchKinds) |> Set.ofArray

            let regexResult =
                if req.IsRegex then
                    try
                        let options =
                            if req.CaseSensitive then
                                RegexOptions.Multiline
                            else
                                RegexOptions.Multiline ||| RegexOptions.IgnoreCase

                        Ok(Some(Regex(req.Query, options, regexTimeout)))
                    with :? ArgumentException as e ->
                        Error e.Message
                else
                    Ok None

            match regexResult with
            | Error message ->
                return
                    { Ok = false
                      Errors =
                        [| { Kind = "regex_invalid"
                             Message = message } |]
                      Truncated = false
                      Matches = [||] }
            | Ok regex ->
                let found = List<SearchMatch>()
                let mutable timedOut = false

                use enumerator =
                    (Seq.append index.JSSources metadata
                     |> Seq.filter (fun s -> Set.contains s.Item.Kind kinds))
                        .GetEnumerator()

                while not timedOut && enumerator.MoveNext() do
                    try
                        found.AddRange(sourceMatches regex req enumerator.Current)
                    with SearchTimeout ->
                        timedOut <- true

                let sorted =
                    found
                    |> Seq.sortBy (fun m -> (m.Kind, m.Schema, m.Name, m.Part, m.Line, m.Column))
                    |> Seq.toArray

                return
                    { Ok = not timedOut
                      Errors =
                        if timedOut then
                            [| { Kind = "regex_timeout"
                                 Message =
                                   sprintf
                                       "Regular expression exceeded %i ms on one of the sources"
                                       (int regexTimeout.TotalMilliseconds) } |]
                        else
                            [||]
                      Truncated = sorted.Length > req.Limit
                      Matches = Array.truncate req.Limit sorted }
        }

    // Actions, triggers and modules are all loaded as ES modules, so they are all checked as such.
    member this.CheckCode(req: CodeCheckRequest) : Task<CodeCheckResult> =
        task {
            match JSEngine.CheckSyntax(req.Source, true) with
            | Ok() ->
                return
                    { Ok = true
                      Errors = [||]
                      Warnings = [||] }
            | Error e ->
                return
                    { Ok = false
                      Errors =
                        [| { Message = e.Message
                             Kind = VEKSyntax
                             Line = e.Line
                             Column = e.Column
                             Path = None } |]
                      Warnings = [||] }
        }

    member this.RunSql(req: SqlQueryRequest) : Task<Result<SqlQueryResult, RunSqlError>> =
        task {
            do! enterReadOnly ctx req.TimeoutMs

            let! unsafeDatabase =
                ctx.Transaction.Connection.Query.ExecuteValueQuery unsafeDatabaseQuery Map.empty ctx.CancellationToken

            match unsafeDatabase with
            | Some(_, _, OzmaDB.SQL.AST.VBool false) ->
                let! ret = runWrappedQuery ctx req.Sql req.Params req.MaxRows
                return Ok ret
            | _ -> return Error RSEUnsafeDatabase
        }

    member this.PgReport(name: string) : Task<SqlQueryResult option> =
        task {
            match Map.tryFind name pgReports with
            | None -> return None
            | Some query ->
                do! enterReadOnly ctx pgReportTimeoutMs

                let! hasExtension =
                    task {
                        if name <> "statements" then
                            return true
                        else
                            let! ret =
                                ctx.Transaction.Connection.Query.ExecuteValueQuery
                                    "SELECT EXISTS (SELECT FROM pg_extension WHERE extname = 'pg_stat_statements')"
                                    Map.empty
                                    ctx.CancellationToken

                            return
                                match ret with
                                | Some(_, _, OzmaDB.SQL.AST.VBool true) -> true
                                | _ -> false
                    }

                if hasExtension then
                    let! ret = runWrappedQuery ctx query [||] pgReportMaxRows
                    return Some ret
                else
                    return
                        Some(
                            sqlFailure
                                VEKExtensionMissing
                                "Extension pg_stat_statements is not installed in this database"
                                None
                        )
        }

    interface IIntrospectionAPI with
        member this.ValidateQuery req = this.ValidateQuery req
        member this.GetSchemas() = this.GetSchemas()
        member this.GetEntity ref = this.GetEntity ref
        member this.GetDependencies ref field = this.GetDependencies ref field
        member this.Search req = this.Search req
        member this.CheckCode req = this.CheckCode req
        member this.RunSql req = this.RunSql req
        member this.PgReport name = this.PgReport name
