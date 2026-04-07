module OzmaDB.OzmaQL.Query

open System.Linq
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open FSharpPlus
open Newtonsoft.Json.Linq
open Npgsql

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.Serialization
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.Arguments
open OzmaDB.Layout.Types
open OzmaDB.Layout.Errors
open OzmaDB.SQL.Utils
open OzmaDB.SQL.Query

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.Misc

[<SerializeAsObject("error")>]
type OzmaQLExecutionError =
    | [<CaseKey(null, Type = CaseSerialization.InnerObject)>] UVEIntegrity of IntegrityQueryError
    | [<CaseKey("argument", Type = CaseSerialization.InnerObject)>] UVEArgument of ArgumentCheckError
    | [<CaseKey("execution", IgnoreFields = [| "Details" |])>] UVEExecution of Details: string

    member this.LogMessage =
        match this with
        | UVEIntegrity e -> e.LogMessage
        | UVEArgument e -> e.LogMessage
        | UVEExecution details -> details

    [<DataMember>]
    member this.Message =
        match this with
        | UVEIntegrity e -> e.Message
        | UVEArgument e -> e.Message
        | UVEExecution details -> details

    member this.HTTPResponseCode =
        match this with
        | UVEIntegrity e -> 422
        | UVEArgument e -> 422
        | UVEExecution details -> 500

    member this.ShouldLog = false

    member this.Details =
        match this with
        | UVEIntegrity e -> e.Details
        | UVEArgument e -> e.Details
        | UVEExecution details -> Map.empty

    static member private LookupKey = prepareLookupCaseKey<OzmaQLExecutionError>

    member this.Error =
        match this with
        | UVEIntegrity e -> e.Error
        | _ -> OzmaQLExecutionError.LookupKey this |> Option.get

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog
        member this.Details = this.Details

    interface IErrorDetails with
        member this.LogMessage = this.LogMessage
        member this.Message = this.Message
        member this.HTTPResponseCode = this.HTTPResponseCode
        member this.Error = this.Error

type UserViewExecutionException(details: OzmaQLExecutionError, innerException: exn) =
    inherit UserException(details.Message, innerException, true)

    member this.Details = details

    new(details: OzmaQLExecutionError) = UserViewExecutionException(details, null)

type ExecutedAttributesMap = Map<AttributeName, SQL.Value>
type ExecutedAttributeTypesMap = Map<AttributeName, SQL.SimpleValueType>

[<NoEquality; NoComparison>]
type ExecutedValue =
    { Value: SQL.Value
      [<DataMember(EmitDefaultValue = false)>]
      Attributes: ExecutedAttributesMap
      [<DataMember(EmitDefaultValue = false)>]
      Pun: SQL.Value option }

[<NoEquality; NoComparison>]
type ExecutedEntityId =
    { Id: int
      [<DataMember(EmitDefaultValue = false)>]
      SubEntity: ResolvedEntityRef option }

[<NoEquality; NoComparison>]
type ExecutedRow =
    { Values: ExecutedValue[]
      DomainId: GlobalDomainId option
      [<DataMember(EmitDefaultValue = false)>]
      Attributes: ExecutedAttributesMap
      [<DataMember(EmitDefaultValue = false)>]
      EntityIds: Map<DomainIdColumn, ExecutedEntityId>
      [<DataMember(EmitDefaultValue = false)>]
      MainId: int option
      [<DataMember(EmitDefaultValue = false)>]
      MainSubEntity: ResolvedEntityRef option }

[<NoEquality; NoComparison>]
type ExecutedColumnInfo =
    { Name: OzmaQLName
      AttributeTypes: ExecutedAttributeTypesMap
      CellAttributeTypes: ExecutedAttributeTypesMap
      ValueType: SQL.SimpleValueType
      PunType: SQL.SimpleValueType option }

[<NoEquality; NoComparison>]
type ExecutedViewInfo =
    { AttributeTypes: ExecutedAttributeTypesMap
      RowAttributeTypes: ExecutedAttributeTypesMap
      Columns: ExecutedColumnInfo[]
      ArgumentAttributeTypes: Map<ArgumentName, ExecutedAttributeTypesMap> }

[<NoEquality; NoComparison>]
type ExecutingViewExpr =
    { Attributes: ExecutedAttributesMap
      ColumnAttributes: ExecutedAttributesMap[]
      ArgumentAttributes: Map<ArgumentName, ExecutedAttributesMap>
      Rows: IAsyncEnumerable<ExecutedRow> }

[<NoEquality; NoComparison>]
type private ExecutedAttributes =
    { Attributes: ExecutedAttributesMap
      AttributeTypes: ExecutedAttributeTypesMap
      ArgumentAttributes: Map<ArgumentName, ExecutedAttributeTypesMap * ExecutedAttributesMap>
      ColumnAttributes: Map<FieldName, ExecutedAttributeTypesMap * ExecutedAttributesMap> }

// TODO: we could improve performance with pipelining (we could also pipeline attributes query).
let setPragmas
    (connection: QueryConnection)
    (pragmas: CompiledPragmasMap)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        for KeyValue(name, v) in pragmas do
            let q =
                { Parameter = name
                  Scope = Some SQL.SSLocal
                  Value = SQL.SVValue [| v |] }
                : SQL.SetExpr

            let! _ = connection.ExecuteNonQuery (q.ToSQLString()) Map.empty cancellationToken
            ()
    }

let unsetPragmas
    (connection: QueryConnection)
    (pragmas: CompiledPragmasMap)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        for KeyValue(name, v) in pragmas do
            let q =
                { Parameter = name
                  Scope = Some SQL.SSLocal
                  Value = SQL.SVDefault }
                : SQL.SetExpr

            let! _ = connection.ExecuteNonQuery (q.ToSQLString()) Map.empty cancellationToken
            ()
    }

let private splitPairsMap pairsMap =
    let fsts = Map.map (fun name -> fst) pairsMap
    let snds = Map.map (fun name -> snd) pairsMap
    (fsts, snds)

let private getSingleRowQuery (viewExpr: CompiledViewExpr) : (ColumnType[] * SQL.SelectExpr) option =
    let allColumns =
        Seq.append viewExpr.SingleRowQuery.ConstColumns viewExpr.SingleRowQuery.SingleRowColumns

    let colTypes = allColumns |> Seq.map (fun (info, col) -> info.Type) |> Seq.toArray

    if Array.isEmpty colTypes then
        None
    else
        // Some metadata expressions are compiled into SingleRow columns but rely on CTEs
        // defined on the main query. Keep those CTEs available as a fallback.
        let ctes = Option.orElse viewExpr.SingleRowQuery.CTEs viewExpr.Query.Expression.CTEs

        let query =
            { SQL.emptySingleSelectExpr with
                Columns =
                    allColumns
                    |> Seq.map (fun (info, col) -> SQL.SCExpr(Some info.Name, col))
                    |> Seq.toArray }

        let select =
            { SQL.selectExpr (SQL.SSelect query) with
                CTEs = ctes }

        Some(colTypes, select)

let private stripTopLevelLimitOffset (select: SQL.SelectExpr) : SQL.SelectExpr =
    let tree =
        match select.Tree with
        | SQL.SSelect query ->
            let orderLimit =
                { query.OrderLimit with
                    Limit = None
                    Offset = None }

            SQL.SSelect { query with OrderLimit = orderLimit }
        | SQL.SSetOp setOp ->
            let orderLimit =
                { setOp.OrderLimit with
                    Limit = None
                    Offset = None }

            SQL.SSetOp { setOp with OrderLimit = orderLimit }
        | (SQL.SValues _ as values) -> values

    { select with Tree = tree }

// Collect all table names referenced in a ValueExpr (via VEColumn).
let private collectReferencedTables (expr: SQL.ValueExpr) : Set<SQL.TableName> =
    let mutable tables = Set.empty

    let rec traverse =
        function
        | SQL.VEColumn col ->
            match col.Table with
            | Some t -> tables <- Set.add t.Name tables
            | None -> ()
        | SQL.VENot e -> traverse e
        | SQL.VEOr(a, b)
        | SQL.VEAnd(a, b)
        | SQL.VEBinaryOp(a, _, b)
        | SQL.VEDistinct(a, b)
        | SQL.VENotDistinct(a, b)
        | SQL.VEAny(a, _, b)
        | SQL.VEAll(a, _, b)
        | SQL.VEArrayIndex(a, b)
        | SQL.VESimilarTo(a, b)
        | SQL.VENotSimilarTo(a, b) ->
            traverse a
            traverse b
        | SQL.VEIn(e, vals) ->
            traverse e
            Array.iter traverse vals
        | SQL.VENotIn(e, vals) ->
            traverse e
            Array.iter traverse vals
        | SQL.VEBetween(a, b, c)
        | SQL.VENotBetween(a, b, c) ->
            traverse a
            traverse b
            traverse c
        | SQL.VEIsNull e
        | SQL.VEIsNotNull e
        | SQL.VECast(e, _) -> traverse e
        | SQL.VEInQuery(e, _)
        | SQL.VENotInQuery(e, _) ->
            // Subqueries have their own scope — only traverse the outer expr.
            traverse e
        | SQL.VESpecialFunc(_, args)
        | SQL.VEFunc(_, args)
        | SQL.VEArray args -> Array.iter traverse args
        | SQL.VEAggFunc(_, aggExpr, filter) ->
            match aggExpr with
            | SQL.AEAll(exprs, _) -> Array.iter traverse exprs
            | SQL.AEDistinct(e, _) -> traverse e
            | SQL.AEStar -> ()

            Option.iter traverse filter
        | SQL.VEWindowFunc(_, args, _) -> Array.iter traverse args
        | SQL.VECase(cases, def) ->
            Array.iter (fun (cond, res) -> traverse cond; traverse res) cases
            Option.iter traverse def
        | SQL.VEExists _ // subquery — own scope
        | SQL.VESubquery _ // subquery — own scope
        | SQL.VEValue _
        | SQL.VEPlaceholder _ -> ()

    traverse expr
    tables

// Get the effective alias name of a FromExpr's rightmost table (the B side of a JOIN).
let private fromExprAliasName (from: SQL.FromExpr) : SQL.TableName option =
    match from with
    | SQL.FTable ft ->
        match ft.Alias with
        | Some a -> Some a.Name
        | None -> Some ft.Table.Name
    | SQL.FTableExpr fte ->
        match fte.Alias with
        | Some a -> Some a.Name
        | None -> None
    | SQL.FJoin _ ->
        // Joins don't have a single top-level alias.
        None

// Recursively strip LEFT JOINs whose right side is not referenced in the given set of used tables.
// Returns None if the entire FROM clause was stripped (shouldn't happen in practice).
let rec private stripUnusedLeftJoins (usedTables: Set<SQL.TableName>) (from: SQL.FromExpr) : SQL.FromExpr option =
    match from with
    | SQL.FJoin join when join.Type = SQL.Left ->
        // Check if the right side's alias is used.
        let rightAlias = fromExprAliasName join.B

        let rightUsed =
            match rightAlias with
            | Some alias -> Set.contains alias usedTables
            | None -> true // Can't determine — keep it.

        if rightUsed then
            // Right side is used; recurse into left side too.
            match stripUnusedLeftJoins usedTables join.A with
            | Some newA -> Some(SQL.FJoin { join with A = newA })
            | None -> Some from // Left got stripped away entirely — keep original
        else
            // Right side not used — drop this JOIN entirely, keep only the left side.
            stripUnusedLeftJoins usedTables join.A
    | SQL.FJoin join ->
        // Non-LEFT join — must keep, but recurse into left side.
        match stripUnusedLeftJoins usedTables join.A with
        | Some newA -> Some(SQL.FJoin { join with A = newA })
        | None -> Some from
    | _ -> Some from

// Build a minimal count query from a SingleSelectExpr.
// - Drops ORDER BY (never affects row count).
// - Drops all SELECT columns (replaced by count(*)).
// - Strips LEFT JOINs not referenced in WHERE or GROUP BY.
// - If query has GROUP BY or DISTINCT, wraps in outer SELECT count(*).
let private buildCountSingleSelect (query: SQL.SingleSelectExpr) : SQL.SelectExpr =
    let countAll =
        SQL.SCExpr(None, SQL.VEAggFunc(SQL.SQLName "count", SQL.AEStar, None))

    let noOrderLimit =
        { query.OrderLimit with
            OrderBy = [||]
            Limit = None
            Offset = None }

    // Collect tables used in WHERE and GROUP BY.
    let usedTables =
        let fromWhere =
            query.Where |> Option.map collectReferencedTables |> Option.defaultValue Set.empty

        let fromGroupBy =
            query.GroupBy |> Array.map collectReferencedTables |> Array.fold Set.union Set.empty

        Set.union fromWhere fromGroupBy

    let strippedFrom =
        query.From |> Option.bind (stripUnusedLeftJoins usedTables)

    if not (Array.isEmpty query.GroupBy) || query.Distinct then
        // GROUP BY or DISTINCT: the number of output rows = number of groups/distinct combos.
        // We can't simply replace columns — wrap in outer count.
        let inner =
            { query with
                Columns = [| SQL.SCExpr(None, SQL.VEValue SQL.VNull) |] // SELECT NULL — minimal
                OrderLimit = noOrderLimit
                From = strippedFrom }

        let innerAlias : SQL.TableAlias = { Name = SQL.SQLName "__count_inner"; Columns = None }

        let outer =
            { SQL.emptySingleSelectExpr with
                Columns = [| countAll |]
                From = Some(SQL.FTableExpr(SQL.subSelectExpr innerAlias (SQL.selectExpr (SQL.SSelect inner)))) }

        SQL.selectExpr (SQL.SSelect outer)
    else
        // Simple case: replace columns with count(*), strip ORDER BY and unused LEFT JOINs.
        let simplified =
            { query with
                Columns = [| countAll |]
                OrderLimit = noOrderLimit
                From = strippedFrom }

        SQL.selectExpr (SQL.SSelect simplified)

// Build a count query from a full SelectExpr, stripping everything unnecessary.
let private buildCountQuery (select: SQL.SelectExpr) : SQL.SelectExpr =
    // Strip top-level CTEs from count — they may be needed if referenced in WHERE,
    // but since we strip LEFT JOINs (which are the main CTE consumers), keep CTEs for safety.
    match select.Tree with
    | SQL.SSelect query ->
        { (buildCountSingleSelect query) with CTEs = select.CTEs }
    | SQL.SSetOp _
    | SQL.SValues _ ->
        // For set operations and VALUES, fall back to wrapping.
        let noLimit = stripTopLevelLimitOffset select

        let outerAlias : SQL.TableAlias =
            { Name = SQL.SQLName "__request_lines_number"
              Columns = None }

        SQL.selectExpr (
            SQL.SSelect
                { SQL.emptySingleSelectExpr with
                    Columns = [| SQL.SCExpr(None, SQL.VEAggFunc(SQL.SQLName "count", SQL.AEStar, None)) |]
                    From = Some(SQL.FTableExpr(SQL.subSelectExpr outerAlias noLimit)) }
        )

let private getTotalRowsCountQuery (rowsQuery: SQL.SelectExpr) : string =
    let countQuery = buildCountQuery rowsQuery
    countQuery.ToSQLString()

let private requestLinesRowAttributeName = OzmaQLName "__request_lines_number"
let private requestLinesRowColumnName = SQL.SQLName "__request_lines_number"

let private getRequestLinesRowsQuery (baseExpr: SQL.SelectExpr) (rowsExpr: SQL.SelectExpr) : string =
    sprintf
        "WITH __request_lines_base AS (%s), __request_lines_chunk AS (%s) SELECT __request_lines_meta.total AS %s, __request_lines_chunk.* FROM __request_lines_chunk CROSS JOIN (SELECT count(*)::integer AS total FROM __request_lines_base) __request_lines_meta"
        (baseExpr.ToSQLString())
        (rowsExpr.ToSQLString())
        (string requestLinesRowColumnName)

let private parseAttributesResult
    (columns: ColumnType[])
    (values: (SQL.SQLName * SQL.SimpleValueType * SQL.Value)[])
    : ExecutedAttributes =
    let takeViewAttribute colType (_, valType, v) =
        match colType with
        | CTMeta(CMRowAttribute name) -> Some(name, (valType, v))
        | _ -> None

    let viewAttributes = Seq.map2Maybe takeViewAttribute columns values |> Map.ofSeq

    let takeColumnAttribute colType (_, valType, v) =
        match colType with
        | CTColumnMeta(fieldName, CCCellAttribute name) -> Some(fieldName, Map.singleton name (valType, v))
        | _ -> None

    let colAttributes =
        Seq.map2Maybe takeColumnAttribute columns values
        |> Map.ofSeqWith (fun name -> Map.unionUnique)
        |> Map.map (fun fieldName -> splitPairsMap)

    let takeArgAttribute colType (_, valType, v) =
        match colType with
        | CTMeta(CMArgAttribute(argName, name)) -> Some(argName, Map.singleton name (valType, v))
        | _ -> None

    let argAttributes =
        Seq.map2Maybe takeArgAttribute columns values
        |> Map.ofSeqWith (fun name -> Map.unionUnique)
        |> Map.map (fun fieldName -> splitPairsMap)

    { Attributes = Map.map (fun name -> snd) viewAttributes
      AttributeTypes = Map.map (fun name -> fst) viewAttributes
      ColumnAttributes = colAttributes
      ArgumentAttributes = argAttributes }

let private parseResult
    (mainRootEntity: ResolvedEntityRef option)
    (domains: Domains)
    (columns: CompiledColumnInfo[])
    (resultColumns: (SQL.SQLName * SQL.SimpleValueType)[])
    (rows: IAsyncEnumerable<SQL.Value[]>)
    (processFunc: ExecutedViewInfo -> IAsyncEnumerable<ExecutedRow> -> Task<'a>)
    : Task<'a> =
    let takeRowAttribute i (colInfo: CompiledColumnInfo) (_, valType) =
        match colInfo.Type with
        | CTMeta(CMRowAttribute name) -> Some(name, (valType, i))
        | _ -> None

    let rowAttributes =
        Seq.mapi2Maybe takeRowAttribute columns resultColumns |> Map.ofSeq

    let takeCellAttribute i (colInfo: CompiledColumnInfo) (_, valType) =
        match colInfo.Type with
        | CTColumnMeta(fieldName, CCCellAttribute name) -> Some(fieldName, (name, (valType, i)))
        | _ -> None

    let allCellAttributes =
        Seq.mapi2Maybe takeCellAttribute columns resultColumns
        |> Seq.groupBy fst
        |> Seq.map (fun (fieldName, attrs) -> (fieldName, attrs |> Seq.map snd |> Map.ofSeq))
        |> Map.ofSeq

    let takePunAttribute i (colInfo: CompiledColumnInfo) (_, valType) =
        match colInfo.Type with
        | CTColumnMeta(name, CCPun) -> Some(name, (valType, i))
        | _ -> None

    let punAttributes =
        Seq.mapi2Maybe takePunAttribute columns resultColumns |> Map.ofSeq

    let takeDomainColumn i (colInfo: CompiledColumnInfo) =
        match colInfo.Type with
        | CTMeta(CMDomain ns) -> Some(ns, i)
        | _ -> None

    let domainColumns = Seq.mapiMaybe takeDomainColumn columns |> Map.ofSeq

    let takeIdColumn i (colInfo: CompiledColumnInfo) =
        match colInfo.Type with
        | CTMeta(CMId name) -> Some(name, i)
        | _ -> None

    let idColumns = Seq.mapiMaybe takeIdColumn columns |> Map.ofSeq

    let takeSubEntityColumn i (colInfo: CompiledColumnInfo) =
        match colInfo.Type with
        | CTMeta(CMSubEntity name) -> Some(name, i)
        | _ -> None

    let subEntityColumns = Seq.mapiMaybe takeSubEntityColumn columns |> Map.ofSeq

    let takeMainIdColumn i (colInfo: CompiledColumnInfo) =
        match colInfo.Type with
        | CTMeta CMMainId -> Some i
        | _ -> None

    let mainIdColumn = Seq.mapiMaybe takeMainIdColumn columns |> Seq.first

    let takeMainSubEntityColumn i (colInfo: CompiledColumnInfo) =
        match colInfo.Type with
        | CTMeta CMMainSubEntity -> Some i
        | _ -> None

    let mainSubEntityColumn = Seq.mapiMaybe takeMainSubEntityColumn columns |> Seq.first

    let takeColumn i (colInfo: CompiledColumnInfo) (_, valType) =
        match colInfo.Type with
        | CTColumn name ->
            let cellAttributes =
                match Map.tryFind name allCellAttributes with
                | None -> Map.empty
                | Some a -> a

            let punType =
                Option.map (fun (valType, i) -> valType) <| Map.tryFind name punAttributes

            let columnInfo =
                { Name = name
                  AttributeTypes = Map.empty
                  CellAttributeTypes = Map.map (fun name (valType, i) -> valType) cellAttributes
                  ValueType = valType
                  PunType = punType }

            Some(cellAttributes, i, columnInfo)
        | m_ -> None

    let columnsMeta = Seq.mapi2Maybe takeColumn columns resultColumns |> Seq.toArray

    let parseRow (values: SQL.Value[]) =
        let getCell (cellAttributes, i, column) =
            let value = values.[i]
            let attrs = Map.map (fun name (valType, i) -> values.[i]) cellAttributes

            let pun =
                match value with
                | SQL.VNull -> None
                | _ ->
                    match Map.tryFind column.Name punAttributes with
                    | Some(valType, i) -> Some values.[i]
                    | None -> None

            { Attributes = attrs
              Value = value
              Pun = pun }

        let getDomainId i =
            match values.[i] with
            | SQL.VInt id -> Some id
            | SQL.VNull -> None
            | _ -> failwith "Domain id is not an integer"

        let getId i =
            match values.[i] with
            | SQL.VInt id -> Some id
            | SQL.VNull -> None
            | _ -> failwith "Entity id is not an integer"

        let getMainId i =
            match values.[i] with
            | SQL.VInt id -> id
            | _ -> failwith "Entity main id is not an integer"

        let getMainSubEntity i =
            match values.[i] with
            | SQL.VString subEntityString -> parseTypeName (Option.get mainRootEntity) subEntityString
            | _ -> failwith "Main subentity is not a string"

        let domainIds = Map.mapMaybe (fun ns i -> getDomainId i) domainColumns

        let rec getGlobalDomainId =
            function
            | DSingle(id, info) -> Some(id, info)
            | DMulti(ns, subdoms) ->
                match Map.tryFind ns domainIds with
                | None -> None
                | Some localId ->
                    let doms =
                        Map.findOrFailWith
                            (fun () -> sprintf "Unknown local domain id for namespace %i: %i" ns localId)
                            localId
                            subdoms

                    getGlobalDomainId doms

        let domainId = getGlobalDomainId domains

        let getSubEntity entity i =
            match values.[i] with
            | SQL.VString subEntityString -> Some <| parseTypeName entity subEntityString
            | SQL.VNull -> None
            | _ -> failwith "Entity subtype is not a string"

        let getEntityId info =
            let mid = idColumns |> Map.find info.IdColumn |> getId

            match mid with
            // ID can be NULL, for example, in case of JOINs.
            | None -> None
            | Some id ->
                let subEntity =
                    Option.bind (getSubEntity info.RootEntity) (Map.tryFind info.IdColumn subEntityColumns)

                let ret = { Id = id; SubEntity = subEntity }
                Some(info.IdColumn, ret)

        let getEntityIds fields =
            fields |> Map.values |> Seq.mapMaybe getEntityId |> Map.ofSeq

        let entityIds =
            Option.map (snd >> getEntityIds) domainId |> Option.defaultValue Map.empty

        let rowAttrs = Map.map (fun name (valType, i) -> values.[i]) rowAttributes
        let values = Array.map getCell columnsMeta
        let mainId = Option.map getMainId mainIdColumn
        let mainSubEntity = Option.map getMainSubEntity mainSubEntityColumn

        { Attributes = rowAttrs
          Values = values
          DomainId = Option.map fst domainId
          EntityIds = entityIds
          MainId = mainId
          MainSubEntity = mainSubEntity }

    let columns = Array.map (fun (attributes, i, column) -> column) columnsMeta

    let info: ExecutedViewInfo =
        { Columns = columns
          AttributeTypes = Map.empty
          RowAttributeTypes = Map.map (fun name (valType, i) -> valType) rowAttributes
          ArgumentAttributeTypes = Map.empty }

    let rows' = rows.Select(parseRow)
    processFunc info rows'

let convertQueryExecutionException (layout: Layout) (e: QueryExecutionException) =
    let known =
        match e.InnerException with
        | :? PostgresException as inner -> extractIntegrityQueryError layout inner
        | _ -> None

    match known with
    | None -> UVEExecution(fullUserMessage e)
    | Some e -> UVEIntegrity e

let runViewExpr
    (connection: QueryConnection)
    (layout: Layout)
    (viewExpr: CompiledViewExpr)
    (comments: string option)
    (arguments: ArgumentValuesMap)
    (cancellationToken: CancellationToken)
    (processFunc: ExecutedViewInfo -> ExecutingViewExpr -> Task<'a>)
    : Task<'a> =
    task {
        try
            let parameters = prepareArguments viewExpr.Query.Arguments arguments
            let prefix = convertComments comments

            do! setPragmas connection viewExpr.Pragmas cancellationToken

            let emptyAttrsResult =
                { Attributes = Map.empty
                  AttributeTypes = Map.empty
                  ColumnAttributes = Map.empty
                  ArgumentAttributes = Map.empty }

            let getAttrsResult attrsParameters =
                task {
                    match getSingleRowQuery viewExpr with
                    | None -> return emptyAttrsResult
                    | Some(colTypes, query) ->
                        match!
                            connection.ExecuteRowValuesQuery (prefix + string query) attrsParameters cancellationToken
                        with
                        | None -> return failwith "Unexpected empty query result"
                        | Some row -> return parseAttributesResult colTypes row
                }

            let mergeResults attrsResult info rows =
                let getColumnInfo (col: ExecutedColumnInfo) =
                    let attributeTypes =
                        match Map.tryFind col.Name attrsResult.ColumnAttributes with
                        | None -> Map.empty
                        | Some(attrTypes, attrs) -> attrTypes

                    { col with
                        AttributeTypes = attributeTypes }

                let getColumnAttributes (col: ExecutedColumnInfo) =
                    match Map.tryFind col.Name attrsResult.ColumnAttributes with
                    | None -> Map.empty
                    | Some(attrTypes, attrs) -> attrs

                let mergedInfo =
                    { info with
                        ArgumentAttributeTypes = Map.map (fun name -> fst) attrsResult.ArgumentAttributes
                        AttributeTypes = attrsResult.AttributeTypes
                        Columns = Array.map getColumnInfo info.Columns }

                let result =
                    { Attributes = attrsResult.Attributes
                      ColumnAttributes = Array.map getColumnAttributes info.Columns
                      ArgumentAttributes = Map.map (fun name -> snd) attrsResult.ArgumentAttributes
                      Rows = rows }

                processFunc mergedInfo result

            let! ret =
                match viewExpr.RequestLinesNumberPlaceholderId with
                | None ->
                    task {
                        let! attrsResult = getAttrsResult parameters

                        return!
                            connection.ExecuteQuery
                                (prefix + viewExpr.Query.Expression.ToSQLString())
                                parameters
                                cancellationToken
                            <| fun resultColumns rawRows ->
                                parseResult
                                    viewExpr.MainRootEntity
                                    viewExpr.Domains
                                    viewExpr.Columns
                                    resultColumns
                                    rawRows
                                <| fun info rows -> mergeResults attrsResult info rows
                    }
                | Some placeholderId ->
                    task {
                        let baseExpr =
                            Option.defaultValue viewExpr.Query.Expression viewExpr.RequestLinesNumberBaseExpression

                        let rowsQuery = getRequestLinesRowsQuery baseExpr viewExpr.Query.Expression

                        let requestLinesColumnInfo =
                            { Type = CTMeta(CMRowAttribute requestLinesRowAttributeName)
                              Name = requestLinesRowColumnName
                              Info = emptyColumnMetaInfo }
                            : CompiledColumnInfo

                        let queryColumns = Array.append [| requestLinesColumnInfo |] viewExpr.Columns

                        let! (info, rowsArr) =
                            connection.ExecuteQuery (prefix + rowsQuery) parameters cancellationToken
                            <| fun resultColumns rawRows ->
                                parseResult viewExpr.MainRootEntity viewExpr.Domains queryColumns resultColumns rawRows
                                <| fun info rows ->
                                    task {
                                        let! rowsArr = rows.ToArrayAsync(cancellationToken)
                                        return (info, rowsArr)
                                    }

                        let! totalRows =
                            match rowsArr |> Array.tryHead with
                            | Some row ->
                                task {
                                    return
                                        row.Attributes
                                        |> Map.tryFind requestLinesRowAttributeName
                                        |> Option.map SQL.parseSmallIntValue
                                        |> Option.defaultValue 0
                                }
                            | None ->
                                task {
                                    let countQuery = getTotalRowsCountQuery baseExpr

                                    let! countRow =
                                        connection.ExecuteRowValuesQuery
                                            (prefix + countQuery)
                                            parameters
                                            cancellationToken

                                    match countRow with
                                    | None -> return failwith "Unexpected empty request_lines_number count result"
                                    | Some values ->
                                        match values |> Array.tryHead with
                                        | None ->
                                            return failwith "Unexpected empty request_lines_number count row columns"
                                        | Some(_, _, value) -> return SQL.parseSmallIntValue value
                                }

                        let info =
                            { info with
                                RowAttributeTypes = Map.remove requestLinesRowAttributeName info.RowAttributeTypes }

                        let rowsArr =
                            rowsArr
                            |> Array.map (fun row ->
                                { row with
                                    Attributes = row.Attributes |> Map.remove requestLinesRowAttributeName })

                        let attrsParameters = Map.add placeholderId (SQL.VInt totalRows) parameters
                        let! attrsResult = getAttrsResult attrsParameters
                        return! mergeResults attrsResult info (rowsArr.ToAsyncEnumerable())
                    }

            do! unsetPragmas connection viewExpr.Pragmas cancellationToken
            return ret
        with
        | :? QueryExecutionException as e ->
            let details = convertQueryExecutionException layout e
            return raise <| UserViewExecutionException(details, e)
        | :? ArgumentCheckException as e -> return raise <| UserViewExecutionException(UVEArgument e.Details, e)
    }

type ExplainedQuery =
    { Query: string
      Parameters: ExprParameters
      Explanation: JToken }

    member this.ShouldLog = false
    member this.Details = Map.empty

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog
        member this.Details = this.Details


type ExplainedViewExpr =
    { Rows: ExplainedQuery
      Attributes: ExplainedQuery option }

    member this.ShouldLog = false
    member this.Details = Map.empty

    interface ILoggableResponse with
        member this.ShouldLog = this.ShouldLog
        member this.Details = this.Details

let explainViewExpr
    (connection: QueryConnection)
    (layout: Layout)
    (viewExpr: CompiledViewExpr)
    (maybeArguments: ArgumentValuesMap option)
    (explainOpts: ExplainOptions)
    (cancellationToken: CancellationToken)
    : Task<ExplainedViewExpr> =
    task {
        try
            let arguments =
                Option.defaultWith
                    (fun () ->
                        viewExpr.Query.Arguments.Types
                        |> Map.map (fun name arg -> defaultCompiledArgument arg))
                    maybeArguments

            let parameters = prepareArguments viewExpr.Query.Arguments arguments

            let parameters =
                match viewExpr.RequestLinesNumberPlaceholderId with
                | None -> parameters
                | Some placeholderId -> Map.add placeholderId (SQL.VInt 0) parameters

            do! setPragmas connection viewExpr.Pragmas cancellationToken

            let! attrsResult =
                task {
                    match getSingleRowQuery viewExpr with
                    | None -> return None
                    | Some(colTypes, query) ->
                        let! ret = runExplainQuery connection query parameters explainOpts cancellationToken

                        return
                            Some
                                { Query = string query
                                  Parameters = parameters
                                  Explanation = ret }
                }

            let! result = runExplainQuery connection viewExpr.Query.Expression parameters explainOpts cancellationToken

            do! unsetPragmas connection viewExpr.Pragmas cancellationToken

            return
                { Rows =
                    { Query = string viewExpr.Query.Expression
                      Parameters = parameters
                      Explanation = result }
                  Attributes = attrsResult }
        with
        | :? QueryExecutionException as e ->
            let details = convertQueryExecutionException layout e
            return raise <| UserViewExecutionException(details, e)
        | :? ArgumentCheckException as e -> return raise <| UserViewExecutionException(UVEArgument e.Details, e)
    }
