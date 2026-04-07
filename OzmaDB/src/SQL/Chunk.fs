module OzmaDB.SQL.Chunk

open OzmaDB.SQL.AST

type QueryChunk =
    { Offset: ValueExpr option
      Limit: ValueExpr option
      Where: ValueExpr option }

// Build a map from output column alias -> its expression for a list of SELECT columns.
// Only includes named SCExpr columns (SCAll is ignored — we can't know what columns it expands to).
let private buildAliasMap (cols: SelectedColumn[]) : Map<ColumnName, ValueExpr> =
    cols
    |> Array.choose (function
        | SCExpr(Some alias, expr) -> Some(alias, expr)
        | _ -> None)
    |> Map.ofArray

// Returns true if the expression contains any aggregate function call.
let rec private containsAggregate (expr: ValueExpr) : bool =
    match expr with
    | VEAggFunc _ -> true
    | VEValue _
    | VEColumn _
    | VEPlaceholder _ -> false
    | VENot e -> containsAggregate e
    | VEAnd(a, b)
    | VEOr(a, b)
    | VEBinaryOp(a, _, b)
    | VEDistinct(a, b)
    | VENotDistinct(a, b)
    | VEAny(a, _, b)
    | VEAll(a, _, b)
    | VEArrayIndex(a, b)
    | VESimilarTo(a, b)
    | VENotSimilarTo(a, b) -> containsAggregate a || containsAggregate b
    | VEIn(e, vals)
    | VENotIn(e, vals) -> containsAggregate e || Array.exists containsAggregate vals
    | VEBetween(a, b, c)
    | VENotBetween(a, b, c) -> containsAggregate a || containsAggregate b || containsAggregate c
    | VEIsNull e
    | VEIsNotNull e
    | VECast(e, _) -> containsAggregate e
    | VEInQuery(e, _)
    | VENotInQuery(e, _) -> containsAggregate e
    | VESpecialFunc(_, args)
    | VEFunc(_, args)
    | VEArray args -> Array.exists containsAggregate args
    | VEWindowFunc(_, args, _) -> Array.exists containsAggregate args
    | VECase(cases, def) ->
        Array.exists (fun (cond, res) -> containsAggregate cond || containsAggregate res) cases
        || Option.exists containsAggregate def
    | VEExists _
    | VESubquery _ -> false // subqueries have their own scope

// Attempt to expand alias references in a ValueExpr using the provided alias->expr map.
// Returns None if expansion fails (alias maps to an aggregate).
// Unknown aliases (not in the map) are left as-is — they may be real base table column names.
let private tryExpandAliases (aliasMap: Map<ColumnName, ValueExpr>) (expr: ValueExpr) : ValueExpr option =
    let mutable failed = false

    let mapper =
        { idValueExprGenericMapper with
            ColumnReference =
                fun col ->
                    match col.Table with
                    | None ->
                        match Map.tryFind col.Name aliasMap with
                        | Some replacement when not (containsAggregate replacement) -> replacement
                        | Some _ ->
                            // Alias maps to an aggregate — cannot push down.
                            failed <- true
                            VEValue VNull // placeholder, won't be used
                        | None ->
                            // Unknown alias — keep as-is.
                            VEColumn col
                    | Some _ -> VEColumn col }

    let result = genericMapValueExpr mapper expr
    if failed then None else Some result

let emptyQueryChunk =
    { Offset = None
      Limit = None
      Where = None }
    : QueryChunk

let private applyToOrderLimit (chunk: QueryChunk) (orderLimit: OrderLimitClause) =
    let offsetExpr =
        match chunk.Offset with
        | None -> orderLimit.Offset
        | Some offset ->
            Some
            <| match orderLimit.Offset with
               | None -> offset
               | Some oldOffset ->
                   let safeOffset = VESpecialFunc(SFGreatest, [| VEValue(VInt 0); offset |])
                   VEBinaryOp(oldOffset, BOPlus, safeOffset)

    let limitExpr =
        match chunk.Limit with
        | None -> orderLimit.Limit
        | Some limit ->
            Some
            <| match orderLimit.Limit with
               | None -> limit
               | Some oldLimit ->
                   // If offset is used, we need to consider of it in limit too to prevent leaks.
                   let oldLimit =
                       match chunk.Offset with
                       | None -> oldLimit
                       | Some offset ->
                           let safeOffset = VESpecialFunc(SFGreatest, [| VEValue(VInt 0); offset |])
                           let newLimit = VEBinaryOp(oldLimit, BOMinus, safeOffset)
                           VESpecialFunc(SFGreatest, [| VEValue(VInt 0); newLimit |])

                   VESpecialFunc(SFLeast, [| oldLimit; limit |])

    { orderLimit with
        Offset = offsetExpr
        Limit = limitExpr }

type private ChunkApplier(chunk: QueryChunk) =
    let applyLimitSelectTreeExpr: SelectTreeExpr -> SelectTreeExpr =
        function
        | SSelect sel ->
            SSelect
                { sel with
                    OrderLimit = applyToOrderLimit chunk sel.OrderLimit }
        | SValues _ as values ->
            // Outer select is the way.
            let fromAlias =
                { Name = SQLName "inner"
                  Columns = None }

            let innerSelect = selectExpr values
            let tableExpr = subSelectExpr fromAlias innerSelect

            let outerSelect =
                { emptySingleSelectExpr with
                    Columns = [| SCAll None |]
                    From = Some <| FTableExpr tableExpr
                    OrderLimit = applyToOrderLimit chunk emptyOrderLimitClause }

            SSelect outerSelect
        | SSetOp setOp ->
            SSetOp
                { setOp with
                    OrderLimit = applyToOrderLimit chunk setOp.OrderLimit }

    // Try to push the chunk WHERE condition directly into the inner SSelect query,
    // expanding SELECT column aliases in the WHERE expression.
    // This allows PostgreSQL to use indexes on the base tables instead of filtering
    // a materialised subquery result.
    //
    // Safe only when the inner query:
    //   - is a plain SSelect (not a set operation or VALUES)
    //   - has no DISTINCT (DISTINCT filters duplicates after projection; WHERE before projection changes semantics)
    //   - has no GROUP BY (chunk WHERE might reference aggregated column aliases)
    //   - has no existing LIMIT/OFFSET (if inner already has LIMIT, pushing WHERE inside changes result set)
    let tryPushdownWhere (where: ValueExpr) (select: SelectExpr) : SelectExpr option =
        match select.Tree with
        | SSelect inner when
            not inner.Distinct
            && Array.isEmpty inner.GroupBy
            && inner.OrderLimit.Limit.IsNone
            && inner.OrderLimit.Offset.IsNone
            ->
            let aliasMap = buildAliasMap inner.Columns

            match tryExpandAliases aliasMap where with
            | None -> None
            | Some expandedWhere ->
                // Merge with any existing WHERE using AND.
                let mergedWhere =
                    match inner.Where with
                    | None -> expandedWhere
                    | Some existing -> VEAnd(existing, expandedWhere)

                let newTree =
                    SSelect
                        { inner with
                            Where = Some mergedWhere
                            OrderLimit = applyToOrderLimit chunk inner.OrderLimit }

                Some { select with Tree = newTree }
        | _ -> None

    let applySelectExpr (select: SelectExpr) : SelectExpr =
        match chunk.Where with
        | None ->
            { select with
                Tree = applyLimitSelectTreeExpr select.Tree }
        | Some where ->
            // First attempt: push WHERE directly into the inner query.
            match tryPushdownWhere where select with
            | Some pushed -> pushed
            | None ->
                // Fallback: wrap in outer SELECT with restrictions and limits.
                let fromAlias: TableAlias =
                    { Name = SQLName "inner"
                      Columns = None }

                let tableExpr = subSelectExpr fromAlias select

                let outerSelect =
                    { emptySingleSelectExpr with
                        Columns = [| SCAll None |]
                        From = Some <| FTableExpr tableExpr
                        Where = Some where
                        OrderLimit = applyToOrderLimit chunk emptyOrderLimitClause }

                selectExpr (SSelect outerSelect)

    member this.ApplySelectExpr select = applySelectExpr select

let selectExprChunk (chunk: QueryChunk) (select: SelectExpr) : SelectExpr =
    let applier = ChunkApplier chunk
    applier.ApplySelectExpr select
