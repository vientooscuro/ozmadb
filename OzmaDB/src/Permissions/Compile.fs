module OzmaDB.Permissions.Compile

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.Arguments
open OzmaDB.Layout.Types
open OzmaDB.OzmaQL.AST
open OzmaDB.Permissions.Resolve

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.Rename

type CompiledRestriction =
    { From: SQL.FromExpr
      Joins: JoinPaths
      Where: SQL.ValueExpr }

type DMLRestriction =
    { From: SQL.FromExpr option
      Where: SQL.ValueExpr }

let private defaultWhere: SQL.ValueExpr = SQL.VEValue(SQL.VBool true)

let private restrictionJoinNamespace = OzmaQLName "restr"

let private restrictionCompilationFlags =
    { defaultCompilationFlags with
        SubExprJoinNamespace = restrictionJoinNamespace }

let compileRestriction
    (layout: Layout)
    (entityRef: ResolvedEntityRef)
    (arguments: QueryArguments)
    (restr: ResolvedFieldExpr)
    : QueryArguments * CompiledRestriction =
    let entity = layout.FindEntity entityRef |> Option.get
    // We don't want compiler to add type check to the result, because our own typecheck is built into the restriction.
    // Hence, a hack: we pretend to use root entity instead, but add an alias so that expression properly binds.
    let fEntity =
        { fromEntity (relaxEntityRef entity.Root) with
            Alias = Some restrictedEntityRef.Name }

    let (info, from) =
        compileSingleFromExpr restrictionCompilationFlags layout arguments (FEntity fEntity) (Some restr)

    let ret =
        { From = from.From
          Joins = from.Joins
          Where = Option.defaultValue defaultWhere from.Where }

    (info.Arguments, ret)

let restrictionToSelect (ref: ResolvedEntityRef) (restr: CompiledRestriction) : SQL.SelectExpr =
    let select =
        { SQL.emptySingleSelectExpr with
            Columns = [| SQL.SCAll(Some restrictedTableRef) |]
            From = Some restr.From
            Where = Some restr.Where }

    { CTEs = None
      Tree = SQL.SSelect select
      Extra = null }

let restrictionToDMLExpr
    (entityRef: ResolvedEntityRef)
    (newTableName: SQL.TableName)
    (restr: CompiledRestriction)
    : DMLRestriction =
    let opIdColumn =
        SQL.VEColumn
            { Table = Some { Schema = None; Name = newTableName }
              Name = sqlFunId }

    let restrIdColumn =
        SQL.VEColumn
            { Table = Some restrictedTableRef
              Name = sqlFunId }

    // Keep restriction as plain WHERE when FROM is a single table. This avoids an unnecessary self-join.
    match restr.From with
    | SQL.FTable table ->
        let renamesMap = Map.singleton (fromTableName table) newTableName

        { From = None
          Where = SQL.naiveRenameTablesExpr renamesMap restr.Where }
    | _ ->
        let idEq = SQL.VEBinaryOp(opIdColumn, SQL.BOEq, restrIdColumn)

        { From = Some restr.From
          Where = SQL.VEAnd(restr.Where, idEq) }
