module OzmaDB.Triggers.Time

open System
open System.Threading
open System.Threading.Tasks
open NodaTime

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.AST
open OzmaDB.Layout.Types
open OzmaDB.Triggers.Source
open OzmaDB.Triggers.Types
open OzmaDB.Triggers.Merge
open OzmaDB.SQL.Query

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.Utils

[<NoEquality; NoComparison>]
type ClaimedTimeTriggerTask =
    { Id: int
      Attempts: int
      DueAt: Instant
      OffsetValue: int
      OffsetUnit: TriggerTimeOffsetUnit
      Trigger: TriggerRef
      EventEntity: ResolvedEntityRef
      RootEntity: ResolvedEntityRef
      RowId: int
      FieldName: FieldName }

let private queueTableName = "public.time_trigger_tasks"

let private triggerRefForEvent (eventEntity: ResolvedEntityRef) (trigger: MergedTrigger) : TriggerRef =
    let entityRef = Option.defaultValue eventEntity trigger.Inherited

    { Schema = trigger.Schema
      Entity = entityRef
      Name = trigger.Name }

let private queueDeleteByRootAndRow
    (query: QueryConnection)
    (rootEntity: ResolvedEntityRef)
    (rowId: int)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let q =
            sprintf
                "DELETE FROM %s WHERE root_entity_schema = %s AND root_entity_name = %s AND row_id = %s"
                queueTableName
                (SQL.renderSqlString (string rootEntity.Schema))
                (SQL.renderSqlString (string rootEntity.Name))
                (SQL.renderSqlInt rowId)

        let! _ = query.ExecuteNonQuery q Map.empty cancellationToken
        return ()
    }

let private enqueueSingleField
    (query: QueryConnection)
    (eventEntity: ResolvedEntityRef)
    (rootEntity: ResolvedEntityRef)
    (rowId: int)
    (fieldName: FieldName)
    (columnName: SQL.ColumnName)
    (triggers: MergedTrigger[])
    (cancellationToken: CancellationToken)
    : Task =
    task {
        if not <| Array.isEmpty triggers then
            let rootTable: SQL.TableRef =
                { Schema = Some(SQL.SQLName(string rootEntity.Schema))
                  Name = SQL.SQLName(string rootEntity.Name) }

            let values =
                triggers
                |> Seq.map (fun trig -> (triggerRefForEvent eventEntity trig, trig.OnTimeOffsetValue, trig.OnTimeOffsetUnit))
                |> Seq.map (fun (triggerRef: TriggerRef, offsetValue: int, offsetUnit: TriggerTimeOffsetUnit) ->
                    sprintf
                        "(%s, %s, %s, %s, %s, %s)"
                        (SQL.renderSqlString (string triggerRef.Schema))
                        (SQL.renderSqlString (string triggerRef.Entity.Schema))
                        (SQL.renderSqlString (string triggerRef.Entity.Name))
                        (SQL.renderSqlString (string triggerRef.Name))
                        (SQL.renderSqlInt offsetValue)
                        (SQL.renderSqlString (offsetUnit.ToString())))
                |> String.concat ", "

            let q =
                sprintf
                    """
INSERT INTO %s (
  trigger_schema,
  trigger_entity_schema,
  trigger_entity_name,
  trigger_name,
  event_entity_schema,
  event_entity_name,
  root_entity_schema,
  root_entity_name,
  row_id,
  field_name,
  offset_value,
  offset_unit,
  due_at,
  locked_until,
  attempts,
  last_error
)
SELECT
  v.trigger_schema,
  v.trigger_entity_schema,
  v.trigger_entity_name,
  v.trigger_name,
  %s,
  %s,
  %s,
  %s,
  t.%s,
  %s,
  v.offset_value,
  v.offset_unit,
  CASE v.offset_unit
    WHEN 'MINUTES' THEN t.%s - make_interval(mins => v.offset_value)
    WHEN 'HOURS' THEN t.%s - make_interval(hours => v.offset_value)
    WHEN 'DAYS' THEN t.%s - make_interval(days => v.offset_value)
    ELSE t.%s
  END,
  NULL,
  0,
  NULL
FROM %s AS t
JOIN (VALUES %s) AS v(trigger_schema, trigger_entity_schema, trigger_entity_name, trigger_name, offset_value, offset_unit) ON TRUE
WHERE t.%s = %s AND t.%s IS NOT NULL
ON CONFLICT (
  trigger_schema,
  trigger_entity_schema,
  trigger_entity_name,
  trigger_name,
  event_entity_schema,
  event_entity_name,
  row_id,
  field_name
) DO UPDATE SET
  offset_value = EXCLUDED.offset_value,
  offset_unit = EXCLUDED.offset_unit,
  due_at = EXCLUDED.due_at,
  locked_until = NULL,
  attempts = 0,
  last_error = NULL
"""
                    queueTableName
                    (SQL.renderSqlString (string eventEntity.Schema))
                    (SQL.renderSqlString (string eventEntity.Name))
                    (SQL.renderSqlString (string rootEntity.Schema))
                    (SQL.renderSqlString (string rootEntity.Name))
                    (SQL.renderSqlName "id")
                    (SQL.renderSqlString (string fieldName))
                    (columnName.ToSQLString())
                    (columnName.ToSQLString())
                    (columnName.ToSQLString())
                    (columnName.ToSQLString())
                    (rootTable.ToSQLString())
                    values
                    (SQL.renderSqlName "id")
                    (SQL.renderSqlInt rowId)
                    (columnName.ToSQLString())

            let! _ = query.ExecuteNonQuery q Map.empty cancellationToken
            return ()
    }

let scheduleRowTimeTriggers
    (query: QueryConnection)
    (layout: Layout)
    (triggers: MergedTriggers)
    (eventEntity: ResolvedEntityRef)
    (rowId: int)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        match layout.FindEntity eventEntity with
        | None -> return ()
        | Some entity ->
            let rootEntity = entity.Root
            let byField = findMergedTriggersTime eventEntity triggers

            do! queueDeleteByRootAndRow query rootEntity rowId cancellationToken

            for KeyValue(fieldName, mergedTriggers) in byField do
                let columnName = (Map.find fieldName entity.ColumnFields).ColumnName

                do!
                    enqueueSingleField
                        query
                        eventEntity
                        rootEntity
                        rowId
                        fieldName
                        columnName
                        mergedTriggers
                        cancellationToken
    }

let removeRowTimeTriggers
    (query: QueryConnection)
    (layout: Layout)
    (eventEntity: ResolvedEntityRef)
    (rowId: int)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        match layout.FindEntity eventEntity with
        | None -> return ()
        | Some entity -> return! queueDeleteByRootAndRow query entity.Root rowId cancellationToken
    }

let private parseClaimedTask (row: (SQL.SQLName * SQL.SimpleValueType * SQL.Value)[]) : ClaimedTimeTriggerTask =
    let get idx =
        let (_, _, value) = row.[idx]
        value

    let parseName idx =
        match get idx with
        | SQL.VString s -> OzmaQLName s
        | v -> failwithf "Invalid trigger queue name value: %O" v

    let parseInt idx =
        match get idx with
        | SQL.VInt i -> i
        | v -> failwithf "Invalid trigger queue int value: %O" v

    let parseInstant idx =
        match get idx with
        | SQL.VDateTime ts -> ts
        | v -> failwithf "Invalid trigger queue timestamp value: %O" v

    let triggerRef: TriggerRef =
        { Schema = parseName 1
          Entity =
            { Schema = parseName 2
              Name = parseName 3 }
          Name = parseName 4 }

    { Id = parseInt 0
      Trigger = triggerRef
      EventEntity =
        { Schema = parseName 5
          Name = parseName 6 }
      RootEntity =
        { Schema = parseName 7
          Name = parseName 8 }
      RowId = parseInt 9
      FieldName = parseName 10
      OffsetValue = parseInt 11
      OffsetUnit =
        match parseName 12 with
        | OzmaQLName "MINUTES" -> TTOUMinutes
        | OzmaQLName "HOURS" -> TTOUHours
        | OzmaQLName "DAYS" -> TTOUDays
        | OzmaQLName unit -> failwithf "Invalid time-trigger offset unit: %s" unit
      DueAt = parseInstant 13
      Attempts = parseInt 14 }

let tryClaimDueTimeTrigger
    (query: QueryConnection)
    (cancellationToken: CancellationToken)
    : Task<ClaimedTimeTriggerTask option> =
    task {
        let q =
            sprintf
                """
WITH claimed AS (
  SELECT id
  FROM %s
  WHERE due_at <= transaction_timestamp()
    AND (locked_until IS NULL OR locked_until < transaction_timestamp())
  ORDER BY due_at, id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE %s AS q
SET locked_until = transaction_timestamp() + INTERVAL '30 seconds',
    attempts = q.attempts + 1
FROM claimed
WHERE q.id = claimed.id
RETURNING
  q.id,
  q.trigger_schema,
  q.trigger_entity_schema,
  q.trigger_entity_name,
  q.trigger_name,
  q.event_entity_schema,
  q.event_entity_name,
  q.root_entity_schema,
  q.root_entity_name,
  q.row_id,
  q.field_name,
  q.offset_value,
  q.offset_unit,
  q.due_at,
  q.attempts
"""
                queueTableName
                queueTableName

        match! query.ExecuteRowValuesQuery q Map.empty cancellationToken with
        | None -> return None
        | Some row -> return Some(parseClaimedTask row)
    }

let completeClaimedTimeTrigger (query: QueryConnection) (taskId: int) (cancellationToken: CancellationToken) : Task =
    task {
        let q =
            sprintf "DELETE FROM %s WHERE id = %s" queueTableName (SQL.renderSqlInt taskId)

        let! _ = query.ExecuteNonQuery q Map.empty cancellationToken
        return ()
    }

let failClaimedTimeTrigger
    (query: QueryConnection)
    (taskId: int)
    (attempts: int)
    (error: string)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let retrySeconds = min 300 (max 5 (attempts * 5))
        let maxErrorLength = 3000

        let safeError =
            if String.IsNullOrEmpty(error) then "unknown"
            elif error.Length <= maxErrorLength then error
            else error.Substring(0, maxErrorLength)

        let q =
            sprintf
                """
UPDATE %s
SET due_at = transaction_timestamp() + make_interval(secs => %s),
    locked_until = NULL,
    last_error = %s
WHERE id = %s
"""
                queueTableName
                (SQL.renderSqlInt retrySeconds)
                (SQL.renderSqlString safeError)
                (SQL.renderSqlInt taskId)

        let! _ = query.ExecuteNonQuery q Map.empty cancellationToken
        return ()
    }
