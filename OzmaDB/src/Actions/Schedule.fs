module OzmaDB.Actions.Schedule

open System
open System.Threading
open System.Threading.Tasks
open Newtonsoft.Json.Linq
open NodaTime

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.AST
open OzmaDB.Actions.Types
open OzmaDB.SQL.Query

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.Utils

[<NoEquality; NoComparison>]
type ClaimedActionSchedule =
    { Id: int
      Attempts: int
      DueAt: Instant
      ScheduleType: string
      Action: ActionRef
      Args: JObject }

let private schedulesTableName = "public.action_schedules"

let private parseClaimedActionSchedule
    (row: (SQL.SQLName * SQL.SimpleValueType * SQL.Value)[])
    : ClaimedActionSchedule =
    let get idx =
        let (_, _, value) = row.[idx]
        value

    let parseName idx =
        match get idx with
        | SQL.VString s -> OzmaQLName s
        | v -> failwithf "Invalid action schedule name value: %O" v

    let parseInt idx =
        match get idx with
        | SQL.VInt i -> i
        | v -> failwithf "Invalid action schedule int value: %O" v

    let parseInstant idx =
        match get idx with
        | SQL.VDateTime ts -> ts
        | v -> failwithf "Invalid action schedule timestamp value: %O" v

    let parseArgs idx =
        match get idx with
        | SQL.VJson j ->
            match j.Json with
            | :? JObject as obj -> obj
            | _ -> JObject()
        | SQL.VNull -> JObject()
        | v -> failwithf "Invalid action schedule args value: %O" v

    { Id = parseInt 0
      Action =
        { Schema = parseName 1
          Name = parseName 2 }
      DueAt = parseInstant 3
      Attempts = parseInt 4
      Args = parseArgs 5
      ScheduleType =
        match parseName 6 with
        | OzmaQLName s -> s }

let tryClaimDueActionSchedule
    (query: QueryConnection)
    (cancellationToken: CancellationToken)
    : Task<ClaimedActionSchedule option> =
    task {
        let q =
            sprintf
                """
WITH due AS (
  SELECT
    s.id,
    s.schema_id,
    s.schedule_type,
    CASE
      WHEN s.schedule_type = 'ONCE' THEN s.run_at
      ELSE date_trunc('day', transaction_timestamp()) + make_interval(hours => s.hour, mins => s.minute)
    END AS due_at
  FROM %s AS s
  WHERE s.is_enabled
    AND (s.locked_until IS NULL OR s.locked_until < transaction_timestamp())
),
claimed AS (
  SELECT d.id, d.schema_id, d.schedule_type, d.due_at
  FROM due AS d
  JOIN %s AS s ON s.id = d.id
  WHERE d.due_at IS NOT NULL
    AND d.due_at <= transaction_timestamp()
    AND (
      (d.schedule_type = 'ONCE' AND s.last_run_at IS NULL)
      OR (d.schedule_type = 'DAILY' AND (s.last_run_at IS NULL OR s.last_run_at < d.due_at))
    )
  ORDER BY d.due_at, d.id
  LIMIT 1
  FOR UPDATE OF s SKIP LOCKED
)
UPDATE %s AS s
SET locked_until = transaction_timestamp() + INTERVAL '30 seconds',
    attempts = s.attempts + 1
FROM claimed AS c
JOIN public.schemas AS sch ON sch.id = c.schema_id
WHERE s.id = c.id
RETURNING
  s.id,
  sch.name,
  s.action_name,
  c.due_at,
  s.attempts,
  s.args,
  s.schedule_type
"""
                schedulesTableName
                schedulesTableName
                schedulesTableName

        match! query.ExecuteRowValuesQuery q Map.empty cancellationToken with
        | None -> return None
        | Some row -> return Some(parseClaimedActionSchedule row)
    }

let completeClaimedActionSchedule
    (query: QueryConnection)
    (scheduleId: int)
    (dueAt: Instant)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let q =
            sprintf
                """
UPDATE %s
SET last_run_at = @0,
    is_enabled = CASE WHEN schedule_type = 'ONCE' THEN FALSE ELSE is_enabled END,
    locked_until = NULL,
    attempts = 0,
    last_error = NULL
WHERE id = %s
"""
                schedulesTableName
                (SQL.renderSqlInt scheduleId)

        let! _ = query.ExecuteNonQuery q (Map.singleton 0 (SQL.VDateTime dueAt)) cancellationToken
        return ()
    }

let failClaimedActionSchedule
    (query: QueryConnection)
    (scheduleId: int)
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
SET locked_until = transaction_timestamp() + make_interval(secs => %s),
    last_error = %s
WHERE id = %s
"""
                schedulesTableName
                (SQL.renderSqlInt retrySeconds)
                (SQL.renderSqlString safeError)
                (SQL.renderSqlInt scheduleId)

        let! _ = query.ExecuteNonQuery q Map.empty cancellationToken
        return ()
    }
