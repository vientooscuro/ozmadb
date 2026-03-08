module OzmaDB.Outbox.Queue

open System
open System.Threading
open System.Threading.Tasks
open NodaTime

open OzmaDB.OzmaUtils
open OzmaDB.SQL.Query
open OzmaDB.Outbox.HTTP

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.Utils

[<NoComparison; NoEquality>]
type ClaimedOutboxMessage =
    { Id: int
      Method: string
      Url: string
      Headers: Map<string, string>
      Body: string option
      TimeoutMs: int option
      MaxRetries: int
      RetryBaseDelayMs: int
      Attempts: int }

let private outboxTableName = "public.outbox_messages"

let private parseHeaders (raw: string) : Map<string, string> =
    let parsed =
        Newtonsoft.Json.JsonConvert.DeserializeObject<Map<string, string>>(raw)

    if isRefNull parsed then Map.empty else parsed

let private parseClaimedOutboxMessage (row: (SQL.SQLName * SQL.SimpleValueType * SQL.Value)[]) : ClaimedOutboxMessage =
    let get idx =
        let (_, _, value) = row.[idx]
        value

    let parseString idx fieldName =
        match get idx with
        | SQL.VString s -> s
        | v -> failwithf "Invalid outbox %s value: %O" fieldName v

    let parseInt idx fieldName =
        match get idx with
        | SQL.VInt i -> i
        | v -> failwithf "Invalid outbox %s value: %O" fieldName v

    let parseOptionalInt idx fieldName =
        match get idx with
        | SQL.VInt i -> Some i
        | SQL.VNull -> None
        | v -> failwithf "Invalid outbox %s value: %O" fieldName v

    let parseOptionalString idx fieldName =
        match get idx with
        | SQL.VString s -> Some s
        | SQL.VNull -> None
        | v -> failwithf "Invalid outbox %s value: %O" fieldName v

    let parseHeadersValue idx =
        match get idx with
        | SQL.VJson j ->
            match j.Json with
            | :? Newtonsoft.Json.Linq.JObject as obj -> parseHeaders(obj.ToString(Newtonsoft.Json.Formatting.None))
            | _ -> Map.empty
        | SQL.VNull -> Map.empty
        | v -> failwithf "Invalid outbox headers value: %O" v

    { Id = parseInt 0 "id"
      Method = parseString 1 "method"
      Url = parseString 2 "url"
      Headers = parseHeadersValue 3
      Body = parseOptionalString 4 "body"
      TimeoutMs = parseOptionalInt 5 "timeout_ms"
      MaxRetries = parseInt 6 "max_retries"
      RetryBaseDelayMs = parseInt 7 "retry_base_delay_ms"
      Attempts = parseInt 8 "attempts" }

let tryClaimDueOutboxMessage
    (query: QueryConnection)
    (cancellationToken: CancellationToken)
    : Task<ClaimedOutboxMessage option> =
    task {
        let q =
            sprintf
                """
WITH claimed AS (
  SELECT m.id
  FROM %s AS m
  WHERE m.completed_at IS NULL
    AND m.due_at <= transaction_timestamp()
    AND (m.locked_until IS NULL OR m.locked_until < transaction_timestamp())
  ORDER BY m.due_at, m.id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE %s AS m
SET locked_until = transaction_timestamp() + INTERVAL '30 seconds',
    attempts = m.attempts + 1
FROM claimed AS c
WHERE m.id = c.id
RETURNING
  m.id,
  m.method,
  m.url,
  m.headers,
  m.body,
  m.timeout_ms,
  m.max_retries,
  m.retry_base_delay_ms,
  m.attempts
"""
                outboxTableName
                outboxTableName

        match! query.ExecuteRowValuesQuery q Map.empty cancellationToken with
        | None -> return None
        | Some row -> return Some(parseClaimedOutboxMessage row)
    }

let completeClaimedOutboxMessage
    (query: QueryConnection)
    (messageId: int)
    (statusCode: int)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let q =
            sprintf
                """
UPDATE %s
SET completed_at = transaction_timestamp(),
    locked_until = NULL,
    last_status_code = %s,
    last_error = NULL
WHERE id = %s
"""
                outboxTableName
                (SQL.renderSqlInt statusCode)
                (SQL.renderSqlInt messageId)

        let! _ = query.ExecuteNonQuery q Map.empty cancellationToken
        return ()
    }

let failClaimedOutboxMessage
    (query: QueryConnection)
    (message: ClaimedOutboxMessage)
    (statusCode: int option)
    (error: string)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let maxErrorLength = 3000

        let safeError =
            if String.IsNullOrWhiteSpace(error) then "unknown"
            elif error.Length <= maxErrorLength then error
            else error.Substring(0, maxErrorLength)

        let attemptsExceeded = message.Attempts > message.MaxRetries
        let retrySeconds = min 300 (max 5 ((max 1 message.RetryBaseDelayMs) * (pown 2 (message.Attempts - 1)) / 1000))

        let statusCodeSql =
            match statusCode with
            | Some code -> SQL.renderSqlInt code
            | None -> "NULL"

        let q =
            if attemptsExceeded then
                sprintf
                    """
UPDATE %s
SET completed_at = transaction_timestamp(),
    locked_until = NULL,
    last_status_code = %s,
    last_error = %s
WHERE id = %s
"""
                    outboxTableName
                    statusCodeSql
                    (SQL.renderSqlString safeError)
                    (SQL.renderSqlInt message.Id)
            else
                sprintf
                    """
UPDATE %s
SET due_at = transaction_timestamp() + make_interval(secs => %s),
    locked_until = NULL,
    last_status_code = %s,
    last_error = %s
WHERE id = %s
"""
                    outboxTableName
                    (SQL.renderSqlInt retrySeconds)
                    statusCodeSql
                    (SQL.renderSqlString safeError)
                    (SQL.renderSqlInt message.Id)

        let! _ = query.ExecuteNonQuery q Map.empty cancellationToken
        return ()
    }
