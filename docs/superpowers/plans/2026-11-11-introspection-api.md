# OzmaDB Introspection API (MCP backend) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give OzmaDB a root-only `/introspection/*` endpoint group with the primitives an AI-agent tool layer needs (FunQL validation, resolved schema, dependency graph, code search, JS syntax check, metadata dry-run, read-only SQL), and move the external MCP server (`~/PythonProjects/OzmaMCPExternal`) onto them so it becomes a thin adapter.

**Architecture:** New `API/Introspection.fs` (logic over `IContext`: in-memory layout, permissions, prefetched user views, JS sources) and `HTTP/Introspection.fs` (Giraffe routes, `utils.PerformReadJob`, `RTRoot` guard, never commits, so the per-request SERIALIZABLE transaction always rolls back). Metadata dry-run reuses `migrate()` split into validate/apply. The MCP keeps LLM-specific logic (formatting, trimming, guides, reports) and calls the new endpoints, falling back to the current client-side implementation when `/introspection/capabilities` is missing (older instances).

**Tech Stack:** F# / Giraffe 6.4 / Newtonsoft.Json / Npgsql 10 / ClearScript V8 (OzmaDB); Python 3 / httpx / pytest (MCP and the integration test suite).

**Spec:** none (user opted out of a design doc). Decisions come from the recon of both repos on 2026-11-11 and are recorded in "Decisions" below.

## Decisions

- Route prefix `/introspection/`. Every endpoint is root-only (`RTRoot`, same check as `checkIntegrity` in `OzmaDB/src/HTTP/Info.fs:85`); non-root gets 403 `RIAccessDenied`.
- Handlers never call commit. Validation failures are `200 {"ok": false, "errors": [...]}`, not HTTP errors: the caller is an agent that needs the details. HTTP errors only for bad requests, access denied and internal failures.
- Error item shape everywhere: `{ "message": string, "kind": string, "line": int | null, "column": int | null, "path": string | null }`.
- EXPLAIN is not reimplemented: `/views/anonymous/explain` and `/views/by_name/{s}/{n}/explain` already return compiled SQL + PG JSON plan. MCP just starts using them.
- Validation does **not** run the `LIMIT 0` dry-run (it executes SQL and pg_catalog functions are unrestricted). Resolve + compile only.
- Read-only SQL is wrapped as `SELECT * FROM (<sql>) AS __q LIMIT @max` inside a `READ ONLY` transaction with `statement_timeout`/`lock_timeout`. The wrapper makes multi-statement input, `COMMIT` and data-modifying CTEs a syntax error, so no own SQL parser is needed. Disabled unless config `ozmaDB:Introspection:AllowRawSql = true`.
- Metadata dry-run default mode `plan`: run operations, resolve layout/permissions/views/JS, compute DDL plan, do **not** execute DDL. Mode `full` additionally executes DDL and view dry-runs with `SET LOCAL lock_timeout = '2s'`, then rolls back.
- JS check is syntax-only (`V8ScriptEngine.Compile`, no `Evaluate`); module top-level code must never run.
- No new F# test project: endpoints are covered by a pytest integration suite in `tests/introspection/` hitting a local OzmaDB started with `disableSecurity: true` (anonymous = root).

## Global Constraints

- CI must stay green: `dotnet tool run fantomas --check .`, `dotnet restore --locked-mode`, `dotnet publish -c Release -r linux-x64 OzmaDB/OzmaDB.fsproj`. Run `dotnet tool run fantomas .` before every commit.
- New `.fs` files must be added to `OzmaDB/OzmaDB.fsproj` in compile order: API file after `src/API/Domains.fs` and before `src/API/API.fs`; HTTP file after `src/HTTP/Domains.fs`.
- Request/response records are public F# records (Newtonsoft does not serialize private types), camelCase on the wire like the rest of the API.
- No changes to existing endpoint behavior or to `IContext` members other than the additions named in tasks.
- Introspection endpoints must not populate `GetAnonymousView` LRU cache; use resolve functions directly.
- Commit messages: one short English imperative line, no body, no backticks, no Co-Authored-By.

## Review Focus

- Field reached through inheritance: a computed field or view on a child entity using a parent's column must appear in that column's dependencies (Task 5 test `test_dependencies_through_child_entity`).
- Catastrophic regex in `/introspection/search`: must time out per item and return `ok: false` with kind `regex_timeout`, not hang the request (Task 6 test `test_search_regex_timeout`).
- Raw SQL escape attempts: `select 1; commit`, `with d as (delete from ...) select 1`, `select pg_sleep(60)` must fail with an error item, never write or block past the timeout (Task 9 tests).
- Dry-run leaving state behind: after `/introspection/dry_run`, `public.state` version, `public.column_fields` and the in-memory layout are unchanged (Task 8 test `test_dry_run_leaves_no_trace`).
- Validation of a query with `WITH SUPERUSER ROLE` and of a query with `$arg` parameters: both validate without inserting into `public.user_views` (Task 3 tests).

---

## File Map

| File | Change |
|---|---|
| `OzmaDB/src/API/Introspection.fs` | create: `IntrospectionAPI` with all logic |
| `OzmaDB/src/HTTP/Introspection.fs` | create: routes, request/response records, root guard |
| `OzmaDB/src/API/Types.fs` | `IContext`: add `PgFunctions`, `ValidateMigration`; `IOzmaDBAPI`: add `Introspection` |
| `OzmaDB/src/API/API.fs` | wire `IntrospectionAPI` into `OzmaDBAPI` |
| `OzmaDB/src/API/ContextCache.fs` | split `migrate()` (1089-1525) into `validateMigration` + apply; expose new `IContext` members |
| `OzmaDB/src/Permissions/Resolve.fs` | keep `UsedDatabase` in resolved role rules (currently dropped at :164) |
| `OzmaDB/src/Permissions/Types.fs` | add `UsedDatabase` to `AllowedEntity` |
| `OzmaDB/src/JavaScript/Runtime.fs` | add `JSEngine.CheckSyntax` |
| `OzmaDB/src/Program.fs` | register `introspectionApi` in `appEndpoints` (:288); read `Introspection` config |
| `OzmaDB/OzmaDB.fsproj` | two `Compile Include` entries |
| `dev/config.introspection-test.json` | create: static instance, `disableSecurity: true` |
| `tests/introspection/*` | create: pytest suite |
| MCP `ozma_mcp/introspection.py` | create: thin client for `/introspection/*` with capability cache |
| MCP `ozma_mcp/server.py` | switch tools to the client with fallback; new tools |

---

## Part A. MCP fixes that need no OzmaDB changes

Repo: `/Users/vientooscuro/PythonProjects/OzmaMCPExternal`, tests with `pytest`.

### Task 1: MCP correctness fixes

**Files:** Modify `ozma_mcp/server.py`, `ozma_mcp/app.py`, `ozma_mcp/session.py`; Test `tests/test_server_fixes.py`

- [ ] **Step 1: Write failing tests**
  - `test_replace_count_zero_replaces_nothing`: safe-update helper with `replace_count=0` returns the source unchanged (today `replace_count or -1` at `server.py:4385` replaces all). Same for `reports.py`.
  - `test_list_entities_quotes_are_parameterized`: `list_entities(schema="a'b")` sends FunQL with a `$schema` argument, not an interpolated literal. Same for `list_entity_fields`, `search_field`, `query_events` (all filters).
  - `test_pagination_does_not_append_to_query`: `funql_query(query, limit=10, offset=5)` sends `__limit=10&__offset=5` (OzmaDB view flags) and leaves the query text untouched.
  - `test_http_mode_respects_readonly`: with env `OZMA_READONLY=true`, an HTTP-mode session refuses `transaction`.
  - `test_token_reused_between_requests`: two stateless HTTP calls with the same Ozma creds do one password grant (token cache keyed by sha256 of creds, honoring `expires_in`).
- [ ] **Step 2: Run** `pytest tests/test_server_fixes.py -v`, expect 5 failures.
- [ ] **Step 3: Implement.** Remove the duplicate `_user_view_id` (keep the one at :5791 semantics, delete :4871); module listing reads `public.modules (path)` only, without code.
- [ ] **Step 4: Run** the whole `pytest -q`, expect all pass.
- [ ] **Step 5: Commit** `Fix replace count, FunQL interpolation, readonly and token reuse`

### Task 2: EXPLAIN-based view performance analysis

**Files:** Modify `ozma_mcp/server.py` (`analyze_user_view_performance`, :3466); add tool `explain_funql`; Test `tests/test_explain.py`

**Interfaces:** Produces MCP tool `explain_funql(query: str | None, schema: str | None, name: str | None, args: dict | None, analyze: bool = false)` calling `POST views/anonymous/explain` (`{query, args, analyze}`) or `POST views/by_name/{schema}/{name}/explain`.

- [ ] **Step 1: Write failing tests** with a mocked HTTP layer: `test_explain_anonymous_posts_query`, `test_analyze_view_uses_explain_plan` (findings include `seq_scan` on a node with `Plan Rows > 10000`, `nested_loop_large`, `sort_without_index`, and the regex findings are kept only under `static_hints`).
- [ ] **Step 2: Run, expect fail.**
- [ ] **Step 3: Implement.** Plan walker over PG JSON (`Plan`, `Plans`, `Node Type`, `Plan Rows`, `Total Cost`, `Actual Total Time` when analyze). Return compiled SQL trimmed by `OZMA_MAX_CODE_CHARS`.
- [ ] **Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `Use OzmaDB explain for view performance analysis`

---

## Part B. OzmaDB introspection endpoints

Repo: `/Users/vientooscuro/SyncFolder/ozmadb`.

### Task 3: Integration test harness, endpoint skeleton, FunQL validate

**Files:**
- Create: `dev/config.introspection-test.json`, `tests/introspection/conftest.py`, `tests/introspection/test_validate.py`, `tests/introspection/README.md`, `OzmaDB/src/API/Introspection.fs`, `OzmaDB/src/HTTP/Introspection.fs`
- Modify: `OzmaDB/OzmaDB.fsproj`, `OzmaDB/src/Program.fs:288`, `OzmaDB/src/API/Types.fs:100-138,997`, `OzmaDB/src/API/API.fs`, `OzmaDB/src/API/ContextCache.fs:1670-1750`

**Interfaces:**
- Produces fixtures: `api` (httpx client, base URL env `OZMADB_TEST_URL`, default `http://localhost:5000`), `test_schema` (creates schema `mcp_test` with entities `parent(name string, amount int)`, `child(parent: inherits parent, extra string)`, `orders(parent_ref reference(mcp_test.parent), total decimal)` via `POST /transaction`, deletes it on teardown).
- Produces `IContext.PgFunctions` (the value already in `CachedState`).
- Produces `GET /introspection/capabilities` → `{ "version": 1, "features": ["validate", ...] }`; every later task appends its feature name.
- Produces `POST /introspection/funql/validate` body `{ query: string, homeSchema?: string }` → `{ ok, errors, columns: [{name, valueType}], arguments: [{name, argType, optional}], used: {schema: {entity: [field]}}, sql: string | null }`.

- [ ] **Step 1: Harness.** Config: static instance on local PG from `dev/setup.sh`, `disableSecurity: true`, `Introspection.AllowRawSql: true`. README: setup, `dotnet run --project OzmaDB -- dev/config.introspection-test.json`, `pytest tests/introspection`.
- [ ] **Step 2: Write failing tests** in `test_validate.py`:
  - `test_capabilities_lists_validate`
  - `test_validate_ok_returns_columns_and_used` (`select name, amount from mcp_test.parent` → columns `name`, `amount`; `used == {"mcp_test": {"parent": ["amount", "name"]}}`; `sql` contains `FROM`)
  - `test_validate_parse_error` (`select from where` → `ok false`, kind `parse`)
  - `test_validate_unknown_field` (`select nope from mcp_test.parent` → `ok false`, kind `resolve`, message contains `nope`)
  - `test_validate_arguments` (`{ $min int }: select name from mcp_test.parent where amount > $min` → arguments `[{name: "min", argType: "int", optional: false}]`)
  - `test_validate_superuser_role_has_no_side_effects` (query with `WITH SUPERUSER ROLE`; count of `public.user_views` unchanged)
- [ ] **Step 3: Run** `pytest tests/introspection/test_validate.py -v`, expect 404s.
- [ ] **Step 4: Implement.** `HTTP/Introspection.fs`: `introspectionApi : IServiceProvider -> Endpoint list`, private `requireRoot (api: IOzmaDBAPI) (f: unit -> Task<HttpJobResponse>)`, error DU `IntrospectionErrorInfo` implementing `IErrorDetails`. `API/Introspection.fs`: `ValidateQuery(query, homeSchema) : Task<ValidateResult>` calling `resolveAnonymousUserView` with `ctx.Layout`, `ctx.DefaultAttrs`, `ctx.PgFunctions`, `isPrivileged = true`, then `compileViewExpr`; `used` from `selectExprUsedReferences` on the resolved AST (not `FlatUsedDatabase`, which drops superuser-role entities). Map `UserViewResolveException`/`QueryResolveException`/parse errors to error items, extracting line/column when present.
- [ ] **Step 5: Run** tests, expect pass; `dotnet tool run fantomas .`; `dotnet build`.
- [ ] **Step 6: Commit** `Add introspection endpoints with FunQL validation`

### Task 4: Resolved schema

**Files:** Modify `API/Introspection.fs`, `HTTP/Introspection.fs`; Test `tests/introspection/test_schema.py`

**Interfaces:** Produces
- `GET /introspection/schema` → `[{ schema, entities: [{ name, parent: "s.e" | null, isAbstract, isFrozen }] }]`
- `GET /introspection/schema/{schema}/{entity}` → `{ name, parent, children: ["s.e"], mainField, columnFields: [{name, type, default, isNullable, isImmutable, inheritedFrom}], computedFields: [{name, expression, isVirtual, isMaterialized, inheritedFrom}], checkConstraints: [{name, expression}], uniqueConstraints: [{name, columns}], indexes: [{name, expressions, isUnique}], references: [{field, target}] }` with `type` as FunQL text (`reference(mcp_test.parent)`, `array(reference(...))`). Unknown entity → 404.

- [ ] **Step 1: Failing tests:** `test_schema_lists_test_entities`, `test_entity_includes_inherited_fields` (child has `name` with `inheritedFrom: "mcp_test.parent"`), `test_entity_children` (parent lists `mcp_test.child`), `test_entity_reference_type`, `test_entity_unknown_404`.
- [ ] **Step 2: Run, expect fail.**
- [ ] **Step 3: Implement** from `ctx.Layout` (`ResolvedEntity`), serializing types with the existing FunQL pretty-printer.
- [ ] **Step 4: Run, expect pass; fantomas; build.**
- [ ] **Step 5: Commit** `Add resolved schema introspection`

### Task 5: Dependency graph

**Files:** Modify `Permissions/Types.fs:22-47`, `Permissions/Resolve.fs:164`, `API/Introspection.fs`, `HTTP/Introspection.fs`; Test `tests/introspection/test_dependencies.py`

**Interfaces:** Produces `GET /introspection/dependencies?schema=&entity=&field=` (`field` optional: entity-level when absent) → `{ ok, items: [{ kind, schema, name, entity, detail, confidence }] }` where `kind ∈ computed_field | check_constraint | index | user_view | role_rule | main_field | action | trigger | module`, `confidence` is `resolved` for FunQL-derived items and `text` for JS matches, `detail` names the place (`select`, `check`, `update`, `delete` for role rules; `line N` for JS).

- [ ] **Step 1: Failing tests** (fixture adds computed field `parent.double_amount = amount * 2`, check `amount >= 0`, user view `mcp_test.parents_view` selecting `amount`, role `mcp_test_role` with select rule `amount > 0`, action whose source mentions `amount`):
  - `test_dependencies_finds_all_kinds` (all five kinds present for `parent.amount`)
  - `test_dependencies_through_child_entity` (computed field on `child` using `amount` appears under `parent.amount`)
  - `test_dependencies_unused_field_empty` (`orders.total` → `items == []`)
  - `test_dependencies_js_marked_text` (action item has `confidence: "text"`)
- [ ] **Step 2: Run, expect fail.**
- [ ] **Step 3: Implement.** Keep `UsedDatabase` on `AllowedEntity` (role select/update/delete/check combined). Build an inverted index `(schema, entity, field) → items` from layout computed fields and check constraints (stored `UsedDatabase`), indexes (`fieldExprUsedReferences`), `ctx.UserViews` (`selectExprUsedReferences` on `Resolved`), permissions; field references through a child entity are normalized to the entity that declares the field. JS: word-boundary match of the field name in action/trigger/module sources, line numbers. Cache the index keyed by `public.state` version.
- [ ] **Step 4: Run, expect pass; fantomas; build.**
- [ ] **Step 5: Commit** `Add field dependency introspection`

### Task 6: Code and metadata search

**Files:** Modify `API/Introspection.fs`, `HTTP/Introspection.fs`; Test `tests/introspection/test_search.py`

**Interfaces:** Produces `GET /introspection/search?q=&regex=false&caseSensitive=false&kinds=actions,triggers,modules,user_views,computed_fields,check_constraints,role_rules&limit=200` → `{ ok, errors, truncated, matches: [{ kind, schema, name, line, column, excerpt }] }`, all occurrences, excerpt = the matching line trimmed to 240 chars.

- [ ] **Step 1: Failing tests:** `test_search_plain_all_occurrences` (two hits in one action → two matches with distinct lines), `test_search_regex`, `test_search_kinds_filter`, `test_search_limit_truncates`, `test_search_regex_timeout` (`(a+)+$` on a long `aaaa…b` source → `ok false`, kind `regex_timeout`, response under 5 s).
- [ ] **Step 2: Run, expect fail.**
- [ ] **Step 3: Implement** over in-memory sources; .NET `Regex` with `matchTimeout = 500ms` per item, `RegexOptions.Multiline`.
- [ ] **Step 4: Run, expect pass; fantomas; build.**
- [ ] **Step 5: Commit** `Add code and metadata search introspection`

### Task 7: JS syntax check

**Files:** Modify `JavaScript/Runtime.fs` (near `CreateModule`, :748), `API/Introspection.fs`, `HTTP/Introspection.fs`; Test `tests/introspection/test_code_check.py`

**Interfaces:** Produces `JSEngine.CheckSyntax(source: string, isModule: bool) : Result<unit, JSSyntaxError>` (`JSSyntaxError = { Message: string; Line: int option; Column: int option }`) and `POST /introspection/code/check` body `{ kind: "action" | "trigger" | "module", source: string }` → `{ ok, errors }`.

- [ ] **Step 1: Failing tests:** `test_check_valid_action`, `test_check_syntax_error_has_line` (error on line 3 → `line == 3`), `test_check_module_top_level_not_executed` (module `throw new Error("ran")` at top level → `ok true`), `test_check_module_with_import`.
- [ ] **Step 2: Run, expect fail.**
- [ ] **Step 3: Spike, then implement.** Verify that `V8ScriptEngine.Compile(DocumentInfo { Category = ModuleCategory.Standard }, source)` parses without evaluating and does not resolve imports. If ClearScript cannot compile a module without instantiation, fall back to wrapping module source as a function body for syntax only after stripping `import`/`export` statements, and record that limitation in the response as `warnings: ["module checked without import resolution"]`. Use a dedicated short-lived engine, not the request pool.
- [ ] **Step 4: Run, expect pass; fantomas; build.**
- [ ] **Step 5: Commit** `Add JS syntax check introspection`

### Task 8: Metadata dry-run

**Files:** Modify `API/ContextCache.fs:1089-1525`, `API/Types.fs` (`IContext.ValidateMigration`), `API/Introspection.fs`, `HTTP/Introspection.fs`; Test `tests/introspection/test_dry_run.py`

**Interfaces:**
- Produces `IContext.ValidateMigration(mode: DryRunMode) : Task<MigrationValidation>` where `DryRunMode = DRPlan | DRFull` and `MigrationValidation = { Ok: bool; Errors: ErrorItem list; Ddl: string list; BrokenObjects: string list }`.
- Produces `POST /introspection/dry_run` body = `/transaction` body plus `mode?: "plan" | "full"` → `{ ok, errors, results, ddl, brokenObjects }`.

- [ ] **Step 1: Failing tests:**
  - `test_dry_run_add_column_returns_ddl` (insert into `public.column_fields` → `ddl` has one `ALTER TABLE ... ADD COLUMN`)
  - `test_dry_run_broken_computed_reports_error` (computed field with unknown column → `ok false`, message names the field)
  - `test_dry_run_drop_used_column_reports_dependents` (delete `amount` column → errors mention `double_amount` and `parents_view`)
  - `test_dry_run_leaves_no_trace` (state version, column list and `GET /introspection/schema/mcp_test/parent` identical before/after, for both modes)
  - `test_dry_run_full_mode_runs_view_dry_run` (view broken only at SQL execution level is reported in `full`, not in `plan`)
- [ ] **Step 2: Run, expect fail.**
- [ ] **Step 3: Refactor `migrate()`** into `validateMigration mode` (steps: advisory lock, build + resolve layout, user-defined DDL, `planDatabaseMigration`, assertions, JS resolve/load, `prepareActions`/`prepareTriggers`, resolve user views and permissions; in `DRFull` also apply DDL with `SET LOCAL lock_timeout = '2s'` and run `dryRunUserViews`) and the existing apply path that calls it and then commits and swaps `cachedState`. Existing `migrate()` behavior must stay byte-for-byte equivalent for normal commits. `GEMigrationConflict` in dry-run maps to error kind `migration_conflict`.
- [ ] **Step 4: Implement endpoint:** run transaction operations through the existing transaction API, then `ValidateMigration`, never commit.
- [ ] **Step 5: Run** the dry-run tests and the whole suite (regression for `migrate()`), plus manual smoke: create and delete a column via normal `POST /transaction` still works.
- [ ] **Step 6: fantomas; build; commit** `Add metadata dry-run introspection`

### Task 9: Read-only SQL and Postgres diagnostics

**Files:** Modify `API/Introspection.fs`, `HTTP/Introspection.fs`, `Program.fs` (config `ozmaDB:Introspection:AllowRawSql`, default `false`); Test `tests/introspection/test_sql.py`

**Interfaces:** Produces
- `POST /introspection/sql` body `{ sql: string, params?: object[], timeoutMs?: int (default 5000, max 30000), maxRows?: int (default 500, max 5000) }` → `{ ok, errors, columns: [{name, pgType}], rows: [[...]], truncated }`. Disabled → 403 with message `raw SQL is disabled for this instance`.
- `GET /introspection/pg/{report}` with `report ∈ table_sizes | locks | activity | statements | indexes_unused`, canned queries; `statements` returns `ok false`, kind `extension_missing` when `pg_stat_statements` is absent.

- [ ] **Step 1: Failing tests:** `test_sql_select`, `test_sql_params`, `test_sql_rejects_commit` (`select 1; commit`), `test_sql_rejects_modifying_cte`, `test_sql_rejects_insert`, `test_sql_timeout` (`select pg_sleep(60)` with `timeoutMs: 500` → `ok false`, kind `timeout`, response under 3 s), `test_sql_max_rows_truncates`, `test_pg_table_sizes_lists_test_tables`, `test_pg_statements_missing_extension_is_reported`.
- [ ] **Step 2: Run, expect fail.**
- [ ] **Step 3: Implement:** `SET TRANSACTION READ ONLY`, `SET LOCAL statement_timeout`, `SET LOCAL lock_timeout = '1s'`, then `SELECT * FROM (<sql>) AS __q LIMIT <maxRows + 1>` via `QueryConnection.ExecuteQuery`; trailing semicolons and whitespace are stripped before wrapping. Values serialized as JSON scalars, unknown types as text.
- [ ] **Step 4: Run, expect pass; fantomas; build.**
- [ ] **Step 5: Commit** `Add read-only SQL and Postgres diagnostics introspection`

---

## Part C. MCP on top of the introspection API

Repo: `/Users/vientooscuro/PythonProjects/OzmaMCPExternal`. Unit tests mock HTTP; one opt-in integration test file runs against the harness from Task 3 when `OZMADB_TEST_URL` is set.

### Task 10: Introspection client with capability detection

**Files:** Create `ozma_mcp/introspection.py`; Test `tests/test_introspection_client.py`

**Interfaces:** Produces `class IntrospectionClient(session: OzmaSession)` with `async has(feature: str) -> bool` (reads `GET introspection/capabilities` once per session token, 404 → empty set) and one async method per endpoint: `validate`, `schema`, `entity`, `dependencies`, `search`, `check_code`, `dry_run`, `sql`, `pg_report`, each returning the parsed JSON dict.

- [ ] **Step 1: Failing tests:** `test_has_feature_true`, `test_old_instance_has_nothing` (404), `test_capabilities_fetched_once`.
- [ ] **Step 2: Run, expect fail. Step 3: Implement. Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `Add OzmaDB introspection client`

### Task 11: Switch existing tools to server-side primitives

**Files:** Modify `ozma_mcp/server.py`, `ozma_mcp/reports.py` (`safe_update_report_query`); Test `tests/test_tools_introspection.py`

Mapping (old client-side path stays as fallback when `has(feature)` is false):

| Tool | New path |
|---|---|
| `validate_funql`, validation inside `create_user_view`, `safe_update_view_query`, `safe_update_report_query` | `validate`; temporary user view insertion removed when available |
| `list_schemas`, `list_entities`, `list_entity_fields`, `search_field` | `schema`, `entity` |
| `upsert_computed_field` | `entity` for hierarchy conflicts + `dry_run` instead of retry on clash text |
| `where_used_field` | `dependencies` |
| `delete_column_field` | `dependencies` guard: refuse when non-empty unless `force=true` |
| `search_in_actions/triggers/modules/all/metadata`, `search_js_api_usage`, `search_http_api_usage` | `search` (API usage = regex over `(OzmaDB|FunDB)\.(name)\s*\(`) |
| `safe_update_action_function/trigger_function/module` | `check_code` on the patched source before writing; `dry_run=true` uses `dry_run` endpoint |
| `upsert_column_field`, `upsert_unique_constraint`, `upsert_role_entity`, `create_*` | new `dry_run: bool` param routed to `dry_run` endpoint, returns `ddl` |

- [ ] **Step 1: Failing tests:** per row, one test asserting the new endpoint is called when the feature is present and the old FunQL path when absent (e.g. `test_where_used_uses_dependencies`, `test_where_used_fallback`, `test_safe_update_action_checks_syntax`, `test_delete_column_refuses_when_used`, `test_upsert_column_dry_run_returns_ddl`).
- [ ] **Step 2: Run, expect fail. Step 3: Implement. Step 4: Run whole suite, expect pass.**
- [ ] **Step 5: Commit** `Use OzmaDB introspection in MCP tools with fallback`

### Task 12: New MCP tools

**Files:** Modify `ozma_mcp/server.py`, `README.md` tool tables; Test `tests/test_new_tools.py`

**Interfaces:** Produces tools `dry_run_transaction(operations, mode="plan")`, `check_js(kind, source)`, `sql_query(sql, params=None, timeout_ms=5000, max_rows=500)` (registered only when `has("sql")`; absent in `OZMA_READONLY` mode is not required since the endpoint is read-only), `pg_report(report)`.

- [ ] **Step 1: Failing tests:** `test_new_tools_hidden_on_old_instance`, `test_sql_query_passes_params`, `test_pg_report_names_validated` (unknown report rejected client-side).
- [ ] **Step 2: Run, expect fail. Step 3: Implement; update README tables. Step 4: Run, expect pass.**
- [ ] **Step 5: Commit** `Add dry-run, JS check, SQL and Postgres report tools`

### Task 13: End-to-end check

- [ ] Start OzmaDB with `dev/config.introspection-test.json`, run `pytest tests/introspection -v` in ozmadb and `OZMADB_TEST_URL=http://localhost:5000 pytest tests -v` in MCP; both green.
- [ ] Run the CI commands from Global Constraints in ozmadb; all pass.
- [ ] Against a staging instance (not production), call each new MCP tool once through Claude Code and note any response that is too large or unclear for an agent; fix trimming in MCP only.

## Out of scope

- Runtime metrics for actions/triggers (replacing regex heuristics in `analyze_action/trigger/module_performance`): needs tracing in the JS runtime, separate plan.
- Natural-key upsert in `/transaction` and a parameterized `/events` API.
- Embedding an MCP endpoint into OzmaDB itself; changes to `ozmadb-js`/`ozmadb-py` clients.
- Report generator changes (`search_in_report_templates` N+1).
