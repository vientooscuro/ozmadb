# Introspection API integration tests

These tests exercise the root-only `/introspection/*` endpoints against a locally running OzmaDB.

## Setup

1. Create the test role and database (idempotent; extra arguments go to `psql`):

   ```sh
   tests/introspection/setup_db.sh -h /tmp
   ```

2. Create the virtualenv:

   ```sh
   python3 -m venv tests/introspection/.venv
   tests/introspection/.venv/bin/pip install -r tests/introspection/requirements.txt
   ```

## Running

Start OzmaDB with the test config (listens on `http://127.0.0.1:5099`, security disabled):

```sh
tests/introspection/run_server.sh --background
```

The project itself is pinned to a self-contained `linux-x64` build, so `dotnet run` does not work on other hosts.
The script detects the host RID (`osx-arm64`, `osx-x64`, `linux-x64`) and builds a framework-dependent copy into
`tests/introspection/.server/` (gitignored). That build rewrites `OzmaDB/packages.lock.json`,
`OzmaDBSchema/packages.lock.json` and `OzmaUtils/packages.lock.json`; the script restores them afterwards.
On macOS it also downloads `microsoft.clearscript.v8.native.<rid>` from nuget.org, pinned to the ClearScript V8
version resolved in `OzmaDB/packages.lock.json`, and puts the native library next to the binary (cached after the
first run). Without `--background` the server runs in the foreground; `--stop` stops a background server.

Then run the tests and stop the server:

```sh
tests/introspection/.venv/bin/pytest tests/introspection
tests/introspection/run_server.sh --stop
```

Set `OZMADB_TEST_URL` to target another server. The suite is skipped if the server is unreachable.
The `test_schema` fixture creates the `mcp_test` schema and removes it on teardown.

`dev/config.introspection-test.json` sets `ozmaDB.introspection.allowFullDryRun` to `true`, so the `mode: "full"`
dry-run tests can run. With the default (`false`), `POST /introspection/dry_run` with `mode: "full"` returns 403
(`full dry-run is disabled for this instance`); that path is not covered by the suite because it needs a server
started with a different config.

The config also sets `ozmaDB.introspection.allowRawSql` to `true` for the `POST /introspection/sql` tests. With the
default (`false`) the endpoint returns 403 (`raw SQL is disabled for this instance`). Raw SQL runs in a read-only
transaction that is never committed, but it is still refused with 403 (`raw SQL is unavailable: database has
dblink/postgres_fdw or a privileged role`) when the database has `dblink` or `postgres_fdw` installed, or the database
role is a member of a superuser, replication, BYPASSRLS, `pg_*_server_files`, `pg_execute_server_program` or
`pg_signal_backend` role.
That is why the harness role created by `setup_db.sh` must stay a plain non-superuser owner. The tests that install
these extensions or grant such a role connect as `postgres` and are skipped when that is not possible.
Residual risk with raw SQL enabled: root can read anything the database role can, and call functions allowed in
read-only mode, such as `pg_terminate_backend`/`pg_cancel_backend` on other sessions of the same role.

The raw SQL response is capped at 16 MB of text, where every cell (NULL included) also counts as 16 characters;
past the cap the result is truncated with a `result_too_large` error. json/jsonb values longer than 64 KB are not
parsed: they are returned as strings, and their column gets `"jsonAsText": true`.

## Dry run side effects

`POST /introspection/dry_run` runs the operations, including triggers, inside the request transaction and rolls it
back. Effects outside that transaction are disabled for the dry-run request: `OzmaDB.httpRequest` throws
`HTTP requests are disabled during dry run`, `OzmaDB.writeEvent` and API error event logging are no-ops (they write
on a separate connection). `OzmaDB.writeEventSync` and `OzmaDB.enqueueHttpRequest` insert into the request
transaction, so they are rolled back with it. `test_dry_run_disables_trigger_side_effects` covers this.

`GET /introspection/schema/{schema}/{entity}` returns computed fields and check constraints with `expression` set to
the stored source text and `resolvedExpression` to the resolved rendering. Broken computed fields (`allow_broken`)
are listed too, with `isBroken: true`, `error` and `resolvedExpression: null`.
