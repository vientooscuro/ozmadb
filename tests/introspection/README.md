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
