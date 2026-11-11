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
dotnet run --project OzmaDB -- dev/config.introspection-test.json
```

The project is pinned to a self-contained `linux-x64` build. On other hosts (e.g. macOS arm64) build a
framework-dependent copy instead, revert the lock files it rewrites, and put the matching
`ClearScriptV8.<rid>` native library (from the `Microsoft.ClearScript.V8.Native.<rid>` package) next to the binary:

```sh
dotnet build OzmaDB/OzmaDB.fsproj -r osx-arm64 -p:SelfContained=false -p:PublishReadyToRun=false -o /path/to/out
git checkout -- '*/packages.lock.json'
/path/to/out/OzmaDB dev/config.introspection-test.json
```

Then, in another shell:

```sh
tests/introspection/.venv/bin/pytest tests/introspection
```

Set `OZMADB_TEST_URL` to target another server. The suite is skipped if the server is unreachable.
The `test_schema` fixture creates the `mcp_test` schema and removes it on teardown.
