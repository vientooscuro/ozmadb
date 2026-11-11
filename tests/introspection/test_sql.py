import base64
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone

import pytest

from test_dry_run import sql as psql

PROBE_TABLE = "public.mcp_sql_probe"
PROBE_WRITE = "public.mcp_sql_probe_write"


@pytest.fixture(scope="module")
def probe():
    """A plain table and a function writing to it, created behind OzmaDB's back."""
    psql(
        f"create table if not exists {PROBE_TABLE} (x int); truncate {PROBE_TABLE}; "
        f"create or replace function {PROBE_WRITE}() returns int language sql "
        f"as $$ insert into {PROBE_TABLE} values (1) returning x $$"
    )
    yield
    psql(f"drop function if exists {PROBE_WRITE}(); drop table if exists {PROBE_TABLE}")


def probe_count() -> int:
    return int(psql(f"select count(*) from {PROBE_TABLE}").strip())


def run_sql(api, query: str, **extra) -> dict:
    resp = api.post("/introspection/sql", json={"sql": query, **extra})
    assert resp.status_code == 200, resp.text
    return resp.json()


def assert_failed(ret: dict, kind: str | None = None) -> dict:
    assert ret["ok"] is False, ret
    assert ret["rows"] == []
    error = ret["errors"][0]
    assert set(error) == {"message", "kind", "line", "column", "path"}
    if kind is not None:
        assert error["kind"] == kind, error
    return error


def test_capabilities_lists_sql_and_pg_reports(api):
    features = api.get("/introspection/capabilities").json()["features"]
    assert "sql" in features
    assert "pg_reports" in features


def test_sql_select(api):
    ret = run_sql(api, "select 1 as one, 'a'::text as s, true as b, null::int as n;  \n")
    assert ret["ok"] is True
    assert ret["errors"] == []
    assert ret["columns"] == [
        {"name": "one", "pgType": "integer"},
        {"name": "s", "pgType": "text"},
        {"name": "b", "pgType": "boolean"},
        {"name": "n", "pgType": "integer"},
    ]
    assert ret["rows"] == [[1, "a", True, None]]
    assert ret["truncated"] is False


def test_sql_value_serialization(api):
    ret = run_sql(
        api,
        "select '{\"a\": [1, 2]}'::jsonb as j, '\\x0102ff'::bytea as b, "
        "'2024-05-06 07:08:09.5+00'::timestamptz as ts, '2024-05-06 07:08:09'::timestamp as lts, "
        "'2024-05-06'::date as d, 12.50::numeric as num, array[1, 2] as arr, 'NaN'::float8 as nan",
    )
    assert ret["ok"] is True, ret
    [row] = ret["rows"]
    assert row[0] == {"a": [1, 2]}
    assert base64.b64decode(row[1]) == b"\x01\x02\xff"
    assert datetime.fromisoformat(row[2]) == datetime(2024, 5, 6, 7, 8, 9, 500000, tzinfo=timezone.utc)
    assert row[3] == "2024-05-06T07:08:09"
    assert row[4] == "2024-05-06"
    assert row[5] == 12.5
    assert row[6] == "{1,2}"
    assert row[7] == "NaN"


def test_sql_params(api):
    ret = run_sql(api, "select $1::int + 1 as x, $2::text as s", params=[41, "hi"])
    assert ret["ok"] is True, ret
    assert ret["rows"] == [[42, "hi"]]


def test_sql_param_types(api):
    ret = run_sql(
        api,
        "select $1::text, $2::bigint, $3::numeric, $4::boolean, $5::int is null, $6::bigint",
        params=["s", 5, 2.5, True, None, 9223372036854775807],
    )
    assert ret["ok"] is True, ret
    assert ret["rows"] == [["s", 5, 2.5, True, True, 9223372036854775807]]

    for bad in ([1, [2]], [{"a": 1}]):
        resp = api.post("/introspection/sql", json={"sql": "select 1", "params": bad})
        assert resp.status_code == 400, resp.text


def test_sql_validates_body(api):
    for body in (
        {"sql": ""},
        {"sql": "  ;  "},
        {"sql": "select 1", "timeoutMs": 0},
        {"sql": "select 1", "timeoutMs": 30001},
        {"sql": "select 1", "maxRows": 0},
        {"sql": "select 1", "maxRows": 5001},
    ):
        resp = api.post("/introspection/sql", json=body)
        assert resp.status_code == 400, (body, resp.text)


def test_sql_rejects_commit(api, probe):
    assert_failed(run_sql(api, "select 1; commit"))
    # Breaking out of the wrapping subquery must not produce several statements.
    error = assert_failed(
        run_sql(api, f"select 1) as x; commit; insert into {PROBE_TABLE} values (1); select * from (select 1"),
        "syntax",
    )
    assert "multiple commands" in error["message"]
    assert probe_count() == 0


def test_sql_rejects_modifying_cte(api, probe):
    assert_failed(run_sql(api, f"with w as (insert into {PROBE_TABLE} values (1) returning x) select * from w"))
    assert_failed(run_sql(api, f"with w as (select {PROBE_WRITE}() as x) select * from w"), "read_only")
    assert probe_count() == 0


def test_sql_rejects_insert(api, probe):
    assert_failed(run_sql(api, f"insert into {PROBE_TABLE} values (1)"))
    error = assert_failed(run_sql(api, f"select {PROBE_WRITE}()"), "read_only")
    assert "read-only" in error["message"]
    assert probe_count() == 0


def test_sql_rejects_set_transaction_read_write(api, probe):
    assert_failed(run_sql(api, "select 1; set transaction read write"))
    assert_failed(run_sql(api, "select set_config('transaction_read_only', 'off', true)"))
    assert_failed(
        run_sql(
            api,
            "with s as (select set_config('transaction_read_only', 'off', true) as v) "
            f"select {PROBE_WRITE}() from s",
        )
    )
    assert probe_count() == 0


def test_sql_rejects_copy_program(api):
    assert_failed(run_sql(api, "copy (select 1) to program 'true'"))
    error = assert_failed(
        run_sql(api, "select 1) as x; copy (select 1) to program 'true'; select * from (select 1"),
        "syntax",
    )
    assert "multiple commands" in error["message"]


def test_sql_timeout(api):
    start = time.monotonic()
    ret = run_sql(api, "select pg_sleep(60)", timeoutMs=500)
    assert time.monotonic() - start < 3
    assert_failed(ret, "timeout")


def test_sql_syntax_error(api):
    assert_failed(run_sql(api, "selec 1"), "syntax")


def test_sql_after_error_returns_cleanly(api):
    assert_failed(run_sql(api, "select 1/0"), "sql")
    ret = run_sql(api, "select current_setting('transaction_read_only') as ro, 2 as x")
    assert ret["ok"] is True, ret
    assert ret["rows"] == [["on", 2]]
    # The previous request's settings must not leak into ordinary requests on a pooled connection.
    assert api.get("/introspection/schema").status_code == 200


def test_sql_max_rows_truncates(api):
    ret = run_sql(api, "select generate_series(1, 10) as n", maxRows=3)
    assert ret["ok"] is True
    assert ret["rows"] == [[1], [2], [3]]
    assert ret["truncated"] is True

    ret = run_sql(api, "select generate_series(1, 10) as n", maxRows=10)
    assert len(ret["rows"]) == 10
    assert ret["truncated"] is False


def test_sql_result_size_capped(api):
    start = time.monotonic()
    ret = run_sql(api, "select repeat('x', 2000000) as v from generate_series(1, 20)", timeoutMs=30000)
    assert time.monotonic() - start < 15
    assert ret["ok"] is True, ret["errors"]
    assert ret["truncated"] is True
    assert [e["kind"] for e in ret["errors"]] == ["result_too_large"]
    assert 0 < len(ret["rows"]) < 20
    for (value,) in ret["rows"]:
        assert value.endswith("…[truncated]")
        assert len(value) <= 1024 * 1024 + len("…[truncated]")
    assert sum(len(v) for (v,) in ret["rows"]) <= 17 * 1024 * 1024


def test_sql_deep_json_falls_back_to_text(api):
    ret = run_sql(api, "select repeat('[', 100) || repeat(']', 100) as j")
    assert ret["ok"] is True, ret
    ret = run_sql(api, "select (repeat('[', 100) || repeat(']', 100))::jsonb as j")
    assert ret["ok"] is True, ret
    assert ret["rows"] == [["[" * 100 + "]" * 100]]


def test_sql_null_cells_count_toward_result_size(api):
    columns = ", ".join(f"null::int as c{i}" for i in range(300))
    ret = run_sql(api, f"select {columns} from generate_series(1, 5000)", maxRows=5000, timeoutMs=30000)
    assert ret["ok"] is True, ret["errors"]
    assert ret["truncated"] is True
    assert [e["kind"] for e in ret["errors"]] == ["result_too_large"]
    assert 0 < len(ret["rows"]) < 5000


def test_sql_large_json_returned_as_text(api):
    ret = run_sql(api, "select jsonb_build_object('a', repeat('x', 70000)) as big, '{\"a\": 1}'::jsonb as small")
    assert ret["ok"] is True, ret["errors"]
    assert ret["columns"] == [
        {"name": "big", "pgType": "jsonb", "jsonAsText": True},
        {"name": "small", "pgType": "jsonb"},
    ]
    [[big, small]] = ret["rows"]
    assert isinstance(big, str) and big.startswith('{"a": "xxx')
    assert small == {"a": 1}


def superuser_psql(query: str) -> None:
    if shutil.which("psql") is None:
        pytest.skip("psql is required")
    db = "ozmadb_introspection_test"
    out = subprocess.run(
        ["psql", "-h", "localhost", "-p", "5432", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", query],
        env={**os.environ},
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        pytest.skip(f"cannot run psql as postgres: {out.stderr.strip()}")


@pytest.mark.parametrize("extension", ["dblink", "postgres_fdw"])
def test_sql_unavailable_with_cross_connection_extension(api, extension):
    superuser_psql(f"create extension if not exists {extension}")
    try:
        resp = api.post("/introspection/sql", json={"sql": "select 1"})
        assert resp.status_code == 403, resp.text
        assert resp.json()["message"] == "raw SQL is unavailable: database has dblink/postgres_fdw or a privileged role"
        # Canned reports stay available.
        assert pg_report(api, "activity")["ok"] is True
    finally:
        superuser_psql(f"drop extension if exists {extension}")
    assert run_sql(api, "select 1")["ok"] is True


def test_sql_unavailable_for_member_of_privileged_role(api):
    superuser_psql(
        "drop role if exists mcp_sql_bypass; create role mcp_sql_bypass nologin bypassrls; "
        "grant mcp_sql_bypass to ozmadb_introspection_test"
    )
    try:
        resp = api.post("/introspection/sql", json={"sql": "select 1"})
        assert resp.status_code == 403, resp.text
    finally:
        superuser_psql("drop role if exists mcp_sql_bypass")
    assert run_sql(api, "select 1")["ok"] is True


def test_sql_unavailable_for_member_of_pg_signal_backend(api):
    superuser_psql("grant pg_signal_backend to ozmadb_introspection_test")
    try:
        resp = api.post("/introspection/sql", json={"sql": "select 1"})
        assert resp.status_code == 403, resp.text
    finally:
        superuser_psql("revoke pg_signal_backend from ozmadb_introspection_test")
    assert run_sql(api, "select 1")["ok"] is True


def pg_report(api, name: str) -> dict:
    resp = api.get(f"/introspection/pg/{name}")
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_pg_table_sizes_lists_test_tables(api, test_schema):
    ret = pg_report(api, "table_sizes")
    assert ret["ok"] is True, ret
    names = [c["name"] for c in ret["columns"]]
    assert names == ["schema", "table", "total_bytes", "table_bytes", "index_bytes"]
    tables = {(row[0], row[1]) for row in ret["rows"]}
    assert (test_schema, "parent") in tables
    assert not any(schema in ("pg_catalog", "information_schema") for schema, _ in tables)


def test_pg_locks_only_current_database(api):
    ret = pg_report(api, "locks")
    assert ret["ok"] is True, ret
    names = [c["name"] for c in ret["columns"]]
    assert "datname" in names
    [[current]] = run_sql(api, "select current_database()")["rows"]
    datnames = [row[names.index("datname")] for row in ret["rows"]]
    # The request itself holds locks, so the report is never empty.
    assert datnames
    assert set(datnames) == {current}


@pytest.mark.parametrize("name", ["locks", "activity", "indexes_unused"])
def test_pg_reports_run(api, name):
    ret = pg_report(api, name)
    assert ret["ok"] is True, ret
    assert ret["columns"]


def test_pg_statements_missing_extension_is_reported(api):
    installed = run_sql(api, "select count(*) from pg_extension where extname = 'pg_stat_statements'")["rows"]
    if installed != [[0]]:
        pytest.skip("pg_stat_statements is installed in the test database")
    assert_failed(pg_report(api, "statements"), "extension_missing")


def test_pg_unknown_report_404(api):
    assert api.get("/introspection/pg/nope").status_code == 404
