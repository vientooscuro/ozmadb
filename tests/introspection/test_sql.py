import base64
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
