import os
import re
import shutil
import subprocess
import random
import time

import pytest

from conftest import insert, query_rows, run_transaction


def dry_run(api, operations: list[dict], mode: str | None = None) -> dict:
    body: dict = {"operations": operations}
    if mode is not None:
        body["mode"] = mode
    resp = api.post("/introspection/dry_run", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()


def entity_id(api, schema: str, name: str) -> int:
    rows = query_rows(api, f"select id from public.entities where schema_id=>name = '{schema}' and name = '{name}'")
    return rows[0][0]


def schema_id(api, schema: str) -> int:
    return query_rows(api, f"select id from public.schemas where name = '{schema}'")[0][0]


def column_id(api, schema: str, entity: str, name: str) -> int:
    rows = query_rows(
        api,
        "select id from public.column_fields "
        f"where entity_id=>schema_id=>name = '{schema}' and entity_id=>name = '{entity}' and name = '{name}'",
    )
    return rows[0][0]


def insert_op(schema: str, name: str, fields: dict) -> dict:
    return {"type": "insert", "entity": {"schema": schema, "name": name}, "fields": fields}


def delete_op(schema: str, name: str, id_: int) -> dict:
    return {"type": "delete", "entity": {"schema": schema, "name": name}, "id": id_}


def add_column_op(api, name: str) -> dict:
    parent = entity_id(api, "mcp_test", "parent")
    return insert_op("public", "column_fields", {"entity_id": parent, "name": name, "type": "int", "is_nullable": True})


def sql(query: str) -> str:
    """`public.state` and the physical tables are not visible through FunQL, so they are read with psql."""
    if shutil.which("psql") is None:
        pytest.skip("psql is required to inspect the database directly")
    db = "ozmadb_introspection_test"
    env = {**os.environ, "PGPASSWORD": db}
    out = subprocess.run(
        ["psql", "-h", "localhost", "-p", "5432", "-U", db, "-d", db, "-Atc", query],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout


def snapshot(api) -> tuple:
    version = sql("select value from public.state where name = 'StateVersion'")
    physical = sql(
        "select column_name from information_schema.columns "
        "where table_schema = 'mcp_test' and table_name = 'parent' order by column_name"
    )
    columns = query_rows(
        api,
        "select name from public.column_fields "
        "where entity_id=>schema_id=>name = 'mcp_test' and entity_id=>name = 'parent' order by name",
    )
    resp = api.get("/introspection/schema/mcp_test/parent")
    assert resp.status_code == 200, resp.text
    return (version, physical, columns, resp.json())


def test_capabilities_lists_dry_run(api):
    assert "dry_run" in api.get("/introspection/capabilities").json()["features"]


def test_dry_run_add_column_returns_ddl(api, test_schema):
    body = dry_run(api, [add_column_op(api, "dry_col")])
    assert body["ok"] is True, body
    assert body["errors"] == []
    assert body["brokenObjects"] == []
    assert len(body["results"]) == 1
    added = [s for s in body["ddl"] if re.search(r"ALTER TABLE .* ADD COLUMN", s, re.IGNORECASE)]
    assert len(added) == 1, body["ddl"]
    assert "dry_col" in added[0]


def test_dry_run_broken_computed_reports_error(api, test_schema):
    parent = entity_id(api, "mcp_test", "parent")
    op = insert_op(
        "public",
        "computed_fields",
        {"entity_id": parent, "name": "bad_computed", "expression": "no_such_column * 2"},
    )
    for mode in ("plan", "full"):
        body = dry_run(api, [op], mode)
        assert body["ok"] is False, body
        assert any("bad_computed" in e["message"] for e in body["errors"]), body["errors"]
        assert all(set(e) == {"message", "kind", "line", "column", "path"} for e in body["errors"])


def test_dry_run_drop_used_column_reports_dependents(api, test_schema):
    op = delete_op("public", "column_fields", column_id(api, "mcp_test", "parent", "amount"))

    # Check constraints cannot be marked as broken, so layout resolution stops at the first one.
    body = dry_run(api, [op])
    assert body["ok"] is False, body
    assert [e["kind"] for e in body["errors"]] == ["resolve"]
    assert "amount_non_negative" in body["errors"][0]["message"]

    check_id = query_rows(api, "select id from public.check_constraints where name = 'amount_non_negative'")[0][0]
    body = dry_run(api, [op, delete_op("public", "check_constraints", check_id)])
    assert body["ok"] is False, body
    messages = " ".join(e["message"] for e in body["errors"])
    assert "double_amount" in messages, body["errors"]
    assert "parents_view" in messages, body["errors"]
    assert any("double_amount" in name for name in body["brokenObjects"]), body["brokenObjects"]
    assert any("parents_view" in name for name in body["brokenObjects"]), body["brokenObjects"]


@pytest.mark.parametrize("mode", ["plan", "full"])
def test_dry_run_leaves_no_trace(api, test_schema, mode):
    before = snapshot(api)
    body = dry_run(api, [add_column_op(api, "trace_col")], mode)
    assert body["ok"] is True, body
    assert snapshot(api) == before


def test_dry_run_full_mode_runs_view_dry_run(api, test_schema):
    op = insert_op(
        "public",
        "user_views",
        {"schema_id": schema_id(api, "mcp_test"), "name": "sql_broken_view", "query": "select 'abc'::int as x"},
    )
    plan = dry_run(api, [op], "plan")
    assert plan["ok"] is True, plan

    full = dry_run(api, [op], "full")
    assert full["ok"] is False, full
    assert any("sql_broken_view" in e["message"] for e in full["errors"]), full["errors"]


def test_dry_run_without_migration_is_ok(api, test_schema):
    body = dry_run(api, [])
    assert body == {"ok": True, "errors": [], "results": [], "ddl": [], "brokenObjects": []}


def test_dry_run_unknown_mode_is_400(api):
    resp = api.post("/introspection/dry_run", json={"operations": [], "mode": "apply"})
    assert resp.status_code == 400, resp.text


def test_normal_commit_still_migrates(api, test_schema):
    run_transaction(api, [add_column_op(api, "committed_col")])
    fields = api.get("/introspection/schema/mcp_test/parent").json()["columnFields"]
    assert "committed_col" in [f["name"] for f in fields]

    run_transaction(api, [delete_op("public", "column_fields", column_id(api, "mcp_test", "parent", "committed_col"))])
    fields = api.get("/introspection/schema/mcp_test/parent").json()["columnFields"]
    assert "committed_col" not in [f["name"] for f in fields]


PROBE_TRIGGER = """export default async function handler(event, args) {
  OzmaDB.writeEvent({ probe: "dryprobe-" + String(args.total) + "-end" });
  if (Number(args.total) === 9999999) {
    await OzmaDB.httpRequest({ url: "http://127.0.0.1:9/dry-run-probe" });
  }
}
"""


def event_count(api, condition: str) -> int:
    return len(query_rows(api, f"select id from public.events where {condition}"))


def test_dry_run_disables_trigger_side_effects(api, test_schema):
    trigger_id = insert(
        api,
        "public",
        "triggers",
        {
            "schema_id": schema_id(api, "mcp_test"),
            "trigger_entity_id": entity_id(api, "mcp_test", "orders"),
            "name": "dry_run_probe",
            "time": "AFTER",
            "on_insert": True,
            "procedure": PROBE_TRIGGER,
        },
    )
    dry_total, committed_total = random.sample(range(100000, 999999), 2)
    try:
        # Outbound HTTP is refused before any policy check, so the request is never attempted.
        resp = api.post(
            "/introspection/dry_run",
            json={"operations": [insert_op("mcp_test", "orders", {"total": 9999999})]},
        )
        assert resp.status_code != 200, resp.text
        assert "HTTP requests are disabled during dry run" in resp.text

        body = dry_run(api, [insert_op("mcp_test", "orders", {"total": dry_total})])
        assert body["ok"] is True, body

        # The same trigger outside a dry run writes its event; events are written in order, so once it is
        # visible, anything the dry runs had logged would be too.
        order_id = run_transaction(api, [insert_op("mcp_test", "orders", {"total": committed_total})])[0]["id"]
        run_transaction(api, [delete_op("mcp_test", "orders", order_id)])
        deadline = time.monotonic() + 15
        while event_count(api, f"type = 'writeEvent' and request->>'details' like '%dryprobe-{committed_total}-end%'") == 0:
            assert time.monotonic() < deadline, "the committed trigger event was never written"
            time.sleep(0.2)

        assert event_count(api, f"type = 'writeEvent' and request->>'details' like '%dryprobe-{dry_total}-end%'") == 0
        assert event_count(api, "error->>'message' like '%disabled during dry run%'") == 0
    finally:
        run_transaction(api, [delete_op("public", "triggers", trigger_id)])
