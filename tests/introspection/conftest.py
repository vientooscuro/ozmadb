import os

import httpx
import pytest

BASE_URL = os.environ.get("OZMADB_TEST_URL", "http://127.0.0.1:5099")
TEST_SCHEMA = "mcp_test"


def _check(resp: httpx.Response) -> dict:
    assert resp.status_code == 200, f"{resp.request.method} {resp.request.url}: {resp.status_code} {resp.text}"
    return resp.json()


def query_rows(client: httpx.Client, query: str) -> list[list]:
    """Runs an anonymous FunQL query and returns plain row values."""
    body = _check(client.get("/views/anonymous/entries", params={"__query": query}))
    return [[v["value"] for v in row["values"]] for row in body["result"]["rows"]]


def run_transaction(client: httpx.Client, operations: list[dict]) -> list[dict]:
    return _check(client.post("/transaction/", json={"operations": operations}))["results"]


def insert(client: httpx.Client, schema: str, name: str, fields: dict) -> int:
    op = {"type": "insert", "entity": {"schema": schema, "name": name}, "fields": fields}
    return run_transaction(client, [op])[0]["id"]


def _drop_test_schema(client: httpx.Client) -> None:
    rows = query_rows(client, f"select id from public.schemas where name = '{TEST_SCHEMA}'")
    for (schema_id,) in rows:
        run_transaction(
            client,
            [{"type": "recursiveDelete", "entity": {"schema": "public", "name": "schemas"}, "id": schema_id}],
        )


@pytest.fixture(scope="session")
def api():
    client = httpx.Client(base_url=BASE_URL, timeout=60.0)
    try:
        client.get("/ping")
    except httpx.TransportError as e:
        client.close()
        pytest.skip(f"OzmaDB is not reachable at {BASE_URL} ({e}); start it as described in tests/introspection/README.md")
    yield client
    client.close()


@pytest.fixture(scope="session")
def test_schema(api):
    _drop_test_schema(api)

    schema_id = insert(api, "public", "schemas", {"name": TEST_SCHEMA})

    def entity(name: str, **extra) -> int:
        return insert(api, "public", "entities", {"schema_id": schema_id, "name": name, **extra})

    def column(entity_id: int, name: str, type_: str) -> None:
        insert(api, "public", "column_fields", {"entity_id": entity_id, "name": name, "type": type_, "is_nullable": True})

    parent_id = entity("parent")
    column(parent_id, "name", "string")
    column(parent_id, "amount", "int")

    child_id = entity("child", parent_id=parent_id)
    column(child_id, "extra", "string")

    grandchild_id = entity("grandchild", parent_id=child_id)
    insert(api, "public", "computed_fields", {"entity_id": parent_id, "name": "double_amount", "expression": "amount * 2"})
    insert(api, "public", "check_constraints", {"entity_id": parent_id, "name": "amount_non_negative", "expression": "amount >= 0"})

    orders_id = entity("orders")
    column(orders_id, "parent_ref", f"reference({TEST_SCHEMA}.parent)")
    column(orders_id, "total", "decimal")
    insert(api, "public", "unique_constraints", {"entity_id": orders_id, "name": "unique_parent_ref", "columns": ["parent_ref"]})

    insert(api, "public", "computed_fields", {"entity_id": child_id, "name": "amount_plus_one", "expression": "amount + 1"})

    # Views, roles and actions belong to the schema, so dropping it removes them too.
    insert(
        api,
        "public",
        "user_views",
        {"schema_id": schema_id, "name": "parents_view", "query": f"select amount from {TEST_SCHEMA}.parent"},
    )
    role_id = insert(api, "public", "roles", {"schema_id": schema_id, "name": "mcp_test_role"})
    insert(api, "public", "role_entities", {"role_id": role_id, "entity_id": parent_id, "select": "amount > 0"})
    insert(
        api,
        "public",
        "actions",
        {
            "schema_id": schema_id,
            "name": "use_amount",
            "function": "export default async function handler(args) {\n  const amount = args.amount;\n  return { ok: true };\n}\n",
        },
    )

    yield TEST_SCHEMA

    _drop_test_schema(api)
