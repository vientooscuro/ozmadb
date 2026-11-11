from conftest import query_rows


def validate(api, query: str, **extra) -> dict:
    resp = api.post("/introspection/funql/validate", json={"query": query, **extra})
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_capabilities_lists_validate(api):
    resp = api.get("/introspection/capabilities")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["version"] == 1
    assert "validate" in body["features"]


def test_validate_ok_returns_columns_and_used(api, test_schema):
    body = validate(api, "select name, amount from mcp_test.parent")
    assert body["ok"] is True, body
    assert body["errors"] == []
    assert [c["name"] for c in body["columns"]] == ["name", "amount"]
    assert body["used"] == {"mcp_test": {"parent": ["amount", "name"]}}
    assert "FROM" in body["sql"]


def test_validate_parse_error(api):
    body = validate(api, "select from where")
    assert body["ok"] is False
    assert body["errors"][0]["kind"] == "parse"
    assert body["sql"] is None


def test_validate_unknown_field(api, test_schema):
    body = validate(api, "select nope from mcp_test.parent")
    assert body["ok"] is False
    error = body["errors"][0]
    assert error["kind"] == "resolve"
    assert "nope" in error["message"]


def test_validate_arguments(api, test_schema):
    body = validate(api, "{ $min int }: select name from mcp_test.parent where amount > $min")
    assert body["ok"] is True, body
    assert body["arguments"] == [{"name": "min", "argType": "int", "optional": False}]


def test_validate_superuser_role_has_no_side_effects(api, test_schema):
    count_query = "select count(*) as c from public.user_views"
    before = query_rows(api, count_query)

    body = validate(api, "select name from mcp_test.parent with superuser role")
    assert body["ok"] is True, body
    assert body["used"] == {"mcp_test": {"parent": ["name"]}}

    assert query_rows(api, count_query) == before
