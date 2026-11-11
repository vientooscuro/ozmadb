import time

from conftest import insert

SEARCH_SCHEMA = "mcp_test"


def search(api, q, **params):
    return api.get("/introspection/search", params={"q": q, **params})


def matches(api, q, **params) -> list[dict]:
    resp = search(api, q, **params)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True, body
    return body["matches"]


def schema_id(api):
    from conftest import query_rows

    return query_rows(api, f"select id from public.schemas where name = '{SEARCH_SCHEMA}'")[0][0]


def test_capabilities_lists_search(api):
    assert "search" in api.get("/introspection/capabilities").json()["features"]


def test_search_plain_all_occurrences(api, test_schema):
    insert(
        api,
        "public",
        "actions",
        {
            "schema_id": schema_id(api),
            "name": "multi_hit",
            "function": "export default async function handler(args) {\n  const zzmarker = 1;\n  return zzmarker;\n}\n",
        },
    )
    items = matches(api, "zzmarker")
    assert [(m["kind"], m["name"], m["line"], m["column"]) for m in items] == [
        ("action", "multi_hit", 2, 9),
        ("action", "multi_hit", 3, 10),
    ]
    assert items[0]["schema"] == "mcp_test"
    assert items[0]["entity"] is None
    assert items[0]["excerpt"] == "const zzmarker = 1;"


def test_search_case_sensitivity(api, test_schema):
    assert matches(api, "ZZMARKER", caseSensitive="true") == []
    assert len(matches(api, "ZZMARKER")) == 2


def test_search_regex(api, test_schema):
    items = matches(api, r"zz\w+ = \d", regex="true")
    assert [(m["name"], m["line"], m["column"]) for m in items] == [("multi_hit", 2, 9)]


def test_search_regex_invalid(api, test_schema):
    resp = search(api, "(", regex="true")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert body["errors"][0]["kind"] == "regex_invalid"


def test_search_kinds_filter(api, test_schema):
    all_items = matches(api, "amount")
    assert {"action", "computed_field", "check_constraint", "user_view", "role_rule"} <= {m["kind"] for m in all_items}

    only = matches(api, "amount", kinds="computed_fields,check_constraints")
    assert {m["kind"] for m in only} == {"computed_field", "check_constraint"}
    computed = [m for m in only if m["kind"] == "computed_field" and m["name"] == "parent.double_amount"]
    assert computed[0]["entity"] == "mcp_test.parent"
    assert computed[0]["line"] == 1

    assert {m["kind"] for m in matches(api, "amount", kinds="actions")} == {"action"}
    assert [(m["kind"], m["schema"], m["name"]) for m in all_items] == sorted(
        (m["kind"], m["schema"], m["name"]) for m in all_items
    )


def test_search_limit_truncates(api, test_schema):
    resp = search(api, "amount", limit="2")
    body = resp.json()
    assert body["ok"] is True
    assert len(body["matches"]) == 2
    assert body["truncated"] is True

    full = search(api, "amount", limit="2000").json()
    assert full["truncated"] is False


def test_search_bad_params(api, test_schema):
    assert search(api, "").status_code == 400
    assert api.get("/introspection/search").status_code == 400
    assert search(api, "x", kinds="bogus").status_code == 400
    assert search(api, "x", limit="0").status_code == 400
    assert search(api, "x", regex="maybe").status_code == 400


def test_search_regex_timeout(api, test_schema):
    insert(
        api,
        "public",
        "actions",
        {
            "schema_id": schema_id(api),
            "name": "evil_source",
            "function": "export default async function handler(args) {\n  // " + "a" * 40 + "b\n  return 1;\n}\n",
        },
    )
    start = time.monotonic()
    resp = search(api, "(a+)+$", regex="true", kinds="actions")
    elapsed = time.monotonic() - start
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert body["errors"][0]["kind"] == "regex_timeout"
    assert elapsed < 5
