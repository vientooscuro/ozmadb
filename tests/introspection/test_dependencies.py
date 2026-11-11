def get_dependencies(api, schema: str, entity: str, field: str | None = None):
    params = {"schema": schema, "entity": entity}
    if field is not None:
        params["field"] = field
    return api.get("/introspection/dependencies", params=params)


def dependencies(api, schema: str, entity: str, field: str | None = None) -> list[dict]:
    resp = get_dependencies(api, schema, entity, field)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    return body["items"]


def find(items: list[dict], kind: str, name: str) -> list[dict]:
    return [i for i in items if i["kind"] == kind and i["name"] == name]


def test_capabilities_lists_dependencies(api):
    assert "dependencies" in api.get("/introspection/capabilities").json()["features"]


def test_dependencies_finds_all_kinds(api, test_schema):
    items = dependencies(api, "mcp_test", "parent", "amount")
    assert {"computed_field", "check_constraint", "user_view", "role_rule", "action"} <= {i["kind"] for i in items}

    assert find(items, "computed_field", "double_amount") == [
        {
            "kind": "computed_field",
            "schema": "mcp_test",
            "name": "double_amount",
            "entity": "mcp_test.parent",
            "detail": None,
            "confidence": "resolved",
        }
    ]
    assert find(items, "check_constraint", "amount_non_negative")[0]["entity"] == "mcp_test.parent"
    view = find(items, "user_view", "parents_view")
    assert view == [
        {
            "kind": "user_view",
            "schema": "mcp_test",
            "name": "parents_view",
            "entity": None,
            "detail": None,
            "confidence": "resolved",
        }
    ]
    rules = find(items, "role_rule", "mcp_test_role")
    assert rules == [
        {
            "kind": "role_rule",
            "schema": "mcp_test",
            "name": "mcp_test_role",
            "entity": "mcp_test.parent",
            "detail": "select",
            "confidence": "resolved",
        }
    ]

    keys = [(i["kind"], i["schema"], i["name"], i["detail"] or "") for i in items]
    assert keys == sorted(keys)
    assert len(keys) == len(set(keys))


def test_dependencies_through_child_entity(api, test_schema):
    items = dependencies(api, "mcp_test", "parent", "amount")
    assert find(items, "computed_field", "amount_plus_one")[0]["entity"] == "mcp_test.child"
    # Inherited fields resolve to the declaring entity.
    assert dependencies(api, "mcp_test", "child", "amount") == items


def test_dependencies_unused_field_empty(api, test_schema):
    assert dependencies(api, "mcp_test", "orders", "total") == []


def test_dependencies_js_marked_text(api, test_schema):
    items = dependencies(api, "mcp_test", "parent", "amount")
    assert find(items, "action", "use_amount") == [
        {
            "kind": "action",
            "schema": "mcp_test",
            "name": "use_amount",
            "entity": None,
            "detail": "line 2",
            "confidence": "text",
        }
    ]


def test_dependencies_entity_level(api, test_schema):
    items = dependencies(api, "mcp_test", "parent")
    assert find(items, "computed_field", "double_amount")
    assert find(items, "user_view", "parents_view")
    assert find(items, "computed_field", "amount_plus_one")


def test_dependencies_unknown_404(api, test_schema):
    assert get_dependencies(api, "mcp_test", "nope").status_code == 404
    assert get_dependencies(api, "no_such_schema", "parent").status_code == 404
    assert get_dependencies(api, "mcp_test", "parent", "nope").status_code == 404
