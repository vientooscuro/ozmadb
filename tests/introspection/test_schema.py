from conftest import insert, query_rows, run_transaction


def get_schema(api):
    resp = api.get("/introspection/schema")
    assert resp.status_code == 200, resp.text
    return resp.json()


def get_entity(api, schema: str, entity: str):
    return api.get(f"/introspection/schema/{schema}/{entity}")


def test_capabilities_lists_schema(api):
    assert "schema" in api.get("/introspection/capabilities").json()["features"]


def test_schema_lists_test_entities(api, test_schema):
    body = get_schema(api)
    by_name = {s["schema"]: s for s in body}
    assert "mcp_test" in by_name
    assert "public" in by_name
    assert [s["schema"] for s in body] == sorted(by_name)
    entities = {e["name"]: e for e in by_name["mcp_test"]["entities"]}
    assert sorted(entities) == ["child", "grandchild", "orders", "parent"]
    assert [e["name"] for e in by_name["mcp_test"]["entities"]] == ["child", "grandchild", "orders", "parent"]
    assert entities["child"]["parent"] == "mcp_test.parent"
    assert entities["parent"]["parent"] is None
    assert entities["parent"]["isAbstract"] is False
    assert entities["parent"]["isFrozen"] is False


def test_entity_includes_inherited_fields(api, test_schema):
    resp = get_entity(api, "mcp_test", "child")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["name"] == "child"
    assert body["parent"] == "mcp_test.parent"
    fields = {f["name"]: f for f in body["columnFields"]}
    assert fields["name"]["inheritedFrom"] == "mcp_test.parent"
    assert fields["name"]["type"] == "string"
    assert fields["name"]["default"] is None
    assert fields["name"]["isNullable"] is True
    assert fields["name"]["isImmutable"] is False
    assert fields["extra"]["inheritedFrom"] is None


def test_entity_children(api, test_schema):
    body = get_entity(api, "mcp_test", "parent").json()
    assert body["children"] == ["mcp_test.child"]
    assert body["parent"] is None
    assert body["mainField"] == "id"
    assert body["indexes"] == []


def test_entity_reference_type(api, test_schema):
    body = get_entity(api, "mcp_test", "orders").json()
    fields = {f["name"]: f for f in body["columnFields"]}
    assert fields["parent_ref"]["type"] == "reference(mcp_test.parent)"
    assert fields["total"]["type"] == "decimal"
    assert body["references"] == [{"field": "parent_ref", "target": "mcp_test.parent"}]


def test_entity_unknown_404(api, test_schema):
    assert get_entity(api, "mcp_test", "nope").status_code == 404
    assert get_entity(api, "no_such_schema", "parent").status_code == 404


def test_entity_children_direct_only(api, test_schema):
    assert get_entity(api, "mcp_test", "parent").json()["children"] == ["mcp_test.child"]
    assert get_entity(api, "mcp_test", "child").json()["children"] == ["mcp_test.grandchild"]


def test_entity_computed_field(api, test_schema):
    parent = {f["name"]: f for f in get_entity(api, "mcp_test", "parent").json()["computedFields"]}
    assert parent["double_amount"]["inheritedFrom"] is None
    assert "amount" in parent["double_amount"]["expression"]
    assert parent["double_amount"]["isVirtual"] is False
    child = {f["name"]: f for f in get_entity(api, "mcp_test", "child").json()["computedFields"]}
    assert child["double_amount"]["inheritedFrom"] == "mcp_test.parent"


def test_entity_check_constraint(api, test_schema):
    checks = {c["name"]: c for c in get_entity(api, "mcp_test", "parent").json()["checkConstraints"]}
    assert "amount" in checks["amount_non_negative"]["expression"]


def test_entity_unique_constraint(api, test_schema):
    uniques = {c["name"]: c for c in get_entity(api, "mcp_test", "orders").json()["uniqueConstraints"]}
    assert uniques["unique_parent_ref"]["columns"] == ["parent_ref"]


def test_entity_expressions_are_stored_source(api, test_schema):
    body = get_entity(api, "mcp_test", "parent").json()
    computed = {f["name"]: f for f in body["computedFields"]}["double_amount"]
    assert computed["expression"] == "amount * 2"
    assert "amount" in computed["resolvedExpression"]
    assert computed["isBroken"] is False
    assert computed["error"] is None
    check = {c["name"]: c for c in body["checkConstraints"]}["amount_non_negative"]
    assert check["expression"] == "amount >= 0"
    assert "amount" in check["resolvedExpression"]
    child = {f["name"]: f for f in get_entity(api, "mcp_test", "child").json()["computedFields"]}
    assert child["double_amount"]["expression"] == "amount * 2"


def test_entity_includes_broken_computed_field(api, test_schema):
    orders_id = query_rows(
        api, "select id from public.entities where schema_id=>name = 'mcp_test' and name = 'orders'"
    )[0][0]
    field_id = insert(
        api,
        "public",
        "computed_fields",
        {"entity_id": orders_id, "name": "broken_calc", "expression": "no_such_column + 1", "allow_broken": True},
    )
    try:
        computed = {f["name"]: f for f in get_entity(api, "mcp_test", "orders").json()["computedFields"]}
        broken = computed["broken_calc"]
        assert broken["isBroken"] is True
        assert broken["error"]
        assert broken["expression"] == "no_such_column + 1"
        assert broken["resolvedExpression"] is None
        assert broken["isVirtual"] is False
        assert broken["inheritedFrom"] is None
    finally:
        run_transaction(
            api, [{"type": "delete", "entity": {"schema": "public", "name": "computed_fields"}, "id": field_id}]
        )
