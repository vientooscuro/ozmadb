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
    assert sorted(entities) == ["child", "orders", "parent"]
    assert [e["name"] for e in by_name["mcp_test"]["entities"]] == ["child", "orders", "parent"]
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
    assert body["computedFields"] == []
    assert body["checkConstraints"] == []
    assert body["uniqueConstraints"] == []
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
