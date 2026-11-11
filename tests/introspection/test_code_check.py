def check(api, kind: str, source: str) -> dict:
    resp = api.post("/introspection/code/check", json={"kind": kind, "source": source})
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_capabilities_lists_code_check(api):
    assert "code_check" in api.get("/introspection/capabilities").json()["features"]


def test_check_valid_action(api):
    body = check(api, "action", "export default async function handler(args) {\n  return { ok: true };\n}\n")
    assert body == {"ok": True, "errors": [], "warnings": []}


def test_check_syntax_error_has_line(api):
    source = "export default async function handler(args) {\n  const a = 1;\n  const b = ;\n}\n"
    body = check(api, "trigger", source)
    assert body["ok"] is False
    assert len(body["errors"]) == 1
    error = body["errors"][0]
    assert error["kind"] == "syntax"
    assert error["line"] == 3
    assert error["column"] == 13
    assert "SyntaxError" in error["message"]
    assert error["path"] is None
    assert body["warnings"] == []


def test_check_module_top_level_not_executed(api):
    body = check(api, "module", 'throw new Error("ran");\n')
    assert body["ok"] is True, body
    assert body["errors"] == []


def test_check_module_with_import(api):
    source = "import { helper } from 'nonexistent/module';\nexport const run = () => helper();\n"
    body = check(api, "module", source)
    assert body["ok"] is True, body
    assert body["warnings"] == []


def test_check_module_syntax_is_module_grammar(api):
    body = check(api, "module", "export const a = 1;\nexport const a = 2;\n")
    assert body["ok"] is False
    assert body["errors"][0]["line"] == 2


def test_check_empty_source_is_valid(api):
    assert check(api, "action", "")["ok"] is True


def test_check_unknown_kind_is_400(api):
    resp = api.post("/introspection/code/check", json={"kind": "script", "source": "1"})
    assert resp.status_code == 400, resp.text


def test_check_missing_source_is_400(api):
    resp = api.post("/introspection/code/check", json={"kind": "action"})
    assert resp.status_code == 400, resp.text


def test_check_too_large_source_is_400(api):
    resp = api.post("/introspection/code/check", json={"kind": "action", "source": "a" * (1024 * 1024 + 1)})
    assert resp.status_code == 400, resp.text
