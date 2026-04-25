# FunDB.finishWith Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `FunDB.finishWith(status, userData?, message?)` to the JS action API — commits the transaction and tells the UI to show a colored toast (green/yellow/red).

**Architecture:** `finishWith` stores finish info on a mutable field of `OzmaJSEngine` (the F# proxy) via `apiProxy.SetFinishWith(...)`. After `action.Run()` returns normally, `Actions.fs` reads `action.FinishInfo` and includes it in `ActionResponse`. The frontend reads `ret.finishInfo` and shows a colored toast before processing `ret.result` as usual.

**Tech Stack:** F# / Newtonsoft.Json (backend), Jint JS engine (JS prelude), TypeScript (ozmadb-js), Vue 2 / Bootstrap-Vue / SCSS (ozma frontend)

---

## File Map

| File | Change |
|---|---|
| `OzmaDB/src/API/Types.fs:802` | Add `ActionFinishInfo` type; extend `ActionResponse` with `FinishInfo` field |
| `OzmaDB/src/API/JavaScript.fs:441` | Add mutable `currentFinishInfo` field + `SetFinishWith` method + `CurrentFinishInfo` property to `OzmaJSEngine` |
| `OzmaDB/src/API/JavaScript.fs:388` | Add `finishWith` to `OzmaDBCurrent` JS class in prelude string |
| `OzmaDB/src/Actions/Run.fs:59` | Add `FinishInfo` property to `ActionScript` (via downcast to `OzmaJSEngine`) |
| `OzmaDB/src/API/Actions.fs:54` | Pass `action.FinishInfo` into `ActionResponse` after `action.Run()` |
| `ozmadb-js/src/types.ts:363` | Add `ActionFinishStatus`, `IActionFinishInfo`; extend `IActionResult` |
| `ozmadb-js/src/serverside/api.d.ts:65` | Add `finishWith` to `IOzmaDBAPI` |
| `ozma/src/state/actions.ts:48` | Show toast from `ret.finishInfo` after successful action |
| `ozma/src/modules.ts:73` | Add `action_finish_success/warning/error` i18n keys (en/ru/es) |
| `ozma/src/styles/style.scss` | Add `--status-error` token + `.finish-toast--*` CSS for glass themes |

---

## Task 1: Extend ActionResponse with FinishInfo (F# — Types.fs)

**Files:**
- Modify: `OzmaDB/src/API/Types.fs` around line 802

- [ ] **Open `OzmaDB/src/API/Types.fs` and find `type ActionResponse` (line 802). Add the new types immediately before it:**

```fsharp
type ActionFinishInfo =
    { Status: string   // "success" | "warning" | "error"
      UserData: JToken option
      Message: string option }

type ActionResponse =
    { Result: JObject option
      FinishInfo: ActionFinishInfo option }
```

The existing `type ActionResponse = { Result: JObject option }` becomes the new definition above (with `FinishInfo` added).

- [ ] **Build to verify no type errors:**

```bash
cd /Users/vientooscuro/SyncFolder/ozmadb
dotnet build OzmaDB/OzmaDB.fsproj 2>&1 | grep -E "error|warning FS" | head -20
```

Expected: errors about `ActionResponse` missing `FinishInfo` in `Actions.fs` — that's correct, we'll fix it in Task 3.

- [ ] **Commit:**

```bash
git add OzmaDB/src/API/Types.fs
git commit -m "feat: add ActionFinishInfo type and extend ActionResponse"
```

---

## Task 2: Add SetFinishWith to OzmaJSEngine (F# — JavaScript.fs)

**Files:**
- Modify: `OzmaDB/src/API/JavaScript.fs` — inside `OzmaJSEngine` class (starts line 438)

The `OzmaJSEngine` class currently starts with:
```fsharp
type OzmaJSEngine(runtime: JSRuntime, env: JSEnvironment, settings: JSHostSettings) as this =
    inherit SchedulerJSEngine<Task.SerializingTrackingTaskScheduler>(runtime, env)

    let mutable topLevelAPI = None: IOzmaDBAPI option
    let apiHandle = AsyncLocal<APIHandle>()
```

- [ ] **Add a mutable field and two members to `OzmaJSEngine`. Insert after the existing `let mutable topLevelAPI = None` line:**

```fsharp
    let mutable currentFinishInfo: ActionFinishInfo option = None
```

Then, find the end of the class's `let` bindings (before the first `do` or `member`) and add:

```fsharp
    member _.SetFinishWith(body: JObject) =
        let status =
            match body.Value<string>("status") with
            | null -> "success"
            | s -> s
        let userData =
            match body.["userData"] with
            | null -> None
            | t -> Some t
        let message =
            match body.Value<string>("message") with
            | null -> None
            | s -> Some s
        currentFinishInfo <- Some { Status = status; UserData = userData; Message = message }

    member _.CurrentFinishInfo = currentFinishInfo
```

- [ ] **Build to verify:**

```bash
dotnet build OzmaDB/OzmaDB.fsproj 2>&1 | grep -E "^.*error FS" | head -10
```

Expected: same errors about `ActionResponse` in `Actions.fs` only (from Task 1).

- [ ] **Commit:**

```bash
git add OzmaDB/src/API/JavaScript.fs
git commit -m "feat: add SetFinishWith and CurrentFinishInfo to OzmaJSEngine"
```

---

## Task 3: Add finishWith to JS prelude (JavaScript.fs)

**Files:**
- Modify: `OzmaDB/src/API/JavaScript.fs` — inside the embedded JS string, `OzmaDBCurrent` class

The prelude contains the `OzmaDBCurrent` class. The `cancelWith` method is at line 388:
```js
        cancelWith(userData, message) {
            throw new OzmaDBError({ message, userData });
        };
```

- [ ] **Add `finishWith` immediately after `cancelWith` (before the closing `};` of `OzmaDBCurrent`):**

```js
        finishWith(status, userData, message) {
            apiProxy.SetFinishWith({ status, userData, message });
        };
```

- [ ] **Build:**

```bash
dotnet build OzmaDB/OzmaDB.fsproj 2>&1 | grep -E "^.*error FS" | head -10
```

- [ ] **Commit:**

```bash
git add OzmaDB/src/API/JavaScript.fs
git commit -m "feat: add finishWith to JS prelude"
```

---

## Task 4: Expose FinishInfo on ActionScript and wire into ActionResponse

**Files:**
- Modify: `OzmaDB/src/Actions/Run.fs` around line 59
- Modify: `OzmaDB/src/API/Actions.fs` around line 54

### Run.fs — add FinishInfo property

`ActionScript.Runtime` returns `JSEngine`, but the actual runtime is always `OzmaJSEngine`. Add `FinishInfo` via downcast.

The current end of `ActionScript` (line 59):
```fsharp
    member this.Runtime = engine
```

- [ ] **Add `FinishInfo` property after `Runtime`:**

```fsharp
    member this.Runtime = engine

    member this.FinishInfo =
        match engine with
        | :? OzmaJSEngine as e -> e.CurrentFinishInfo
        | _ -> None
```

You'll need to open the `OzmaDB.API` namespace (where `OzmaJSEngine` lives) at the top of `Run.fs`. Check existing opens and add if missing:

```fsharp
open OzmaDB.API.JavaScript
```

### Actions.fs — use FinishInfo in runAction

Current `runAction` in `Actions.fs` (line 51-55):
```fsharp
                    let runAction () =
                        task {
                            let! res = action.Run(args, ctx.CancellationToken)
                            return Ok { Result = res }
                        }
```

- [ ] **Change to pass FinishInfo:**

```fsharp
                    let runAction () =
                        task {
                            let! res = action.Run(args, ctx.CancellationToken)
                            return Ok { Result = res; FinishInfo = action.FinishInfo }
                        }
```

- [ ] **Build — expect clean:**

```bash
dotnet build OzmaDB/OzmaDB.fsproj 2>&1 | grep -E "^.*error FS" | head -10
```

Expected: 0 errors.

- [ ] **Commit:**

```bash
git add OzmaDB/src/Actions/Run.fs OzmaDB/src/API/Actions.fs
git commit -m "feat: wire action.FinishInfo into ActionResponse"
```

---

## Task 5: Extend ozmadb-js types

**Files:**
- Modify: `ozmadb-js/src/types.ts` around line 363
- Modify: `ozmadb-js/src/serverside/api.d.ts` around line 65

### types.ts

Current `IActionResult` (line 363):
```ts
export interface IActionResult {
  result: unknown
}
```

- [ ] **Add new types before `IActionResult` and extend it:**

```ts
export type ActionFinishStatus = 'success' | 'warning' | 'error'

export interface IActionFinishInfo {
  status: ActionFinishStatus
  userData?: unknown
  message?: string
}

export interface IActionResult {
  result: unknown
  finishInfo?: IActionFinishInfo
}
```

### api.d.ts

Current `IOzmaDBAPI` ends with `cancelWith` (line 65):
```ts
  cancelWith: (userData: any, message?: string) => any // noreturn
```

- [ ] **Add `finishWith` after `cancelWith`:**

```ts
  cancelWith: (userData: any, message?: string) => any // noreturn
  finishWith: (status: ActionFinishStatus, userData?: any, message?: string) => void
```

Add the import of `ActionFinishStatus` at the top of `api.d.ts` if it's not already exported from the same file. Since `api.d.ts` doesn't import from `types.ts`, define it inline:

```ts
type ActionFinishStatus = 'success' | 'warning' | 'error'
```

Add this line near the top of `api.d.ts`, before `IOzmaDBAPI`.

- [ ] **Build ozmadb-js:**

```bash
cd /Users/vientooscuro/SyncFolder/ozmadb/ozmadb-js
npm run build 2>&1 | tail -20
```

Expected: successful build.

- [ ] **Commit:**

```bash
git add ozmadb-js/src/types.ts ozmadb-js/src/serverside/api.d.ts
git commit -m "feat: add ActionFinishStatus, IActionFinishInfo to ozmadb-js types"
```

---

## Task 6: Frontend — show toast from finishInfo

**Files:**
- Modify: `ozma/src/state/actions.ts` around line 48
- Modify: `ozma/src/modules.ts` around lines 73, 87, 101
- Modify: `ozma/src/styles/style.scss`

### actions.ts — show toast

Current block in `saveAndRunAction` (line 44–52):
```ts
          try {
            ret = await dispatch(
              'callApi',
              {
                func: (api: FunDBAPI) => api.runAction(ref, args),
              },
              { root: true },
            )
          } catch (e) {
```

- [ ] **After `ret = await dispatch(...)` succeeds (between the assignment and the closing `}` of the `try`), add:**

```ts
            if (ret.finishInfo) {
              const { status, message } = ret.finishInfo
              app.$bvToast.toast(message ?? '', {
                title: i18n.tc(`action_finish_${status}`),
                toastClass: `finish-toast finish-toast--${status}`,
                solid: true,
                noAutoHide: status !== 'success',
              })
            }
```

The full `try` block becomes:
```ts
          try {
            ret = await dispatch(
              'callApi',
              {
                func: (api: FunDBAPI) => api.runAction(ref, args),
              },
              { root: true },
            )
            if (ret.finishInfo) {
              const { status, message } = ret.finishInfo
              app.$bvToast.toast(message ?? '', {
                title: i18n.tc(`action_finish_${status}`),
                toastClass: `finish-toast finish-toast--${status}`,
                solid: true,
                noAutoHide: status !== 'success',
              })
            }
          } catch (e) {
```

### modules.ts — i18n keys

Locale messages are defined inline in `modules.ts`. Find the `en`, `ru`, `es` blocks (around lines 66–107) and add keys next to `exception_in_action`:

**en block** — after `exception_in_action: 'Exception in action',`:
```ts
    action_finish_success: 'Done',
    action_finish_warning: 'Warning',
    action_finish_error: 'Error',
```

**ru block** — after `exception_in_action: 'Исключение в действии',`:
```ts
    action_finish_success: 'Готово',
    action_finish_warning: 'Предупреждение',
    action_finish_error: 'Ошибка',
```

**es block** — after `exception_in_action: 'La excepción está en acción',`:
```ts
    action_finish_success: 'Listo',
    action_finish_warning: 'Advertencia',
    action_finish_error: 'Error',
```

### style.scss — toast colors

- [ ] **Append at the end of `ozma/src/styles/style.scss`:**

```scss
/* finishWith toast status colors */
html[data-theme-style='light-glass'] {
  --status-error: #dc2626;
}

html[data-theme-style='dark-glass'] {
  --status-error: #f87171;
}

html[data-theme-style='light-glass'],
html[data-theme-style='dark-glass'] {
  .finish-toast--success {
    background-color: rgba(15, 118, 110, 0.10) !important;
    border-color: var(--accent) !important;
    color: var(--accent) !important;

    .toast-header {
      background-color: rgba(15, 118, 110, 0.14) !important;
      color: var(--accent) !important;
      border-bottom-color: var(--accent) !important;
    }
  }

  .finish-toast--warning {
    background-color: rgba(246, 185, 65, 0.13) !important;
    border-color: var(--accent-2) !important;
    color: var(--accent-2) !important;

    .toast-header {
      background-color: rgba(246, 185, 65, 0.18) !important;
      color: var(--accent-2) !important;
      border-bottom-color: var(--accent-2) !important;
    }
  }

  .finish-toast--error {
    background-color: rgba(220, 38, 38, 0.10) !important;
    border-color: var(--status-error) !important;
    color: var(--status-error) !important;

    .toast-header {
      background-color: rgba(220, 38, 38, 0.14) !important;
      color: var(--status-error) !important;
      border-bottom-color: var(--status-error) !important;
    }
  }
}

html[data-theme-style='dark-glass'] {
  .finish-toast--success {
    background-color: rgba(79, 214, 190, 0.10) !important;
  }

  .finish-toast--error {
    background-color: rgba(248, 113, 113, 0.12) !important;
  }
}
```

- [ ] **Build ozma (type-check):**

```bash
cd /Users/vientooscuro/SyncFolder/ozma
npx vue-tsc --noEmit 2>&1 | head -30
```

Expected: 0 errors.

- [ ] **Commit:**

```bash
git add src/state/actions.ts src/modules.ts src/styles/style.scss
git commit -m "feat: show finishWith colored toast in UI"
```

---

## Task 7: Smoke test

No automated tests exist for the action pipeline, so verify manually.

- [ ] **Write a test action in your dev instance. In the OzmaDB action editor, create an action with this body:**

```js
// Test success
FunDB.finishWith('success', null, 'Operation completed successfully')

// To test warning, swap the line above with:
// FunDB.finishWith('warning', null, 'Something looks off')

// To test error:
// FunDB.finishWith('error', null, 'Something went wrong')
```

- [ ] **Run the action from the UI and verify:**
  - Toast appears with correct color (green/yellow/red)
  - `noAutoHide` for warning and error (toast stays until dismissed)
  - `success` toast auto-hides
  - Any DB changes made before `finishWith` are committed (add an insert before `finishWith` and verify it's persisted)

- [ ] **Verify `finishWith` with `userData` works as before:**

```js
const result = { someKey: 'someValue' }
FunDB.finishWith('success', result, 'Done')
// If the action button is configured to follow the result link, it still works
```

- [ ] **Verify `cancelWith` is unaffected — run an action that calls `cancelWith` and confirm it still rolls back.**

---

## Self-Review Checklist

- ✅ `ActionFinishInfo` defined in Task 1, used consistently in Tasks 2, 4 — names match
- ✅ `SetFinishWith` defined in Task 2, called from JS prelude in Task 3 — names match
- ✅ `action.FinishInfo` defined in Task 4 Run.fs, used in Actions.fs — names match
- ✅ `IActionFinishInfo.status` (lowercase) in types.ts matches what `SetFinishWith` serializes as `status`
- ✅ `action_finish_${status}` i18n template matches keys `action_finish_success/warning/error`
- ✅ `finish-toast--${status}` CSS class matches selectors in style.scss
- ✅ `noAutoHide: status !== 'success'` — success auto-hides, warning/error do not
- ✅ Glass dark-glass override for `--status-error` (`#f87171`) in Task 6 style.scss
- ✅ No `cancelWith` behavior changed
