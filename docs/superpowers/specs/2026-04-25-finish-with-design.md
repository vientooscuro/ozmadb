# FunDB.finishWith — Design Spec

**Date:** 2026-04-25  
**Status:** Approved

## Overview

Add `FunDB.finishWith(status, userData?, message?)` to the JS action API. Unlike `cancelWith` (which rolls back the transaction), `finishWith` **commits** the transaction and signals the UI to display a colored toast alert.

## Motivation

`cancelWith` aborts the transaction and optionally passes data back to the UI. There was no way to commit changes AND show a status notification. `finishWith` fills this gap.

## API

### JS (action code)

```js
FunDB.finishWith(status, userData?, message?)
// status: 'success' | 'warning' | 'error'  — required
// userData: any                              — optional, passed back as action result
// message: string                           — optional, shown in toast body
```

`finishWith` is **not noreturn** — execution continues after the call. If called multiple times, the last call wins. The transaction commits when the action function returns normally.

### TypeScript definition (`serverside/api.d.ts`)

```ts
finishWith: (status: ActionFinishStatus, userData?: any, message?: string) => void
```

## Architecture

### Data flow

```
JS action calls finishWith(status, userData, message)
  → apiProxy.SetFinishWith({ status, userData, message })   [stores in mutable field on proxy]

action.Run() returns normally (res)
  → Actions.fs reads action.FinishInfo
  → returns Ok { Result = res; FinishInfo = Some { Status; UserData; Message } }

Transaction commits (Ok returned from within transaction scope)

Client receives ActionResponse with finishInfo
  → shows colored toast
  → processes result via attrToLink as usual
```

### Backend (F# — ozmadb)

**`OzmaDB/src/API/Types.fs`** — new types alongside `ActionResponse`:

```fsharp
[<JsonConverter(typeof<StringEnumConverter>)>]
type ActionFinishStatus =
    | [<EnumMember(Value = "success")>] AFSSuccess
    | [<EnumMember(Value = "warning")>] AFSWarning
    | [<EnumMember(Value = "error")>] AFSError

type ActionFinishInfo =
    { Status: ActionFinishStatus
      UserData: JToken option
      Message: string option }

// ActionResponse extended:
type ActionResponse =
    { Result: JObject option
      FinishInfo: ActionFinishInfo option }
```

**`OzmaDB/src/API/JavaScript.fs`** — mutable field + method on the proxy class:

```fsharp
let mutable private currentFinishInfo: ActionFinishInfo option = None

member _.SetFinishWith(body: JObject) =
    // parse status/userData/message from body, store
    currentFinishInfo <- Some { ... }

member _.CurrentFinishInfo = currentFinishInfo
```

JS prelude (inside the same file, `OzmaDBCurrent` class):

```js
finishWith(status, userData, message) {
    apiProxy.SetFinishWith({ status, userData, message });
};
```

**`OzmaDB/src/Actions/Run.fs`** — expose FinishInfo from ActionScript:

```fsharp
member this.FinishInfo = this.Runtime.CurrentFinishInfo
```

**`OzmaDB/src/API/Actions.fs`** — read after Run:

```fsharp
let runAction () =
    task {
        let! res = action.Run(args, ctx.CancellationToken)
        return Ok { Result = res; FinishInfo = action.FinishInfo }
    }
```

No new exception types needed. The outer `with :? ActionRunException` catch is unchanged.

### ozmadb-js

**`src/types.ts`** — extend `IActionResult`:

```ts
export type ActionFinishStatus = 'success' | 'warning' | 'error'

export interface IActionFinishInfo {
  status: ActionFinishStatus
  userData?: unknown
  message?: string
}

export interface IActionResult {
  result: unknown
  finishInfo?: IActionFinishInfo   // new
}
```

**`src/serverside/api.d.ts`** — add to `IOzmaDBAPI`:

```ts
finishWith: (status: ActionFinishStatus, userData?: any, message?: string) => void
```

### Frontend (ozma UI)

**`src/state/actions.ts`** — show toast after successful action:

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
// existing attrToLink logic follows
```

**i18n keys** (add to existing locale files):
- `action_finish_success` — "Done" / "Готово"
- `action_finish_warning` — "Warning" / "Предупреждение"  
- `action_finish_error` — "Error" / "Ошибка"

**`src/styles/style.scss`** — toast colors for glass themes:

```scss
// New CSS variable for error status (no existing token):
html[data-theme-style='light-glass'] { --status-error: #dc2626; }
html[data-theme-style='dark-glass']  { --status-error: #f87171; }

// Toast variants using glass tokens:
html[data-theme-style='light-glass'],
html[data-theme-style='dark-glass'] {
  .finish-toast--success {
    background: rgba(var(--accent-rgb), 0.10);
    border-color: var(--accent);
    color: var(--accent);
  }
  .finish-toast--warning {
    background: rgba(var(--accent-2-rgb), 0.13);
    border-color: var(--accent-2);
    color: var(--accent-2);
  }
  .finish-toast--error {
    background: rgba(var(--status-error-rgb), 0.10);
    border-color: var(--status-error);
    color: var(--status-error);
  }
}
```

> Note: `--accent-rgb`, `--accent-2-rgb`, `--status-error-rgb` will be added as companion RGB variables alongside the hex tokens so `rgba()` works. Alternatively, hardcode the rgba values per theme.

## Color tokens

| Status | light-glass bg | dark-glass bg | border/text |
|---|---|---|---|
| success | `rgba(15,118,110, 0.10)` | `rgba(79,214,190, 0.10)` | `--accent` |
| warning | `rgba(246,185,65, 0.14)` | `rgba(246,185,65, 0.12)` | `--accent-2` |
| error | `rgba(220,38,38, 0.10)` | `rgba(248,113,113, 0.12)` | `--status-error` |

## Toast behavior

- **success**: auto-hides (default Bootstrap-Vue timeout)
- **warning**: `noAutoHide: true` — requires manual dismiss
- **error**: `noAutoHide: true` — requires manual dismiss

## Scope

- No changes to `cancelWith` behavior
- `finishWith` in triggers: out of scope (triggers don't have `cancelWith` either, only actions do)
- Non-glass themes: toast uses Bootstrap default `variant` fallback (success/warning/danger)

## Files to change

| File | Change |
|---|---|
| `OzmaDB/src/API/Types.fs` | Add `ActionFinishStatus`, `ActionFinishInfo`, extend `ActionResponse` |
| `OzmaDB/src/API/JavaScript.fs` | Add mutable field, `SetFinishWith`, `CurrentFinishInfo`, JS `finishWith` |
| `OzmaDB/src/Actions/Run.fs` | Add `FinishInfo` property to `ActionScript` |
| `OzmaDB/src/API/Actions.fs` | Pass `action.FinishInfo` into `ActionResponse` |
| `ozmadb-js/src/types.ts` | Add `ActionFinishStatus`, `IActionFinishInfo`, extend `IActionResult` |
| `ozmadb-js/src/serverside/api.d.ts` | Add `finishWith` to `IOzmaDBAPI` |
| `ozma/src/state/actions.ts` | Show toast from `ret.finishInfo` |
| `ozma/src/styles/style.scss` | Add `--status-error`, `.finish-toast--*` styles |
| `ozma/src/locales/*.json` | Add `action_finish_success/warning/error` keys |
