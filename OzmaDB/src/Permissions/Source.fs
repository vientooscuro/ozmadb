module OzmaDB.Permissions.Source

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.AST

type ResolvedRoleRef = ResolvedEntityRef

type SourceAllowedField =
    { Insert: bool
      Select: string option
      Update: string option
      Check: string option }

type SourceAllowedEntity =
    { AllowBroken: bool
      AllowAllFields: bool
      Check: string option
      Insert: bool
      Select: string option
      Update: string option
      Delete: string option
      Fields: Map<FieldName, SourceAllowedField> }

type SourceAllowedSchema =
    { Entities: Map<EntityName, SourceAllowedEntity> }

type SourceAllowedDatabase =
    { Schemas: Map<SchemaName, SourceAllowedSchema> }

    member this.FindEntity(entity: ResolvedEntityRef) =
        match Map.tryFind entity.Schema this.Schemas with
        | None -> None
        | Some schema -> Map.tryFind entity.Name schema.Entities

// Identifies which actions/triggers a role may run with elevated (root) privileges.
// Checked at action/trigger invocation time; matching means the code executes as RTRoot.
type SourcePrivilegedActionRef =
    | SPAAll
    | SPASchema of SchemaName
    | SPAAction of ResolvedEntityRef

type SourcePrivilegedTriggerRef =
    | SPTAll
    | SPTSchema of SchemaName
    | SPTTrigger of ResolvedEntityRef

type SourceRole =
    { Parents: Set<ResolvedRoleRef>
      Permissions: SourceAllowedDatabase
      AllowBroken: bool
      AllowAllEntities: bool
      AllowAllInsert: bool
      AllowAllUpdate: bool
      AllowAllDelete: bool
      DeniedUserViews: Set<ResolvedUserViewRef>
      PrivilegedActions: Set<SourcePrivilegedActionRef>
      PrivilegedTriggers: Set<SourcePrivilegedTriggerRef> }

type SourcePermissionsSchema = { Roles: Map<RoleName, SourceRole> }

let emptySourcePermissionsSchema: SourcePermissionsSchema = { Roles = Map.empty }

let mergeSourcePermissionsSchema (a: SourcePermissionsSchema) (b: SourcePermissionsSchema) : SourcePermissionsSchema =
    { Roles = Map.unionUnique a.Roles b.Roles }

type SourcePermissions =
    { Schemas: Map<SchemaName, SourcePermissionsSchema> }

let emptySourcePermissions: SourcePermissions = { Schemas = Map.empty }
