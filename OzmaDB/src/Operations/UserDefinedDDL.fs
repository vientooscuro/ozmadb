module OzmaDB.Operations.UserDefinedDDL

open System
open System.Linq
open System.Threading
open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open Npgsql

open OzmaDB.OzmaUtils
open OzmaDB.Connection
open OzmaDB.SQL.Query

module SQL = OzmaDB.SQL.AST

let private registryMissing (e: exn) =
    match e with
    | :? QueryExecutionException as qe ->
        match qe.InnerException with
        | :? PostgresException as pe -> pe.SqlState = "42P01" || pe.SqlState = "42703"
        | _ -> false
    | _ -> false

let private isDuplicateDdlError (e: exn) =
    match e with
    | :? QueryExecutionException as qe ->
        match qe.InnerException with
        | :? PostgresException as pe ->
            // User-defined DDL is executed both pre- and post-migration.
            // Non-idempotent statements like "create type" can fail during the post phase.
            // Treat "already exists"/duplicate-object errors as benign so registry writes stay stable.
            pe.SqlState = "42710" // duplicate_object
            || pe.SqlState = "42P07" // duplicate_table (defensive)
            || pe.SqlState = "42723" // duplicate_function
        | _ -> false
    | _ -> false

let private registryTablesExist
    (transaction: DatabaseTransaction)
    (cancellationToken: CancellationToken)
    : Task<bool> =
    task {
        let! res =
            transaction.Connection.Query.ExecuteValueQuery
                "select to_regclass('public.user_types') is not null and to_regclass('public.user_functions') is not null"
                Map.empty
                cancellationToken

        match res with
        | Some(_, _, SQL.VBool exists) -> return exists
        | _ -> return false
    }

let private loadUserTypes (transaction: DatabaseTransaction) (cancellationToken: CancellationToken) =
    task {
        try
            let! exists = registryTablesExist transaction cancellationToken

            if not exists then
                return [||]
            else
                let! rows =
                    transaction.System.UserTypes
                        .Where(fun t -> t.Enabled)
                        .OrderBy(fun t -> t.Priority)
                        .ThenBy(fun t -> t.Schema.Name)
                        .ThenBy(fun t -> t.Name)
                        .Select(fun t -> struct (t.Schema.Name, t.Name, t.Priority, t.Ddl))
                        .ToArrayAsync(cancellationToken)

                return rows
        with e when registryMissing e ->
            return [||]
    }

let private loadUserFunctions (transaction: DatabaseTransaction) (cancellationToken: CancellationToken) =
    task {
        try
            let! exists = registryTablesExist transaction cancellationToken

            if not exists then
                return [||]
            else
                let! rows =
                    transaction.System.UserFunctions
                        .Where(fun f -> f.Enabled)
                        .OrderBy(fun f -> f.Priority)
                        .ThenBy(fun f -> f.Schema.Name)
                        .ThenBy(fun f -> f.Name)
                        .ThenBy(fun f -> f.Signature)
                        .Select(fun f -> struct (f.Schema.Name, f.Name, f.Signature, f.Priority, f.Ddl))
                        .ToArrayAsync(cancellationToken)

                return rows
        with e when registryMissing e ->
            return [||]
    }

let getProtectedUserFunctions
    (transaction: DatabaseTransaction)
    (cancellationToken: CancellationToken)
    : Task<Set<SQL.SchemaObject>> =
    task {
        let! functions = loadUserFunctions transaction cancellationToken

        return
            functions
            |> Seq.map (fun entry ->
                let struct (schemaName, functionName, _, _, _) = entry
                let objRef: SQL.SchemaObject =
                    { Schema = Some <| SQL.SQLName schemaName
                      Name = SQL.SQLName functionName }
                objRef)
            |> Set.ofSeq
    }

let applyUserDefinedDDL (transaction: DatabaseTransaction) (cancellationToken: CancellationToken) : Task =
    task {
        let! userTypes = loadUserTypes transaction cancellationToken
        let! userFunctions = loadUserFunctions transaction cancellationToken

        let execWithSavepoint (savepointName: string) (ddl: string) =
            task {
                let savepointSql = sprintf "savepoint %s" savepointName
                let rollbackSql = sprintf "rollback to savepoint %s" savepointName
                let releaseSql = sprintf "release savepoint %s" savepointName

                let! _ = transaction.Connection.Query.ExecuteNonQuery savepointSql Map.empty cancellationToken

                try
                    let! _ = transaction.Connection.Query.ExecuteNonQuery ddl Map.empty cancellationToken
                    let! _ = transaction.Connection.Query.ExecuteNonQuery releaseSql Map.empty cancellationToken
                    ()
                with e ->
                    // Any SQL error marks the whole tx aborted; rollback to savepoint restores it.
                    let! _ = transaction.Connection.Query.ExecuteNonQuery rollbackSql Map.empty cancellationToken
                    let! _ = transaction.Connection.Query.ExecuteNonQuery releaseSql Map.empty cancellationToken
                    return raise e
            }

        for i, entry in Seq.indexed userTypes do
            let struct (schemaName, typeName, _, ddl) = entry
            let savepointName = sprintf "__udfddl_type_%d" i

            try
                do! execWithSavepoint savepointName ddl
            with e ->
                if isDuplicateDdlError e then
                    ()
                else
                    raisefWithInner Exception e "Failed to apply user type %s.%s" schemaName typeName

        for i, entry in Seq.indexed userFunctions do
            let struct (schemaName, functionName, signature, _, ddl) = entry
            let savepointName = sprintf "__udfddl_func_%d" i

            try
                do! execWithSavepoint savepointName ddl
            with e ->
                if isDuplicateDdlError e then
                    ()
                else
                    raisefWithInner Exception e "Failed to apply user function %s.%s%s" schemaName functionName signature
    }
