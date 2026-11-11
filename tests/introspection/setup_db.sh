#!/bin/sh
# Idempotently creates the role and database used by the introspection integration tests.
# Extra arguments are passed to psql (e.g. `-h /tmp -p 5432`).
set -e

name="ozmadb_introspection_test"

psql -U postgres -v ON_ERROR_STOP=1 "$@" <<EOF
select format('create role %I login password %L', '$name', '$name')
where not exists (select from pg_roles where rolname = '$name')
\gexec
select format('create database %I owner %I', '$name', '$name')
where not exists (select from pg_database where datname = '$name')
\gexec
\c $name
alter schema "public" owner to "$name";
EOF
