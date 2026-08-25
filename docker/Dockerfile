FROM ubuntu:24.04

# ca-certificates are needed for the OIDC interactions.
# libgssapi-krb5-2 is probed by Npgsql on startup; without it every start logs a failure to load
# the shared object, even though we never authenticate over GSSAPI.
RUN apt-get update && apt-get install -y \
  ca-certificates jq libgssapi-krb5-2 \
  && rm -rf /var/lib/apt/lists/*

COPY out/ozmadb/ /opt/ozmadb
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh

EXPOSE 5000/tcp
CMD ["/usr/local/bin/docker-entrypoint.sh"]
