#!/bin/sh
# Builds a host-native, framework-dependent OzmaDB into tests/introspection/.server and runs it
# with dev/config.introspection-test.json on 127.0.0.1:5099.
#
# Usage: run_server.sh [--background | --stop]
#   (default)     run in the foreground
#   --background  run detached, write .server/server.pid, wait until /ping answers
#   --stop        stop the background server
set -e

repo="$(cd "$(dirname "$0")/../.." && pwd)"
server_dir="$repo/tests/introspection/.server"
out_dir="$server_dir/bin"
pid_file="$server_dir/server.pid"
log_file="$server_dir/server.log"
url="http://127.0.0.1:5099"

stop_server() {
    if [ -f "$pid_file" ]; then
        pid="$(cat "$pid_file")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            echo "Stopped OzmaDB (pid $pid)"
        fi
        rm -f "$pid_file"
    else
        echo "No pid file, nothing to stop"
    fi
}

mode="foreground"
case "$1" in
    "") ;;
    --background) mode="background" ;;
    --stop)
        stop_server
        exit 0
        ;;
    *)
        echo "Usage: $0 [--background | --stop]" >&2
        exit 2
        ;;
esac

case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) rid="osx-arm64" ;;
    Darwin-x86_64) rid="osx-x64" ;;
    Linux-x86_64) rid="linux-x64" ;;
    *)
        echo "Unsupported host: $(uname -s)-$(uname -m)" >&2
        exit 1
        ;;
esac

mkdir -p "$server_dir"
cd "$repo"

# Building for a non-default RID rewrites the lock files; always put them back.
restore_lock_files() {
    git -C "$repo" checkout -- OzmaDB/packages.lock.json OzmaDBSchema/packages.lock.json OzmaUtils/packages.lock.json
}
trap restore_lock_files EXIT

dotnet build OzmaDB/OzmaDB.fsproj -r "$rid" -p:SelfContained=false -p:PublishReadyToRun=false -o "$out_dir"

restore_lock_files
trap - EXIT

# The project references only the linux-x64 V8 native package; fetch the same version for other hosts.
if [ "$rid" != "linux-x64" ]; then
    native_lib="ClearScriptV8.$rid.dylib"
    if [ ! -f "$server_dir/native/$native_lib" ]; then
        version="$(grep -A2 '"Microsoft.ClearScript.V8.Native.linux-x64": {' OzmaDB/packages.lock.json | sed -n 's/.*"resolved": "\(.*\)".*/\1/p' | head -n 1)"
        if [ -z "$version" ]; then
            echo "Cannot find the ClearScript V8 version in OzmaDB/packages.lock.json" >&2
            exit 1
        fi
        package="microsoft.clearscript.v8.native.$rid"
        echo "Downloading $package $version"
        rm -rf "$server_dir/native-pkg"
        mkdir -p "$server_dir/native-pkg" "$server_dir/native"
        curl -fsSL -o "$server_dir/native-pkg/pkg.nupkg" \
            "https://api.nuget.org/v3-flatcontainer/$package/$version/$package.$version.nupkg"
        unzip -q -o "$server_dir/native-pkg/pkg.nupkg" "runtimes/$rid/native/*" -d "$server_dir/native-pkg"
        cp "$server_dir/native-pkg/runtimes/$rid/native/$native_lib" "$server_dir/native/"
        rm -rf "$server_dir/native-pkg"
    fi
    cp "$server_dir/native/$native_lib" "$out_dir/"
fi

if [ "$mode" = "foreground" ]; then
    exec "$out_dir/OzmaDB" dev/config.introspection-test.json
fi

stop_server >/dev/null
nohup "$out_dir/OzmaDB" dev/config.introspection-test.json >"$log_file" 2>&1 &
echo $! >"$pid_file"

for _ in $(seq 1 90); do
    if curl -fs -o /dev/null "$url/ping"; then
        echo "OzmaDB is up at $url (pid $(cat "$pid_file"), log $log_file)"
        exit 0
    fi
    if ! kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo "OzmaDB exited during startup, see $log_file" >&2
        rm -f "$pid_file"
        exit 1
    fi
    sleep 1
done

echo "OzmaDB did not answer $url/ping in time, see $log_file" >&2
exit 1
