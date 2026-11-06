#!/usr/bin/env bash
# SPDX-License-Identifier: MPL-2.0
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: bash scripts/build-release.sh <linux-musl-target> <tcp|quic>" >&2
    exit 2
fi

target=$1
variant=$2
case "$target" in
    x86_64-unknown-linux-musl|aarch64-unknown-linux-musl) ;;
    *) echo "Unsupported release target: $target" >&2; exit 2 ;;
esac

features=(--no-default-features)
case "$variant" in
    tcp) ;;
    quic) features+=(--features quic) ;;
    *) echo "Unsupported release variant: $variant" >&2; exit 2 ;;
esac

# The workflow uses a native runner for each architecture. Both Rust and ring's
# C code must use the musl toolchain, and the CRT must be linked statically.
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc
export CC_x86_64_unknown_linux_musl=musl-gcc
export CC_aarch64_unknown_linux_musl=musl-gcc
export RUSTFLAGS="${RUSTFLAGS:-} -C target-feature=+crt-static"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}"

cargo build --locked --release --target "$target" "${features[@]}"
binary="$CARGO_TARGET_DIR/$target/release/mqtt_ring_bench"

# A static PIE may have a dynamic section, but it must have neither a program
# interpreter nor dependencies on shared libraries. Read ELF metadata directly.
program_headers=$(readelf --program-headers --wide "$binary")
dynamic_section=$(readelf --dynamic --wide "$binary")
if grep -q 'INTERP' <<< "$program_headers" || grep -q '(NEEDED)' <<< "$dynamic_section"; then
    echo "Release binary is not fully statically linked: $binary" >&2
    exit 1
fi
"$binary" --help > /dev/null

version=$(cargo metadata --locked --no-deps --format-version 1 | python3 -c '
import json, sys
metadata = json.load(sys.stdin)
print(next(package["version"] for package in metadata["packages"] if package["name"] == "mqtt_ring_bench"))
')
archive_name="mqtt_ring_bench-${version}-${target}-${variant}"
dist="$CARGO_TARGET_DIR/release-dist"
mkdir -p "$dist"
staging=$(mktemp -d)
trap 'rm -rf "$staging"' EXIT
mkdir "$staging/$archive_name"
install -m 755 "$binary" "$staging/$archive_name/mqtt_ring_bench"
install -m 644 README.md LICENSE "$staging/$archive_name/"
tar -czf "$dist/$archive_name.tar.gz" -C "$staging" "$archive_name"
(
    cd "$dist"
    sha256sum "$archive_name.tar.gz" > "$archive_name.tar.gz.sha256"
    sha256sum --check "$archive_name.tar.gz.sha256"
)
echo "Release archive: $dist/$archive_name.tar.gz"
