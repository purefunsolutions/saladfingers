# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
# mkCargoTestBinaries — compile a workspace's `cargo test` executables (without
# running them) and harvest them, plus a `run-all-tests` runner, into $out/bin.
#
# This is how GPU-gated tests reach rented hardware: a consumer (e.g. infurer)
# calls this with its own crane machinery, and the harvested test binaries' RPATHs
# drag the exact CUDA store paths into the image closure — so no cargo, nextest, or
# source ever ships in the image. Drop the result into an image's `contents`; the
# binaries land at /bin, and `sf-agent run` execs the JobSpec command
# (`/bin/run-all-tests --ignored`).
#
# Args: pkgs, craneLib, commonArgs, cargoArtifacts (the consumer's), plus
#   testPackages : list of `-p` package names whose tests to compile
#   pname        : derivation name
{
  pkgs,
  craneLib,
  commonArgs,
  cargoArtifacts,
  testPackages,
  pname ? "cargo-test-binaries",
}: let
  inherit (pkgs) lib;
  pkgFlags = lib.concatMapStringsSep " " (p: "-p ${lib.escapeShellArg p}") testPackages;
in
  craneLib.mkCargoDerivation (commonArgs
    // {
      inherit cargoArtifacts pname;
      version = commonArgs.version or "0.1.0";
      # We ship test binaries, not cargo state.
      doInstallCargoArtifacts = false;

      # Compile the test harnesses (no execution — the GPU is on the rented node)
      # and record the compiler's JSON so we can find the executables.
      buildPhaseCargoCommand = ''
        cargo test --release --no-run --message-format json ${pkgFlags} \
          | tee "$TMPDIR/cargo-test.json" > /dev/null
      '';

      # Harvest every test executable (integration + unit-test harnesses), strip
      # cargo's metadata-hash suffix, and emit a runner that forwards args.
      installPhaseCommand = ''
        mkdir -p "$out/bin"
        names=""
        for exe in $(${pkgs.jq}/bin/jq -r \
          'select(.reason=="compiler-artifact" and .profile.test==true and .executable!=null) | .executable' \
          "$TMPDIR/cargo-test.json"); do
          base="$(basename "$exe" | sed 's/-[0-9a-f]\{16,\}$//')"
          install -Dm755 "$exe" "$out/bin/$base"
          names="$names $base"
        done
        if [ -z "$names" ]; then
          echo "mkCargoTestBinaries: no test executables harvested for ${pname}" >&2
          exit 1
        fi
        {
          echo '#!/bin/sh'
          echo '# Run every harvested test binary, forwarding args (e.g. --ignored).'
          echo 'set -u; rc=0'
          for n in $names; do
            printf 'echo "== %s =="; /bin/%s "$@" || rc=$?\n' "$n" "$n"
          done
          echo 'exit "$rc"'
        } > "$out/bin/run-all-tests"
        chmod +x "$out/bin/run-all-tests"
      '';
    })
