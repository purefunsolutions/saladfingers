# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
{
  perSystem = {
    pkgs,
    config,
    craneLib,
    ...
  }: {
    devShells.default = craneLib.devShell {
      inherit (config) checks;
      packages =
        [
          config.treefmt.build.wrapper
          pkgs.cargo-nextest
          pkgs.cargo-audit
          pkgs.skopeo
          pkgs.jq
          pkgs.reuse
        ]
        # Ephemeral-local-Garage backing for the `presign_round_trip` test, so a
        # developer's `cargo nextest run` exercises the same path as CI. Linux-only:
        # garage isn't guaranteed to build on the flake's darwin systems.
        ++ (pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [pkgs.garage]);
    };
  };
}
