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
      packages = [
        config.treefmt.build.wrapper
        pkgs.cargo-nextest
        pkgs.cargo-audit
        pkgs.skopeo
        pkgs.jq
        pkgs.reuse
      ];
    };
  };
}
