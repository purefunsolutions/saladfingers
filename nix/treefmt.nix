# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
{inputs, ...}: {
  imports = [
    inputs.treefmt-nix.flakeModule
  ];

  perSystem = {
    treefmt.config = {
      projectRootFile = "flake.nix";

      settings.global.excludes = [
        "Cargo.lock"
        "flake.lock"
        "saladfingers-images.lock"
        "docs/reference/**"
        "LICENSES/**"
        "target/**"
        "result"
        "result-*"
      ];

      programs = {
        alejandra.enable = true;
        deadnix.enable = true;
        statix.enable = true;
        rustfmt.enable = true;
        shellcheck.enable = true;
        taplo.enable = true;
        yamlfmt.enable = true;
        mdformat.enable = true;
      };
    };
  };
}
