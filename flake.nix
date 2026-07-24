# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
{
  description = "saladfingers — rent SaladCloud consumer GPUs; build and push the images the jobs run";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nix2container = {
      url = "github:nlewo/nix2container";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs @ {
    flake-parts,
    self,
    ...
  }: let
    # Build a saladfingers image with the caller's pkgs + CUDA/ROCm userspace,
    # reusing saladfingers' prebuilt sf-agent and nix2container. Closing over *this*
    # flake's `self`/`inputs` is what lets a consumer skip both.
    mkSaladImage = {pkgs, ...} @ args: let
      system = pkgs.stdenv.hostPlatform.system;
    in
      import ./nix/image-lib.nix {
        inherit pkgs;
        n2c = inputs.nix2container.packages.${system}.nix2container;
        sfAgent = self.packages.${system}.sf-agent;
      } (builtins.removeAttrs args ["pkgs"]);

    # The flake-parts module over mkSaladImage. Bound here rather than read back from
    # `config.flake.flakeModules` (which `imports` may not do) so this flake can both
    # import it — nix/images.nix defines saladfingers' own images through it, so CI
    # exercises the consumer path — and export it below.
    imagesModule = import ./nix/flake-module.nix {inherit mkSaladImage;};
  in
    flake-parts.lib.mkFlake {inherit inputs;} {
      imports = [
        # Declares the `flake.flakeModules` output (with `_class`/`key` wrapping).
        flake-parts.flakeModules.flakeModules
        ./nix
        imagesModule
      ];
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      # Consumer API (e.g. infurer): build GPU-test images without re-deriving the
      # agent or nix2container. Both are lazy functions — `nix flake check` never
      # forces them, so this adds no build to saladfingers itself.
      flake.lib = {
        # Harvest a workspace's `cargo test` binaries into an image-ready $out/bin.
        mkCargoTestBinaries = import ./nix/lib.nix;
        inherit mkSaladImage;
      };

      # The ergonomic form of the same thing, for flake-parts consumers:
      # `saladfingers.images.<name>` → `packages.<name>-image`.
      flake.flakeModules = {
        default = imagesModule;
        images = imagesModule;
      };
    };
}
