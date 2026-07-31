# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
# The saladfingers flake-parts module: `saladfingers.images.<name>` builds
# `packages.<name>-image` with mkSaladImage.
#
# A consumer adds it to their flake-parts `imports`:
#
#     imports = [inputs.saladfingers.flakeModules.default];
#     perSystem = {pkgs, ...}: {
#       saladfingers.images.kernel-test = {
#         gpu = "cuda-min";
#         cudaPackages = inf.cudaPackages;
#         contents = [kernelTests];
#       };
#     };
#
# The `-image` suffix is a HARD CONTRACT: `saladfingers image push <name>` runs
# `nix run <root>#packages.<system>.<name>-image.copyTo` (see
# crates/saladfingers-cli/src/image.rs). Deriving that package name from the attribute
# key — instead of leaving it to the consumer to spell correctly — is the point of this
# module; `pkgs`, `nativePkgs` and `name` are injected, so none can be got wrong.
#
# Images are declared once, under `saladfingers.imageSystem` (default x86_64-linux — the
# only platform SaladCloud runs), but the resulting `<name>-image` package is emitted in
# EVERY system of the consumer's flake. In the image system it is built the plain way; in
# every other system it is the same target image assembled natively by that system
# (`nativePkgs`), which is what lets `nix build .#packages.aarch64-darwin.foo-image` and
# `saladfingers image push` work on a Mac with no Linux builder. See nix/image-lib.nix
# for why cross-assembly is sound.
#
# `mkSaladImage` is applied by saladfingers' own flake.nix, so it closes over
# *saladfingers'* `self` and `inputs`, not the importing flake's: the consumer gets the
# prebuilt `sf-agent` and nix2container from saladfingers' locked inputs and needs
# neither nix2container in their own inputs nor a rebuild of the agent.
{mkSaladImage}: {
  # A stable key so that importing both `flakeModules.default` and its
  # `flakeModules.images` alias deduplicates, instead of declaring
  # `perSystem.saladfingers.images` twice (which the module system rejects).
  key = "saladfingers#flakeModules.images";
  _class = "flake";

  imports = [
    ({
      lib,
      config,
      withSystem,
      flake-parts-lib,
      ...
    }: let
      # Shared by both emissions below so their argument handling and error text cannot
      # drift apart.
      mkImage = {
        name,
        def,
        pkgs,
        nativePkgs,
      }:
        lib.throwIf (def ? name || def ? pkgs || def ? nativePkgs)
        "saladfingers.images.${name}: `name`, `pkgs` and `nativePkgs` are supplied by the flakeModule (the package is `${name}-image`); remove them from the definition."
        (mkSaladImage (def // {inherit name pkgs nativePkgs;}));

      inherit (config.saladfingers) imageSystem;
      # Bound out here because the cross-system emission below shadows `config` with the
      # per-system one.
      flakeSystems = config.systems;
    in {
      options.saladfingers.imageSystem = lib.mkOption {
        type = lib.types.str;
        default = "x86_64-linux";
        description = ''
          The system whose `saladfingers.images` definitions are built, i.e. the platform
          the images themselves target. SaladCloud is linux/amd64 only, so the default is
          the right answer for every deployed image.

          Declaring images under this system also makes them available as
          `packages.<other-system>.<name>-image`, assembled natively by that other system
          — that is how a macOS host builds and pushes a linux/amd64 image without a Linux
          builder.
        '';
      };

      options.perSystem = flake-parts-lib.mkPerSystemOption ({
        config,
        pkgs,
        ...
      }: {
        options.saladfingers.images = lib.mkOption {
          # `lazyAttrsOf` twice, over `raw`, on purpose. Image definitions hold
          # derivations, whole package *sets* and functions, and both of the obvious
          # alternatives mishandle them: `types.attrsOf` forces every value to WHNF just
          # to compute the attribute names, and `types.anything` recursively merges any
          # plain attrset it is given — passing `rocmPackages = pkgs.rocmPackages` then
          # evaluates every attribute in that scope and dies on a deprecated one
          # (`clang-ocl`, verified). `raw` hands each value to mkSaladImage untouched.
          type = lib.types.lazyAttrsOf (lib.types.lazyAttrsOf lib.types.raw);
          default = {};
          example = lib.literalExpression ''
            {
              kernel-test = {
                gpu = "cuda-min";
                cudaPackages = inf.cudaPackages;
                contents = [kernelTests];
              };
            }
          '';
          description = ''
            SaladCloud container images to build, keyed by image name.

            Each value is a `mkSaladImage` argument set *minus* `pkgs`, `nativePkgs` and
            `name`, which this module supplies: `tag`, `contents`, `entrypoint`, `cmd`,
            `gpu` (one of `none`, `cuda-min`, `cuda-runtime`, `cuda-full`,
            `rocm-runtime`), `cudaPackages`, `rocmPackages`, `weights`, `env`, `ports`,
            `extraContents`, `maxLayers`, `arch` — see `nix/image-lib.nix` in
            saladfingers for what each means.

            `saladfingers.images.<name>` produces `packages.<name>-image`. That name is
            the contract `saladfingers image push <name>` relies on — it runs
            `nix run .#packages.<system>.<name>-image.copyTo` — so defining an image
            here is all that is needed to make it buildable and pushable.
          '';
        };

        config.packages =
          lib.mapAttrs'
          (name: def:
            lib.nameValuePair "${name}-image" (mkImage {
              inherit name def pkgs;
              nativePkgs = pkgs;
            }))
          config.saladfingers.images;
      });

      # The cross-system emission: in every system that is NOT the image system, re-expose
      # the image system's images, assembled by this system.
      #
      # `withSystem` must come from the TOP-LEVEL module args — inside `perSystem` it is
      # deliberately an error (flake-parts sets it to a throwing alias there), because
      # reaching across systems is exactly what perSystem is meant to prevent. Skipping
      # `system == imageSystem` leaves that system's packages to the definition above,
      # rather than defining `packages` twice for it. The `elem` guard makes this a no-op
      # for a flake whose `systems` does not include the image system at all (e.g. a
      # linux-only consumer), so nothing about their evaluation changes.
      #
      # Names this system declares itself are skipped, and that is not merely defensive:
      # a consumer's `perSystem` runs for *every* system, so unless they gate their image
      # definitions (as nix/images.nix does) the same name is declared here too, and
      # emitting both would be a duplicate `packages.<name>-image` definition — a module
      # error, not a silent override. A locally declared image wins in its own system.
      config.perSystem = {
        system,
        pkgs,
        config,
        ...
      }: {
        packages =
          lib.optionalAttrs
          (system != imageSystem && builtins.elem imageSystem flakeSystems)
          (withSystem imageSystem (target:
            lib.mapAttrs'
            (name: def:
              lib.nameValuePair "${name}-image" (mkImage {
                inherit name def;
                inherit (target) pkgs;
                nativePkgs = pkgs;
              }))
            (builtins.removeAttrs target.config.saladfingers.images
              (builtins.attrNames config.saladfingers.images))));
      };
    })
  ];
}
