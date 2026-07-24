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
# module; `pkgs` and `name` are injected, so neither can be got wrong.
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
      flake-parts-lib,
      ...
    }: {
      options.perSystem = flake-parts-lib.mkPerSystemOption ({
        config,
        pkgs,
        ...
      }: let
        mkImage = name: def:
          lib.throwIf (def ? name || def ? pkgs)
          "saladfingers.images.${name}: `name` and `pkgs` are supplied by the flakeModule (the package is `${name}-image`); remove them from the definition."
          (mkSaladImage (def // {inherit name pkgs;}));
      in {
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

            Each value is a `mkSaladImage` argument set *minus* `pkgs` and `name`, which
            this module supplies: `tag`, `contents`, `entrypoint`, `cmd`, `gpu` (one of
            `none`, `cuda-min`, `cuda-runtime`, `cuda-full`, `rocm-runtime`),
            `cudaPackages`, `rocmPackages`, `weights`, `env`, `ports`, `extraContents`,
            `maxLayers` — see `nix/image-lib.nix` in saladfingers for what each means.

            `saladfingers.images.<name>` produces `packages.<name>-image`. That name is
            the contract `saladfingers image push <name>` relies on — it runs
            `nix run .#packages.<system>.<name>-image.copyTo` — so defining an image
            here is all that is needed to make it buildable and pushable.
          '';
        };

        config.packages =
          lib.mapAttrs'
          (name: def: lib.nameValuePair "${name}-image" (mkImage name def))
          config.saladfingers.images;
      });
    })
  ];
}
