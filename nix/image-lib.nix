# SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
#
# SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
# mkSaladImage — the one image constructor.
#
# Every image gets: `sf-agent` at /bin/sf-agent as its entrypoint, busybox (so
# `container.command = ["/bin/sh","-c",...]` always works), CA certs, the FHS
# dynamic-loader symlink (host-injected FHS binaries like nvidia-smi need
# /lib64/ld-linux-x86-64.so.2), and writable /work + /tmp. GPU userspace is
# layered on by flavor:
#   none | cuda-min | cuda-runtime | cuda-full | rocm-runtime
# The NVIDIA/AMD *driver* is injected by the host; images bring only userspace.
{
  pkgs,
  n2c,
  sfAgent,
}: let
  inherit (pkgs) lib;

  # Host driver-injection dirs (see the file for the full rationale). Baked GPU
  # userspace is deliberately NOT here — it resolves via nix RPATHs.
  injectedLibDirs = import ./injected-lib-dirs.nix;

  gpuLibs = {
    cudaPackages,
    rocmPackages,
    flavor,
  }:
    {
      none = [];
      # Just the CUDA runtime API. Enough for images whose binaries only link
      # libcudart and launch their own kernels (e.g. AOT-compiled kernel tests);
      # the driver (libcuda.so.1) is host-injected. No math libs → ~10× smaller
      # than cuda-full, so cold starts are much faster.
      cuda-min = map (lib.getOutput "lib") (with cudaPackages; [cuda_cudart]);
      cuda-runtime = map (lib.getOutput "lib") (with cudaPackages; [cuda_cudart libcublas cuda_nvrtc libcurand]);
      cuda-full = map (lib.getOutput "lib") (with cudaPackages; [cuda_cudart libcublas cuda_nvrtc libcurand cudnn]);
      # The ROCm userspace: HIP runtime (clr) + the query tools. Everything these need
      # at run time (comgr, LLVM, libelf, libz, ...) resolves via nix RPATHs from their
      # closure — the image must NOT force these onto LD_LIBRARY_PATH (see `ldPath`).
      rocm-runtime = with rocmPackages; [clr rocminfo rocm-smi];
    }.${
      flavor
    };

  # /bin: sf-agent + busybox applets (incl. sh, so `container.command =
  # ["/bin/sh","-c",...]` always works). Runtime deps ride along in the image
  # closure computed by nix2container.
  baseRoot = pkgs.buildEnv {
    name = "sf-base-root";
    paths = [sfAgent pkgs.busybox];
    pathsToLink = ["/bin"];
  };

  # Minimal /etc + writable dirs. /etc must exist and be writable: the NVIDIA
  # prestart hook regenerates /etc/ld.so.cache in-container and aborts the start
  # if it can't. The /lib64 loader symlink lets host-injected FHS glibc binaries
  # (nvidia-smi & co.) actually execute; glibc is already in the closure.
  etcRoot = pkgs.runCommand "sf-etc-root" {} ''
    mkdir -p $out/etc/ssl/certs $out/tmp $out/work $out/root $out/lib64
    ln -s ${pkgs.glibc}/lib/ld-linux-x86-64.so.2 $out/lib64/ld-linux-x86-64.so.2
    cp ${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt $out/etc/ssl/certs/ca-bundle.crt
    printf 'root:x:0:0:root:/root:/bin/sh\nnobody:x:65534:65534:nobody:/:/bin/false\n' > $out/etc/passwd
    printf 'root:x:0:\nnobody:x:65534:\n' > $out/etc/group
  '';
in
  {
    name,
    tag ? null,
    contents ? [],
    entrypoint ? ["/bin/sf-agent"],
    cmd ? ["serve"],
    gpu ? "none",
    cudaPackages ? null,
    rocmPackages ? null,
    weights ? [],
    env ? {},
    ports ? [],
    extraContents ? [],
    maxLayers ? 40,
  }: let
    libs = gpuLibs {inherit cudaPackages rocmPackages flavor;};
    flavor = gpu;

    gpuLayer = lib.optional (libs != []) (n2c.buildLayer {
      deps = libs;
      maxLayers = 8;
    });

    # One layer per baked-weights entry: fixed-output store paths dedup across
    # image versions, so a weights blob uploads once, ever.
    bakedWeights = builtins.filter (w: w ? source) weights;
    weightLayers = map (w:
      n2c.buildLayer {
        copyToRoot = [
          (pkgs.buildEnv {
            name = "weights-${baseNameOf w.targetDir}";
            paths = [w.source];
            extraPrefix = w.targetDir;
          })
        ];
      })
    bakedWeights;

    # LD_LIBRARY_PATH = ONLY the host driver-injection dirs. Baked GPU userspace resolves via
    # the nix RPATHs the toolchain baked into each binary; listing store paths here breaks the
    # loader's RPATH walk for deep transitive deps (a HIP kernel dies on `libz.so.1: cannot
    # open`) — AND it is futile on SaladCloud's AMD nodes, whose host injection OVERRIDES the
    # image's LD_LIBRARY_PATH outright (verified live: our store-path prefix never appears in
    # the running container). These injection dirs are the only way host-injected libs
    # (libcuda.so.1, libnvidia-ml.so.1, and the WSL librocdxg.so dispatch backend) resolve —
    # nix glibc ignores /etc/ld.so.cache. Binaries INHERIT this global plus whatever the host
    # appends; they are NOT wrapped with `--set` (which would discard the host's librocdxg).
    # The one wrinkle — the host injects a whole ROCm into /opt/rocm-host/lib whose libhsa
    # shadows ours for the nixpkgs query tools (rocminfo/rocm-smi use DT_RUNPATH, so
    # LD_LIBRARY_PATH wins) — is handled per-binary in nix/images.nix (rocmTools), a runtime
    # wrapper being the only LD_LIBRARY_PATH tweak that survives the injection override.
    ldPath = lib.concatStringsSep ":" injectedLibDirs;

    envList =
      [
        "PATH=/bin:/usr/bin:/usr/local/bin:/usr/local/nvidia/bin"
        "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
        "LD_LIBRARY_PATH=${ldPath}"
        # Injection triggers on NVIDIA_VISIBLE_DEVICES in the merged OCI env;
        # bake the conventional defaults so images work even where the platform
        # doesn't set them. Per-image env below can override (later wins).
        "NVIDIA_VISIBLE_DEVICES=all"
        "NVIDIA_DRIVER_CAPABILITIES=compute,utility"
      ]
      ++ lib.mapAttrsToList (k: v: "${k}=${v}") env
      ++ lib.optionals (weights != []) (map (w: "${w.envVar}=${w.targetDir}")
        (builtins.filter (w: (w ? envVar) && w.envVar != null) weights));
  in
    n2c.buildImage ({
        inherit name maxLayers;
        copyToRoot = [baseRoot etcRoot] ++ contents ++ extraContents;
        layers = gpuLayer ++ weightLayers;
        config = {
          Entrypoint = entrypoint;
          Cmd = cmd;
          Env = envList;
          WorkingDir = "/work";
          User = "root";
          ExposedPorts = builtins.listToAttrs (map (p: {
              name = "${toString p}/tcp";
              value = {};
            })
            ports);
          Labels = {"org.opencontainers.image.source" = "saladfingers";};
        };
      }
      // lib.optionalAttrs (tag != null) {inherit tag;})
