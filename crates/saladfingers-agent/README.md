<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# saladfingers-agent

`sf-agent`, the small binary baked into every
[saladfingers](https://crates.io/crates/saladfingers) image. It runs a job and
uploads its results (`run`), serves an interactive session (`serve`), or reports
the GPU it landed on (`probe`).

The repository's Nix image library puts it into images for you. To build an image
another way, install it with:

```sh
cargo install saladfingers-agent
```

Part of saladfingers: <https://github.com/purefunsolutions/saladfingers>.

## License

Licensed under any of MIT, Apache-2.0, or BSD-3-Clause, at your option.
