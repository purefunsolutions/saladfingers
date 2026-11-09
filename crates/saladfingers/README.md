<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# saladfingers

Rent cheap consumer GPUs from [SaladCloud](https://salad.com) for **minimum billed
seconds**. SaladCloud bills per second, and only while an instance is running.
saladfingers runs a job on a GPU container group, ships its results to object
storage, and tears the group down, so billing stops as soon as the work does.

```sh
cargo install saladfingers
```

The images the jobs run in carry `sf-agent` (the
[`saladfingers-agent`](https://crates.io/crates/saladfingers-agent) crate). The
repository's Nix flake builds them. For the design, the commands and the Nix image
tooling, see the repository: <https://github.com/purefunsolutions/saladfingers>.

## License

Licensed under any of MIT, Apache-2.0, or BSD-3-Clause, at your option.
