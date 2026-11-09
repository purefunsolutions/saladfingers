<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions

SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause
-->

# saladfingers-protocol

The wire contract between the [`saladfingers`](https://crates.io/crates/saladfingers)
CLI and `sf-agent`, the agent inside every saladfingers image: `JobSpec`,
`ResultEnvelope` and the session HTTP API. The default
`transfer` feature adds the artifact transfer format both sides use: a
`tar | zstd` stream split into numbered parts, moved through presigned URLs.

Part of saladfingers: <https://github.com/purefunsolutions/saladfingers>.

## License

Licensed under any of MIT, Apache-2.0, or BSD-3-Clause, at your option.
