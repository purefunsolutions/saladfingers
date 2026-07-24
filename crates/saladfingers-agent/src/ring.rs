// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! Bounded per-exec output ring.
//!
//! Retains the last `capacity` bytes of an exec's merged stdout/stderr as
//! offset-tagged segments, so the `/v1/exec/{id}/output` long-poll can serve any
//! byte cursor and report `truncated` when the cursor fell off the back of the ring.
//! Offsets are absolute over the exec's whole lifetime (they never reset on eviction).

use std::collections::VecDeque;

use base64::Engine;
use saladfingers_protocol::agent_api::{OutputChunk, Stream};

/// One contiguous run of bytes from a single stream.
struct Segment {
    stream: Stream,
    offset: u64,
    data: Vec<u8>,
}

/// A bounded ring of exec output, addressed by absolute byte offset.
pub struct OutputRing {
    capacity: usize,
    segments: VecDeque<Segment>,
    /// Offset of the oldest byte still retained (advances as segments are evicted).
    base_offset: u64,
    /// Total bytes ever written == the offset the next byte will get.
    total: u64,
    /// Bytes currently retained across all segments.
    buffered: usize,
}

impl OutputRing {
    /// A new empty ring retaining at most `capacity` bytes.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            segments: VecDeque::new(),
            base_offset: 0,
            total: 0,
            buffered: 0,
        }
    }

    /// Total bytes ever written (the offset just past the newest byte).
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Append bytes from `stream`, evicting the oldest data beyond `capacity`.
    pub fn push(&mut self, stream: Stream, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        self.segments.push_back(Segment {
            stream,
            offset: self.total,
            data: data.to_vec(),
        });
        self.total += data.len() as u64;
        self.buffered += data.len();
        // Evict whole segments while more than one remains over capacity.
        while self.buffered > self.capacity && self.segments.len() > 1 {
            let seg = self.segments.pop_front().expect("len > 1");
            self.buffered -= seg.data.len();
        }
        // If a single retained segment still exceeds capacity, trim its head.
        if self.buffered > self.capacity
            && let Some(front) = self.segments.front_mut()
        {
            let overflow = self.buffered - self.capacity;
            if overflow < front.data.len() {
                front.data.drain(0..overflow);
                front.offset += overflow as u64;
                self.buffered -= overflow;
            }
        }
        self.base_offset = self.segments.front().map_or(self.total, |s| s.offset);
    }

    /// Chunks at or after `cursor`, plus the next cursor and whether the ring had
    /// already evicted bytes the caller asked for (`truncated`).
    #[must_use]
    pub fn read_from(&self, cursor: u64) -> (Vec<OutputChunk>, u64, bool) {
        let truncated = cursor < self.base_offset;
        let start = cursor.max(self.base_offset);
        let mut chunks = Vec::new();
        for seg in &self.segments {
            let seg_end = seg.offset + seg.data.len() as u64;
            if seg_end <= start {
                continue;
            }
            let from = start.saturating_sub(seg.offset) as usize;
            let bytes = &seg.data[from.min(seg.data.len())..];
            if bytes.is_empty() {
                continue;
            }
            chunks.push(OutputChunk {
                stream: seg.stream,
                offset: seg.offset + from as u64,
                data_b64: base64::engine::general_purpose::STANDARD.encode(bytes),
            });
        }
        (chunks, self.total, truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(chunk: &OutputChunk) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(&chunk.data_b64)
            .expect("valid base64")
    }

    #[test]
    fn reads_from_a_cursor_and_advances() {
        let mut ring = OutputRing::new(1024);
        ring.push(Stream::Stdout, b"hello ");
        ring.push(Stream::Stderr, b"world");
        let (chunks, next, truncated) = ring.read_from(0);
        assert!(!truncated);
        assert_eq!(next, 11);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].stream, Stream::Stdout);
        assert_eq!(decode(&chunks[0]), b"hello ");
        assert_eq!(chunks[1].stream, Stream::Stderr);
        assert_eq!(chunks[1].offset, 6);

        // A cursor mid-stream returns only the tail.
        let (chunks, _, truncated) = ring.read_from(6);
        assert!(!truncated);
        assert_eq!(chunks.len(), 1);
        assert_eq!(decode(&chunks[0]), b"world");

        // A cursor at the end returns nothing.
        let (chunks, next, _) = ring.read_from(11);
        assert!(chunks.is_empty());
        assert_eq!(next, 11);
    }

    #[test]
    fn evicts_oldest_and_flags_truncation() {
        let mut ring = OutputRing::new(10);
        ring.push(Stream::Stdout, b"aaaaa"); // offsets 0..5
        ring.push(Stream::Stdout, b"bbbbb"); // offsets 5..10
        ring.push(Stream::Stdout, b"ccccc"); // offsets 10..15 → evicts "aaaaa"
        assert_eq!(ring.total(), 15);
        // Reading from 0 is truncated (0..5 evicted); we still get the retained tail.
        let (chunks, next, truncated) = ring.read_from(0);
        assert!(truncated);
        assert_eq!(next, 15);
        let got: Vec<u8> = chunks.iter().flat_map(decode).collect();
        assert_eq!(got, b"bbbbbccccc");
        // Reading from a retained offset is not truncated.
        let (_, _, truncated) = ring.read_from(5);
        assert!(!truncated);
    }

    #[test]
    fn trims_a_single_oversized_segment() {
        let mut ring = OutputRing::new(4);
        ring.push(Stream::Stdout, b"0123456789"); // one 10-byte segment into a 4-byte ring
        assert_eq!(ring.total(), 10);
        let (chunks, next, truncated) = ring.read_from(0);
        assert!(truncated);
        assert_eq!(next, 10);
        // Only the last 4 bytes survive, at their true offsets.
        let bytes: Vec<u8> = chunks.iter().flat_map(decode).collect();
        assert_eq!(bytes, b"6789");
        assert_eq!(chunks[0].offset, 6);
    }
}
