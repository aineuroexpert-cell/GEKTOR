# src/infrastructure/feature_store.py
"""
[GEKTOR APEX v5.2] Transactional Feature Store for ML Pipeline.

Solves the Feature-Space Synchronization problem:
  At T₀: feature vector X (256 floats → scaled int64) is appended to disk.
  At T₂₀: label y arrives from TripleBarrierLabeler.
  Background stitcher joins (X, y) into training dataset.

Architecture:
  - Feature vectors are written to an append-only memory-mapped file (WAL).
    This is a sequential O(1) write that does NOT block the event loop.
    The OS kernel handles page flushing asynchronously.

  - Features are indexed by intent_id (monotonic key).
    A lightweight in-memory index maps intent_id → file offset.

  - When a label arrives (callback from TripleBarrierLabeler), the
    stitcher reads the feature vector from the mmap at the stored offset
    and writes the complete (X, y) row to the training dataset.

  - The hot trading path NEVER touches the feature store after
    the initial append. Zero latency impact on the execution core.

Memory model:
  - Index: dict[int, int] — intent_id → offset. Max 10k entries.
    Each entry = ~100 bytes → 1MB total. Negligible.
  - Feature WAL: memory-mapped file. OS manages physical pages.
    Only recently written pages are in RAM (working set).
  - Stale features (resolved intents) are marked for compaction.

Crash safety:
  - mmap with MAP_SHARED ensures data survives process crash.
  - On restart, the index is rebuilt from the WAL header scan.
  - Unstitched features (label not yet arrived) are preserved.
"""
from __future__ import annotations

import mmap
import os
import struct
import time
from typing import Final, Any, Callable

import numpy as np
from loguru import logger


# Feature record layout in WAL:
# [8B intent_id] [8B timestamp_ms] [8B n_features] [n_features × 8B values]
# [8B label] [8B exit_px] [8B pnl_bps] [1B is_labeled]
_HEADER_SIZE: Final[int] = 24  # intent_id + ts + n_features
_LABEL_SIZE: Final[int] = 25   # label + exit_px + pnl_bps + is_labeled flag
_HEADER_FMT: Final[str] = "<qqq"  # 3 × int64 little-endian
_LABEL_FMT: Final[str] = "<qqqb"  # 3 × int64 + 1 × bool


class FeatureStore:
    """
    Append-only WAL for feature vectors with deferred label stitching.

    Hot path (T₀): append_features() — O(1) sequential write to mmap.
    Cold path (T₂₀): stitch_label() — O(1) random write to update label.
    Background: export_training_data() — batch read for ML training.

    Memory: index is bounded (max_entries). WAL is OS-managed mmap.
    """
    __slots__ = (
        '_wal_path', '_wal_fd', '_wal_mmap', '_wal_size', '_wal_pos',
        '_index', '_n_features', '_record_size',
        '_max_entries', '_entries_count',
        '_pending_labels',
    )

    def __init__(
        self,
        wal_path: str = "artifacts/feature_wal.bin",
        n_features: int = 64,
        max_entries: int = 50_000,
    ) -> None:
        """
        Args:
            wal_path: Path to the append-only WAL file.
            n_features: Number of features per vector (fixed).
            max_entries: Max feature records before compaction.
        """
        self._wal_path = wal_path
        self._n_features = n_features
        self._max_entries = max_entries
        self._entries_count = 0

        # Record size: header + features + label trailer
        self._record_size = _HEADER_SIZE + (n_features * 8) + _LABEL_SIZE

        # intent_id → offset in WAL
        self._index: dict[int, int] = {}

        # Labels waiting for features (shouldn't happen, but defensive)
        self._pending_labels: dict[int, tuple[int, int, int]] = {}

        # Initialize WAL
        self._wal_fd = -1
        self._wal_mmap: mmap.mmap | None = None
        self._wal_pos = 0
        self._init_wal()

    def _init_wal(self) -> None:
        """Create or open WAL file with mmap."""
        os.makedirs(os.path.dirname(self._wal_path) or ".", exist_ok=True)

        wal_file_size = self._record_size * self._max_entries

        if os.path.exists(self._wal_path):
            file_size = os.path.getsize(self._wal_path)
            if file_size < wal_file_size:
                # Extend file
                with open(self._wal_path, "ab") as f:
                    f.write(b"\x00" * (wal_file_size - file_size))
        else:
            # Create new file
            with open(self._wal_path, "wb") as f:
                f.write(b"\x00" * wal_file_size)

        self._wal_fd = os.open(self._wal_path, os.O_RDWR)

        try:
            self._wal_mmap = mmap.mmap(self._wal_fd, wal_file_size)
        except Exception as e:
            logger.error("💀 [FEATURE_STORE] mmap failed: {}", e)
            os.close(self._wal_fd)
            self._wal_fd = -1
            raise

        # Rebuild index from existing data (crash recovery)
        self._rebuild_index()

        logger.info(
            "📦 [FEATURE_STORE] WAL initialized: {} features × {} max entries "
            "= {:.1f}MB | {} existing records recovered",
            self._n_features, self._max_entries,
            wal_file_size / (1024 * 1024),
            self._entries_count,
        )

    def _rebuild_index(self) -> None:
        """Scan WAL to rebuild in-memory index after crash."""
        if self._wal_mmap is None:
            return

        pos = 0
        max_pos = self._record_size * self._max_entries

        while pos + _HEADER_SIZE <= max_pos:
            header_bytes = self._wal_mmap[pos:pos + _HEADER_SIZE]
            intent_id, ts, n_feat = struct.unpack(_HEADER_FMT, header_bytes)

            if intent_id == 0 and ts == 0:
                # Empty slot — end of valid data
                break

            if n_feat != self._n_features:
                # Corrupted record — stop
                logger.warning(
                    "⚠️ [FEATURE_STORE] Corrupted record at offset {}. "
                    "Stopping recovery.", pos,
                )
                break

            self._index[intent_id] = pos
            self._entries_count += 1
            pos += self._record_size

        self._wal_pos = pos

    def append_features(
        self,
        intent_id: int,
        features: np.ndarray,
        timestamp_ms: int = 0,
    ) -> bool:
        """
        Append feature vector to WAL. O(1) sequential write.

        Called in the HOT PATH at T₀ (signal emission).
        Must be non-blocking. mmap write is a memory copy —
        the OS flushes pages asynchronously.

        Args:
            intent_id: Monotonic intent identifier.
            features: numpy int64 array of shape (n_features,).
            timestamp_ms: Exchange timestamp (0 = auto).

        Returns:
            True if written, False if WAL is full or error.
        """
        if self._wal_mmap is None:
            return False

        if self._entries_count >= self._max_entries:
            logger.warning(
                "⚠️ [FEATURE_STORE] WAL full ({} entries). "
                "Compaction needed.", self._max_entries,
            )
            return False

        if len(features) != self._n_features:
            logger.error(
                "💀 [FEATURE_STORE] Feature dim mismatch: "
                "expected {}, got {}",
                self._n_features, len(features),
            )
            return False

        if timestamp_ms == 0:
            timestamp_ms = int(time.time() * 1000)

        try:
            offset = self._wal_pos

            # Write header
            header = struct.pack(
                _HEADER_FMT,
                intent_id, timestamp_ms, self._n_features,
            )
            self._wal_mmap[offset:offset + _HEADER_SIZE] = header

            # Write features (direct int64 bytes)
            feat_offset = offset + _HEADER_SIZE
            feat_bytes = features.astype(np.int64).tobytes()
            feat_end = feat_offset + len(feat_bytes)
            self._wal_mmap[feat_offset:feat_end] = feat_bytes

            # Write empty label trailer
            label_offset = feat_end
            label_data = struct.pack(_LABEL_FMT, 0, 0, 0, 0)
            self._wal_mmap[label_offset:label_offset + _LABEL_SIZE] = label_data

            # Update index
            self._index[intent_id] = offset
            self._wal_pos += self._record_size
            self._entries_count += 1

            # Check if a label was already waiting (race: label arrived first)
            if intent_id in self._pending_labels:
                label, exit_px, pnl_bps = self._pending_labels.pop(intent_id)
                self._write_label_at(offset, label, exit_px, pnl_bps)

            return True

        except Exception as e:
            logger.error("💀 [FEATURE_STORE] Write error: {}", e)
            return False

    def stitch_label(
        self,
        intent_id: int,
        label: int,
        exit_px: int = 0,
        pnl_bps: int = 0,
    ) -> bool:
        """
        Stitch label to feature vector. O(1) random write.

        Called at T₂₀ when TripleBarrierLabeler resolves an intent.
        Looks up the feature record by intent_id and writes the label
        into the pre-allocated trailer section.

        Args:
            intent_id: Intent to label.
            label: +1 (TP), -1 (SL), 0 (vertical).
            exit_px: Realized exit price (scaled).
            pnl_bps: Realized P&L in basis points.

        Returns:
            True if stitched, False if feature not found.
        """
        if self._wal_mmap is None:
            return False

        offset = self._index.get(intent_id)
        if offset is None:
            # Feature not yet written — queue the label
            self._pending_labels[intent_id] = (label, exit_px, pnl_bps)
            return False

        return self._write_label_at(offset, label, exit_px, pnl_bps)

    def _write_label_at(
        self, offset: int, label: int, exit_px: int, pnl_bps: int,
    ) -> bool:
        """Write label into the trailer of a feature record."""
        try:
            label_offset = offset + _HEADER_SIZE + (self._n_features * 8)
            label_data = struct.pack(_LABEL_FMT, label, exit_px, pnl_bps, 1)
            self._wal_mmap[label_offset:label_offset + _LABEL_SIZE] = label_data
            return True
        except Exception as e:
            logger.error("💀 [FEATURE_STORE] Label write error: {}", e)
            return False

    def export_training_data(
        self, only_labeled: bool = True,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Batch export for ML training. Called from background thread.

        Returns:
            (X, y) where X is (N, n_features) int64 and y is (N,) int64.
        """
        if self._wal_mmap is None:
            return np.empty((0, self._n_features), dtype=np.int64), np.empty(0, dtype=np.int64)

        features_list = []
        labels_list = []

        for intent_id, offset in self._index.items():
            # Read label trailer
            label_offset = offset + _HEADER_SIZE + (self._n_features * 8)
            label_bytes = self._wal_mmap[label_offset:label_offset + _LABEL_SIZE]
            label, exit_px, pnl_bps, is_labeled = struct.unpack(_LABEL_FMT, label_bytes)

            if only_labeled and not is_labeled:
                continue

            # Read features
            feat_offset = offset + _HEADER_SIZE
            feat_bytes = self._wal_mmap[feat_offset:feat_offset + self._n_features * 8]
            feat_array = np.frombuffer(feat_bytes, dtype=np.int64).copy()

            features_list.append(feat_array)
            labels_list.append(label)

        if not features_list:
            return np.empty((0, self._n_features), dtype=np.int64), np.empty(0, dtype=np.int64)

        X = np.stack(features_list)
        y = np.array(labels_list, dtype=np.int64)
        return X, y

    def compaction(self) -> int:
        """
        Remove labeled records from WAL and rebuild.

        Called from background maintenance task. NOT from hot path.

        Returns:
            Number of records compacted (removed).
        """
        if self._wal_mmap is None:
            return 0

        # Identify unlabeled records to keep
        keep: list[tuple[int, int]] = []  # (intent_id, offset)

        for intent_id, offset in self._index.items():
            label_offset = offset + _HEADER_SIZE + (self._n_features * 8)
            label_bytes = self._wal_mmap[label_offset:label_offset + _LABEL_SIZE]
            _, _, _, is_labeled = struct.unpack(_LABEL_FMT, label_bytes)

            if not is_labeled:
                keep.append((intent_id, offset))

        removed = self._entries_count - len(keep)

        if removed == 0:
            return 0

        # Read records to keep
        kept_data: list[bytes] = []
        for _, offset in keep:
            record = bytes(self._wal_mmap[offset:offset + self._record_size])
            kept_data.append(record)

        # Rewrite WAL
        self._wal_mmap.seek(0)
        new_pos = 0
        new_index: dict[int, int] = {}

        for i, (intent_id, _) in enumerate(keep):
            self._wal_mmap[new_pos:new_pos + self._record_size] = kept_data[i]
            new_index[intent_id] = new_pos
            new_pos += self._record_size

        # Zero remaining space
        remaining = (self._max_entries * self._record_size) - new_pos
        if remaining > 0:
            self._wal_mmap[new_pos:new_pos + remaining] = b"\x00" * remaining

        self._index = new_index
        self._wal_pos = new_pos
        self._entries_count = len(keep)

        logger.info(
            "🗜️ [FEATURE_STORE] Compacted {} records. {} remaining.",
            removed, self._entries_count,
        )
        return removed

    def close(self) -> None:
        """Flush and close WAL."""
        if self._wal_mmap is not None:
            try:
                self._wal_mmap.flush()
                self._wal_mmap.close()
            except Exception as e:
                logger.warning(f"[FeatureStore] WAL mmap close error: {e}")
        if self._wal_fd >= 0:
            try:
                os.close(self._wal_fd)
            except Exception as e:
                logger.warning(f"[FeatureStore] WAL fd close error: {e}")
        self._wal_mmap = None
        self._wal_fd = -1

    @property
    def entries_count(self) -> int:
        return self._entries_count

    @property
    def labeled_count(self) -> int:
        """Count of stitched (labeled) records."""
        count = 0
        if self._wal_mmap is None:
            return 0
        for offset in self._index.values():
            label_offset = offset + _HEADER_SIZE + (self._n_features * 8)
            is_labeled = self._wal_mmap[label_offset + 24]  # Last byte
            if is_labeled:
                count += 1
        return count
