"""Dedicated durable Raft journal, indexed by absolute log position."""

import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import struct

from .platform_support import WINDOWS, atomic_write, lock_directory, sync_file
from .journal import (
    CorruptionError, DEFAULT_SEGMENT_SIZE, Generations, Journal, ensure_directory,
)


META, HARD, EDIT = 1, 2, 3
U64 = struct.Struct(">Q")
ENTRY = struct.Struct(">QBII")
BATCH_SIZE = 16
MAX_INDEX = 2**63 - 1
SNAPSHOT_FILE = "snapshot.bin"
MAX_OBSOLETE_FRAMES = 512


def encode_json(data):
    return json.dumps(data, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def unsigned(value):
    return type(value) is int and 0 <= value <= MAX_INDEX


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(256 * 1024), b""):
            digest.update(chunk)
        return source.tell(), digest.hexdigest()


def valid_snapshot(info):
    return (
        isinstance(info, dict) and set(info) == {"index", "term", "size", "digest"}
        and all(unsigned(info[key]) and info[key] > 0 for key in ("index", "term", "size"))
        and isinstance(info["digest"], str)
        and re.fullmatch(r"[0-9a-f]{64}", info["digest"]) is not None
    )


class RaftLog:
    def __init__(self, journal, *, repair=False):
        self.journal = journal
        self.identity = None
        self.snapshot = None
        self.base = self.base_term = self.commit = self.term = 0
        self.vote = None
        # Terms permit election and conflict checks without reading entry bodies.
        self.entries = []
        self.record_count = 0
        for location, kind, data in journal.frames(repair=repair):
            self.accept(location, kind, data)

    @property
    def last_index(self):
        return self.base + len(self.entries)

    def term_at(self, index):
        if index == self.base:
            return self.base_term
        if not self.base < index <= self.last_index:
            raise IndexError(index)
        return self.entries[index - self.base - 1][0]

    def __getitem__(self, index):
        if index == self.base:
            return {"term": self.base_term}
        return self.batch(index, 1)[0]

    def batch(self, first, count):
        if not self.base < first <= self.last_index + 1:
            raise IndexError(first)
        entries = []
        previous = data = None
        offset = first - self.base - 1
        for _, location, start, end in self.entries[offset:offset + count]:
            if location != previous:
                kind, data = self.journal.read(location)
                if kind != EDIT:
                    raise CorruptionError("Raft index points to a non-entry frame")
                previous = location
            term, legacy, id_size, sql_size = ENTRY.unpack_from(data, start)
            entry = {"term": term}
            if id_size:
                body = start + ENTRY.size
                entry["request_id"] = data[body:body + id_size].decode("ascii")
                entry["sql"] = data[end - sql_size:end].decode("utf-8")
            if legacy:
                entry["legacy"] = True
            entries.append(entry)
        return entries

    def accept(self, location, kind, data):
        self.record_count += 1
        if kind == META and self.identity is None:
            identity, snapshot = json.loads(data)
            if (
                not isinstance(identity, dict) or set(identity) != {"version", "cluster", "node"}
                or identity["version"] != 1
                or not all(isinstance(identity[field], str) for field in ("cluster", "node"))
                or (snapshot is not None and not valid_snapshot(snapshot))
            ):
                raise CorruptionError("Invalid Raft generation metadata")
            self.identity, self.snapshot = identity, snapshot
            if snapshot is not None:
                self.commit = self.base = snapshot["index"]
                self.term = self.base_term = snapshot["term"]
        elif kind == HARD and self.identity is not None:
            term, vote, commit = json.loads(data)
            self.validate_hard(term, vote, commit)
            self.term, self.vote, self.commit = term, vote, commit
        elif kind == EDIT and self.identity is not None:
            first, entries = self.parse_edit(data)
            self.index_edit(location, first, entries)
        else:
            raise CorruptionError("Invalid Raft journal frame order")

    def validate_hard(self, term, vote, commit):
        if (
            self.identity is None
            or not unsigned(term) or term < max(self.term, self.term_at(self.last_index))
            or not unsigned(commit) or not self.commit <= commit <= self.last_index
            or (vote is not None and not isinstance(vote, str))
            or (term == self.term and self.vote is not None and vote != self.vote)
        ):
            raise CorruptionError("Invalid Raft hard state")

    def parse_edit(self, data):
        if len(data) < U64.size:
            raise CorruptionError("Missing Raft edit index")
        first = U64.unpack_from(data)[0]
        if self.identity is None or not self.commit < first <= self.last_index + 1:
            raise CorruptionError("Raft edit crosses the committed prefix or creates a gap")
        entries, position = [], U64.size
        previous_term = self.term_at(first - 1)
        while position < len(data):
            if position + ENTRY.size > len(data):
                raise CorruptionError("Incomplete Raft entry header")
            term, legacy, id_size, sql_size = ENTRY.unpack_from(data, position)
            start = position + ENTRY.size
            end = start + id_size + sql_size
            if (
                not 0 < term <= self.term or term < previous_term or end > len(data)
                or bool(id_size) != bool(sql_size) or legacy not in (0, 1)
                or (legacy and not id_size)
            ):
                raise CorruptionError("Invalid Raft entry")
            if id_size:
                data[start:start + id_size].decode("ascii")
                data[start + id_size:end].decode("utf-8")
            entries.append((term, position, end))
            position, previous_term = end, term
        if not entries or first + len(entries) - 1 > MAX_INDEX:
            raise CorruptionError("Invalid Raft edit length")
        return first, entries

    def index_edit(self, location, first, entries):
        self.entries[first - self.base - 1:] = [
            (term, location, start, end) for term, start, end in entries
        ]

    def record(self, kind, data, *, sync=True):
        if kind == HARD:
            self.validate_hard(*json.loads(data))
        location = self.journal.append(kind, data)
        if sync:
            self.journal.sync()
        self.accept(location, kind, data)

    def edit(self, first, entries, *, sync=True):
        if not entries:
            return
        data = bytearray(U64.pack(first))
        for entry in entries:
            request_id = entry.get("request_id", "").encode("ascii")
            sql = entry.get("sql", "").encode("utf-8")
            data.extend(ENTRY.pack(entry["term"], int(entry.get("legacy", False)), len(request_id), len(sql)))
            data.extend(request_id)
            data.extend(sql)
        first, parsed = self.parse_edit(data)
        location = self.journal.append(EDIT, data)
        if sync:
            self.journal.sync()
        self.index_edit(location, first, parsed)
        self.record_count += 1


class Storage:
    def __init__(self, directory, *, segment_size=DEFAULT_SEGMENT_SIZE, repair=True):
        self.directory = Path(directory).resolve()
        if repair:
            ensure_directory(self.directory)
        self.path = self.directory / "raft.json"
        self.native_marker = self.directory / "NATIVE"
        self.lock = lock_directory(self.directory / "lock", create=repair)
        self.log = None
        try:
            if self.native_marker.exists() and not (self.directory / "raft" / "CURRENT").is_file():
                raise CorruptionError("Native Raft generation pointer is missing")
            self.generations = Generations(self.directory / "raft", segment_size, existing=not repair)
            journal = self.generations.open()
            try:
                self.log = RaftLog(journal, repair=repair)
                self.verify_snapshot()
                if repair:
                    self.generations.retire(journal)
            except BaseException:
                journal.close()
                raise
        except BaseException:
            self.lock.close()
            raise

    def initialize(self, identity):
        if self.log.identity is not None:
            if self.log.identity != identity:
                raise ValueError("The data directory belongs to a different node or cluster")
        elif self.native_marker.exists():
            raise CorruptionError("Initialized Raft journal has no metadata")
        elif self.path.exists():
            self.migrate(identity)
        else:
            self.log.record(META, encode_json([identity, None]))
        if not self.native_marker.exists():
            atomic_write(self.native_marker, lambda file: None)

    def migrate(self, identity):
        with self.path.open(encoding="utf-8") as file:
            state = json.load(file)
        if state["identity"] != identity:
            raise ValueError("The data directory belongs to a different node or cluster")
        items = state["log"]
        if (
            not items or items[0] != {"term": 0}
            or not unsigned(state["commit"]) or state["commit"] >= len(items)
        ):
            raise CorruptionError("Invalid legacy Raft log")
        self.replace(identity, None, None, 1, legacy=state)

    @property
    def snapshot_path(self):
        return self.log.journal.directory / SNAPSHOT_FILE

    def verify_snapshot(self):
        snapshot = self.log.snapshot
        if snapshot is not None:
            if fingerprint(self.snapshot_path) != (snapshot["size"], snapshot["digest"]):
                raise CorruptionError("Raft snapshot size or SHA-256 mismatch")

    def hard(self, term, vote, commit):
        self.log.record(HARD, encode_json([term, vote, commit]))
        self.reclaim_obsolete_frames()

    def edit(self, first, entries):
        self.log.edit(first, entries)
        self.reclaim_obsolete_frames()

    def reclaim_obsolete_frames(self):
        log = self.log
        if log.record_count > MAX_OBSOLETE_FRAMES + 2 * len(log.entries):
            self.replace(log.identity, log.snapshot, self.snapshot_path, log.base + 1)

    @staticmethod
    def stage_snapshot(source, target):
        # Snapshot files are immutable. Reusing the inode avoids copying a
        # snapshot when a generation is rewritten to discard obsolete edits.
        if WINDOWS:
            with source.open("rb") as original:
                atomic_write(target, lambda file: shutil.copyfileobj(original, file, 256 * 1024))
            return
        try:
            os.link(source, target)
        except OSError as error:
            if error.errno != errno.EXDEV:
                raise
            with source.open("rb") as original, target.open("xb") as file:
                shutil.copyfileobj(original, file, 256 * 1024)
                sync_file(file)

    def replace(self, identity, snapshot, source, first, *, legacy=None):
        old = self.log
        journal = self.generations.create()
        try:
            log = RaftLog(journal)
            if snapshot is not None:
                self.stage_snapshot(source, journal.directory / SNAPSHOT_FILE)
            log.record(META, encode_json([identity, snapshot]), sync=False)
            term, vote, commit = old.term, old.vote, old.commit
            if legacy is not None:
                term, vote, commit = legacy["term"], legacy["voted_for"], legacy["commit"]
            log.record(HARD, encode_json([term, vote, log.base]), sync=False)
            if legacy is not None:
                items = legacy["log"]
                for index in range(1, len(items), BATCH_SIZE):
                    entries = [
                        {**entry, "legacy": True} if "request_id" in entry else entry
                        for entry in items[index:index + BATCH_SIZE]
                    ]
                    log.edit(index, entries, sync=False)
            else:
                for index in range(first, old.last_index + 1, BATCH_SIZE):
                    log.edit(index, old.batch(index, BATCH_SIZE), sync=False)
            log.record(HARD, encode_json([term, vote, max(commit, log.base)]), sync=False)
            self.generations.publish(journal)
        except BaseException:
            journal.close()
            raise
        old.journal.close()
        self.log = log
        self.generations.retire(journal)

    def verify(self):
        journal = Journal(self.log.journal.directory, self.generations.segment_size)
        try:
            checked = RaftLog(journal)
            if (
                checked.identity != self.log.identity or checked.snapshot != self.log.snapshot
                or (checked.term, checked.vote, checked.commit) != (self.log.term, self.log.vote, self.log.commit)
                or checked.entries != self.log.entries
            ):
                raise CorruptionError("Raft journal replay does not match the active state")
            self.verify_snapshot()
        finally:
            journal.close()

    def close(self):
        try:
            if self.log is not None:
                self.log.journal.close()
        finally:
            self.lock.close()
