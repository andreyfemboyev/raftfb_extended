"""Fixed-membership Raft and its replicated SQL state machine."""

import base64
from dataclasses import dataclass
from typing import BinaryIO
import http.client
import json
import os
import random
import threading
import time
import traceback

from .journal import CorruptionError
from .platform_support import sync_file
from .relational import Relational
from .storage import BATCH_SIZE, Storage, fingerprint


HEARTBEAT = 0.075
ELECTION = (0.45, 0.85)
RPC_TIMEOUT = 0.23
PROPOSAL_TIMEOUT = 1.75
MAX_SQL_BYTES = 16_384
MAX_REQUEST_ID = 128
SNAPSHOT_CHUNK = 128 * 1024
SNAPSHOT_INTERVAL = 256
MAX_UNCOMMITTED = 1024


@dataclass
class SnapshotTransfer:
    info: dict
    file: BinaryIO
    offset: int = 0

    def message(self):
        self.file.seek(self.offset)
        chunk = self.file.read(SNAPSHOT_CHUNK)
        return {
            "snapshot": self.info, "offset": self.offset,
            "data": base64.b64encode(chunk).decode("ascii"),
        }


def rpc(address, path, data, timeout=RPC_TIMEOUT):
    connection = http.client.HTTPConnection(*address, timeout=timeout)
    try:
        connection.request(
            "POST", path, json.dumps(data), {"Content-Type": "application/json"}
        )
        with connection.getresponse() as response:
            return response.status, json.load(response)
    finally:
        connection.close()


class Node:
    def __init__(self, node_id, members, cluster, storage: Storage, *, snapshot_interval=SNAPSHOT_INTERVAL):
        self.id = node_id
        self.members = members
        self.cluster = cluster
        self.peers = {key: address for key, address in members.items() if key != self.id}
        self.quorum = len(members) // 2 + 1
        self.storage = storage
        self.changed = threading.Condition()
        self.stopped = threading.Event()
        self.peer_wakes = {peer: threading.Event() for peer in self.peers}
        self.threads = []
        self.role = "follower"
        self.leader = None
        self.votes = set()
        self.next_index = {}
        self.match_index = {}
        self.pending = {}
        self.snapshot_interval = snapshot_interval
        if type(snapshot_interval) is not int or snapshot_interval <= 0:
            raise ValueError("Snapshot interval must be a positive integer")
        self.identity = {"version": 1, "cluster": cluster, "node": node_id}
        storage.initialize(self.identity)
        self.term, self.voted_for, self.commit = self.log.term, self.log.vote, self.log.commit
        if self.voted_for is not None and self.voted_for not in members:
            raise CorruptionError("Persisted vote belongs to an unknown member")

        self.local_snapshot = storage.directory / "local-snapshot.tmp"
        self.incoming_snapshot = storage.directory / "incoming-snapshot.tmp"
        for path in (self.local_snapshot, self.incoming_snapshot):
            path.unlink(missing_ok=True)
        self.incoming = None
        self.db = Relational(storage.directory / "relational", storage.generations.segment_size)
        try:
            if self.applied > self.commit:
                raise CorruptionError("Relational checkpoint exceeds durable Raft commit")
            if self.applied < self.log.base:
                self.db.restore(storage.snapshot_path, self.log.base)
            self.reset_election()
            self.apply()
        except BaseException:
            self.db.store.close()
            raise

    @property
    def log(self):
        return self.storage.log

    @property
    def applied(self):
        return self.db.applied

    def persist(self):
        self.storage.hard(self.term, self.voted_for, self.commit)

    def reset_election(self):
        self.election_at = time.monotonic() + random.uniform(*ELECTION)

    def start(self):
        jobs = [(self.election_loop, ())]
        jobs.extend((self.peer_loop, (peer, address)) for peer, address in self.peers.items())
        for target, args in jobs:
            thread = threading.Thread(target=self.run_worker, args=(target, args), daemon=True)
            self.threads.append(thread)
            thread.start()

    @staticmethod
    def run_worker(target, args):
        try:
            target(*args)
        except Exception:
            traceback.print_exc()
            os._exit(70)

    def stop(self):
        self.stopped.set()
        self.wake_peers()
        with self.changed:
            self.changed.notify_all()
        for thread in self.threads:
            thread.join()

    def wake_peers(self):
        for wake in self.peer_wakes.values():
            wake.set()

    def election_loop(self):
        while not self.stopped.wait(0.025):
            with self.changed:
                if self.role != "leader" and time.monotonic() >= self.election_at:
                    self.begin_election()

    def begin_election(self):
        self.term += 1
        self.role = "candidate"
        self.leader = None
        self.voted_for = self.id
        self.votes = {self.id}
        self.reset_election()
        self.persist()
        if len(self.votes) >= self.quorum:
            self.become_leader()
        else:
            self.wake_peers()

    def become_follower(self, term, leader=None):
        newer = term > self.term
        if newer:
            self.term = term
            self.voted_for = None
        self.role = "follower"
        self.leader = leader
        self.pending.clear()
        self.reset_election()
        if newer:
            self.persist()
        self.changed.notify_all()

    def become_leader(self):
        self.role = "leader"
        self.leader = self.id
        self.pending.clear()
        self.next_index = dict.fromkeys(self.peers, self.log.last_index + 1)
        self.match_index = dict.fromkeys(self.peers, 0)
        # A current-term no-op lets the leader commit any preceding entries.
        self.storage.edit(self.log.last_index + 1, [{"term": self.term}])
        self.wake_peers()
        self.advance_commit()

    def apply(self):
        while self.applied < self.commit:
            first = self.applied + 1
            entries = self.log.batch(first, min(BATCH_SIZE, self.commit - self.applied))
            for index, entry in enumerate(entries, first):
                self.db.apply(index, entry)
                self.pending.pop(entry.get("request_id"), None)
        if self.applied - self.log.base >= self.snapshot_interval:
            self.make_snapshot()
            self.db.store.compact()

    def advance_commit(self):
        matches = sorted([self.log.last_index, *self.match_index.values()], reverse=True)
        index = matches[self.quorum - 1]
        if index > self.commit and self.log.term_at(index) == self.term:
            self.commit = index
            self.persist()
            self.apply()
            self.wake_peers()
            self.changed.notify_all()

    def peer_loop(self, peer, address):
        voted_term = 0
        delay = HEARTBEAT
        transfer = None
        wake = self.peer_wakes[peer]
        try:
            while not self.stopped.is_set():
                wake.wait(delay)
                wake.clear()
                if self.stopped.is_set():
                    break
                delay = HEARTBEAT
                with self.changed:
                    term, role = self.term, self.role
                    if transfer is not None and (role != "leader" or transfer_term != term):
                        transfer.file.close()
                        transfer = None
                    if role == "candidate" and voted_term != term:
                        path = "/raft/vote"
                        params = {"last_index": self.log.last_index,
                                  "last_term": self.log.term_at(self.log.last_index)}
                    elif role == "leader":
                        start = self.next_index[peer]
                        if transfer is not None or start <= self.log.base:
                            path = "/raft/snapshot"
                            if transfer is None:
                                transfer = SnapshotTransfer(self.log.snapshot, self.storage.snapshot_path.open("rb"))
                                transfer_term = term
                            params = transfer.message()
                        else:
                            path = "/raft/append"
                            prev = start - 1
                            entries = self.log.batch(start, BATCH_SIZE)
                            params = {
                                "prev_index": prev, "prev_term": self.log.term_at(prev),
                                "entries": entries, "commit": self.commit,
                            }
                    else:
                        continue
                    message = {**params, "cluster": self.cluster, "source": self.id, "term": term}
                try:
                    status, result = rpc(address, path, message)
                except (OSError, http.client.HTTPException, ValueError):
                    continue
                if status != 200 or not isinstance(result, dict) or type(result.get("term")) is not int:
                    continue
                with self.changed:
                    if result["term"] > self.term:
                        self.become_follower(result["term"])
                        continue
                    if self.term != term or self.role != role or result["term"] != term:
                        continue
                    if path == "/raft/vote":
                        voted_term = term
                        if result.get("granted") is True:
                            self.votes.add(peer)
                            if len(self.votes) >= self.quorum:
                                self.become_leader()
                    elif path == "/raft/snapshot":
                        if result.get("installed") is True:
                            matched = transfer.info["index"]
                            transfer.file.close()
                            transfer = None
                            self.record_match(peer, matched)
                            delay = 0
                        elif type(result.get("offset")) is int:
                            offset = result["offset"]
                            if 0 <= offset < transfer.info["size"]:
                                delay = 0 if offset > transfer.offset else HEARTBEAT
                                transfer.offset = offset
                    elif result.get("success") is True:
                        self.record_match(peer, prev + len(entries))
                        if self.next_index[peer] <= self.log.last_index:
                            delay = 0
                    elif type(result.get("next_index")) is int:
                        compacted, compacted_term = result.get("compacted"), result.get("compacted_term")
                        if (
                            type(compacted) is int and self.log.base <= compacted <= self.log.last_index
                            and self.log.term_at(compacted) == compacted_term
                        ):
                            new_start = compacted + 1
                        else:
                            new_start = max(1, min(start - 1, result["next_index"]))
                        self.next_index[peer] = new_start
                        delay = 0 if new_start != start else HEARTBEAT
        finally:
            if transfer is not None:
                transfer.file.close()

    def record_match(self, peer, index):
        self.match_index[peer] = max(self.match_index[peer], index)
        self.next_index[peer] = self.match_index[peer] + 1
        self.advance_commit()

    def request_vote(self, message):
        term, candidate = message["term"], message["source"]
        with self.changed:
            if term > self.term:
                self.become_follower(term)
            candidate_tip = (message["last_term"], message["last_index"])
            own_tip = (self.log.term_at(self.log.last_index), self.log.last_index)
            granted = (
                term == self.term and self.voted_for in (None, candidate)
                and candidate_tip >= own_tip
            )
            if granted:
                if self.voted_for != candidate:
                    self.voted_for = candidate
                    self.persist()
                self.reset_election()
            return {"term": self.term, "granted": granted}

    def append_entries(self, message):
        term, leader = message["term"], message["source"]
        prev, prev_term = message["prev_index"], message["prev_term"]
        entries = message["entries"]
        with self.changed:
            if term < self.term:
                return {"term": self.term, "success": False}
            if term > self.term or self.role != "follower":
                self.become_follower(term, leader)
            self.leader = leader
            self.reset_election()

            if prev < self.log.base:
                return {
                    "term": self.term, "success": False, "next_index": self.log.base + 1,
                    "compacted": self.log.base, "compacted_term": self.log.base_term,
                }
            if prev > self.log.last_index:
                return {"term": self.term, "success": False, "next_index": self.log.last_index + 1}
            if self.log.term_at(prev) != prev_term:
                conflict_term = self.log.term_at(prev)
                first = prev
                while first > self.log.base + 1 and self.log.term_at(first - 1) == conflict_term:
                    first -= 1
                return {"term": self.term, "success": False, "next_index": first}

            for offset, entry in enumerate(entries):
                index = prev + 1 + offset
                if index > self.log.last_index or self.log.term_at(index) != entry["term"]:
                    if index <= self.commit:
                        return {"term": self.term, "success": False}
                    self.storage.edit(index, entries[offset:])
                    break
            # A heartbeat only establishes the prefix proven by its previous index.
            commit = min(message["commit"], prev + len(entries))
            if commit > self.commit:
                self.commit = commit
                self.persist()
                self.apply()
                self.changed.notify_all()
            return {"term": self.term, "success": True}

    def make_snapshot(self):
        index = self.applied
        if index <= self.log.base:
            return self.log.snapshot
        term = self.log.term_at(index)
        try:
            self.db.store.snapshot(self.local_snapshot)
            size, digest = fingerprint(self.local_snapshot)
            snapshot = {"index": index, "term": term, "size": size, "digest": digest}
            self.storage.replace(self.identity, snapshot, self.local_snapshot, index + 1)
        finally:
            self.local_snapshot.unlink(missing_ok=True)
        return snapshot

    def install_snapshot(self, message):
        term, leader, info = message["term"], message["source"], message["snapshot"]
        offset, data = message["offset"], message["data"]
        index = info["index"]
        with self.changed:
            if term < self.term:
                return {"term": self.term, "installed": False}
            if term > self.term or self.role != "follower":
                self.become_follower(term, leader)
            self.leader = leader
            self.reset_election()
            if index <= self.commit:
                valid = index < self.log.base or info["term"] == self.log.term_at(index)
                return {"term": self.term, "installed": valid}

            transfer = (leader, term, info)
            if self.incoming is None or self.incoming[0] != transfer:
                if offset != 0:
                    return {"term": self.term, "offset": 0}
                self.incoming_snapshot.unlink(missing_ok=True)
                self.incoming = (transfer, 0)
            received = self.incoming[1]
            if offset != received:
                return {"term": self.term, "offset": received}
            with self.incoming_snapshot.open("ab") as target:
                target.write(data)
                received += len(data)
                if received == info["size"]:
                    sync_file(target)
            if received < info["size"]:
                self.incoming = (transfer, received)
                return {"term": self.term, "offset": received}

            try:
                if fingerprint(self.incoming_snapshot) != (info["size"], info["digest"]):
                    return {"term": self.term, "offset": 0}
                try:
                    self.db.validate_snapshot(self.incoming_snapshot, index)
                except CorruptionError:
                    return {"term": self.term, "offset": 0}
                keep = index <= self.log.last_index and self.log.term_at(index) == info["term"]
                first = index + 1 if keep else self.log.last_index + 1
                # Publish the authoritative snapshot first. Startup resumes the state
                # replacement if interrupted between the two generation switches.
                self.storage.replace(self.identity, info, self.incoming_snapshot, first)
                self.commit = index
                self.db.restore(self.storage.snapshot_path, index)
                self.reset_election()
                self.changed.notify_all()
                return {"term": self.term, "installed": True}
            finally:
                self.incoming = None
                self.incoming_snapshot.unlink(missing_ok=True)

    def maintenance(self, operation):
        with self.changed:
            if operation == "snapshot":
                return 200, {"ok": True, "snapshot": self.make_snapshot()}
            if operation == "compact":
                snapshot = self.make_snapshot()
                self.db.store.compact()
                return 200, {"ok": True, "snapshot": snapshot, **self.stats()}
            if operation == "verify":
                self.storage.verify()
                self.db.verify()
                if self.log.snapshot is not None:
                    self.db.validate_snapshot(self.storage.snapshot_path, self.log.base)
                return 200, {"ok": True, "verified_index": self.applied}
            raise ValueError(f"Unknown maintenance operation {operation}")

    def stats(self):
        with self.changed:
            raft = self.log.journal
            data = self.db.store.journal
            return {
                **self.status(), "raft_bytes": raft.disk_bytes(), "raft_segments": raft.number,
                "data_bytes": data.disk_bytes(), "data_segments": data.number,
                "keys": len(self.db.store.index), "snapshot_bytes": (
                    0 if self.log.snapshot is None else self.log.snapshot["size"]
                ),
            }

    def unavailable(self, code, error):
        return 503, {"ok": False, "code": code, "error": error, "leader": self.leader}

    def propose(self, request_id, sql):
        with self.changed:
            if self.role != "leader" or self.stopped.is_set():
                return self.unavailable("not_leader", "Send this request to the leader")
            completed = self.db.result(request_id)
            if completed is not None:
                return self.result(completed, request_id, sql)
            if request_id in self.pending:
                original, index = self.pending[request_id]
                if original != sql:
                    return self.id_conflict()
            else:
                if self.log.last_index - self.commit >= MAX_UNCOMMITTED:
                    return self.unavailable("commit_timeout", "Uncommitted log limit reached")
                index = self.log.last_index + 1
                self.storage.edit(index, [{"term": self.term, "sql": sql, "request_id": request_id}])
                self.pending[request_id] = (sql, index)
                self.wake_peers()
                self.advance_commit()
            term = self.term
            deadline = time.monotonic() + PROPOSAL_TIMEOUT
            while True:
                completed = self.db.result(request_id)
                if completed is not None:
                    return self.result(completed, request_id, sql)
                if self.role != "leader" or self.term != term or self.stopped.is_set():
                    return self.unavailable("not_leader", "Leadership changed before confirmation")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return self.unavailable("commit_timeout", f"Log entry {index} is unconfirmed")
                self.changed.wait(remaining)

    @staticmethod
    def id_conflict():
        return 409, {"ok": False, "code": "id_conflict", "error": "Request ID used for different SQL"}

    def result(self, completed, request_id, sql):
        original, index, result = completed
        if original != sql:
            return self.id_conflict()
        status = 200 if result["ok"] else 422
        return status, {**result, "request_id": request_id, "index": index}

    def status(self):
        with self.changed:
            return {
                "node": self.id, "role": self.role, "term": self.term, "leader": self.leader,
                "commit_index": self.commit, "last_applied": self.applied,
                "last_log_index": self.log.last_index, "quorum": self.quorum,
                "snapshot_index": self.log.base,
            }
