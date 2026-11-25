import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from raftdb.journal import CRC, HEADER, CorruptionError, TornFrame
from raftdb.kv import KV
from raftdb.raft import Node
from raftdb.relational import Relational
from raftdb.storage import Storage


ROOT = Path(__file__).resolve().parents[1]
MEMBERS = {"a": ("127.0.0.1", 19831), "b": ("127.0.0.1", 19832)}
CLUSTER = "storage-tests"
SMALL_SEGMENT = 90


def segments(root):
    generation = (root / "CURRENT").read_text().strip()
    return sorted((root / generation).glob("*.seg"))


def command(request_id, sql):
    return {"term": 1, "request_id": request_id, "sql": sql}


def append(node, *entries):
    previous = node.log.last_index
    return node.append_entries({
        "source": "b", "term": 1, "prev_index": previous, "prev_term": node.log.term_at(previous),
        "entries": list(entries), "commit": previous + len(entries),
    })


class StorageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def crash(self, source, *args):
        child = subprocess.run(
            [sys.executable, "-c", source, str(self.directory), *map(str, args)],
            cwd=ROOT, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(child.returncode, 86, child.stderr or child.stdout)

    def open_node(self, root=None):
        storage = Storage(self.directory if root is None else root, segment_size=SMALL_SEGMENT)
        try:
            return Node("a", MEMBERS, CLUSTER, storage)
        except BaseException:
            storage.close()
            raise

    @staticmethod
    def close_node(node):
        node.db.store.close()
        node.storage.close()

    def test_rotation_persistence_and_compaction(self):
        store = KV(self.directory, SMALL_SEGMENT)
        try:
            store.write({b"old": b"one", b"new": b"\x00\xff"})
            for _ in range(10):
                store.write({b"old": b"two", b"changes": b"x" * 64})
            store.write({b"old": None, b"changes": None, b"empty": b""})
            self.assertGreater(store.journal.number, 2)
            old = store.journal.directory
            before = store.journal.disk_bytes()
            store.compact()
            self.assertFalse(old.exists())
            self.assertLess(store.journal.disk_bytes(), before)
            self.assertIsNone(store.get(b"old"))
            store.verify()
        finally:
            store.close()
        reopened = KV(self.directory, SMALL_SEGMENT)
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.get(b"new"), b"\x00\xff")
        self.assertEqual(reopened.get(b"empty"), b"")
        self.assertIsNone(reopened.get(b"changes"))

    def test_complete_crc_errors_and_archived_torn_frames_fail_closed(self):
        for fault in ("header", "payload", "archived tail"):
            with self.subTest(fault=fault):
                root = self.directory / fault
                store = KV(root, SMALL_SEGMENT)
                store.write({b"first": b"A" * 50, b"next": b"B" * 50})
                store.close()
                segment = segments(root)[1]
                data = bytearray(segment.read_bytes())
                if fault == "archived tail":
                    del data[-1]
                else:
                    data[7 if fault == "header" else HEADER.size + CRC.size + 6] ^= 1
                segment.write_bytes(data)
                with self.assertRaises(CorruptionError):
                    KV(root, SMALL_SEGMENT)
                self.assertEqual(segment.read_bytes(), data)

    def test_missing_pointer_fails_closed(self):
        store = KV(self.directory, SMALL_SEGMENT)
        store.write({b"key": b"value"})
        store.close()
        segment = segments(self.directory)[0]
        (self.directory / "CURRENT").unlink()
        with self.assertRaisesRegex(CorruptionError, "Missing CURRENT"):
            KV(self.directory, SMALL_SEGMENT)
        self.assertTrue(segment.exists())

    def test_active_raft_torn_tail_and_exclusive_lock(self):
        node = self.open_node()
        self.assertTrue(append(node, {"term": 1})["success"])
        with self.assertRaises(BlockingIOError):
            Storage(self.directory)
        active = node.log.journal.path(node.log.journal.number)
        size = active.stat().st_size
        self.close_node(node)
        with active.open("ab") as file:
            file.write(b"RDB2")
        with self.assertRaises(TornFrame):
            Storage(self.directory, repair=False)
        self.assertEqual(active.stat().st_size, size + 4)
        recovered = self.open_node()
        self.addCleanup(self.close_node, recovered)
        self.assertEqual(active.stat().st_size, size)
        self.assertEqual(recovered.log.last_index, 1)
        self.assertEqual(recovered.applied, 1)
        recovered.storage.verify()

    def test_atomic_sql_recovery_after_a_torn_record_across_segments(self):
        node = self.open_node()
        append(node, command("create", "CREATE TABLE items (id INTEGER PRIMARY KEY, note TEXT)"),
               command("seed", "INSERT INTO items VALUES (1, 'old'), (2, 'old')"))
        self.close_node(node)
        self.crash("""
import os, sys
from unittest.mock import patch
from raftdb import journal
from raftdb.kv import PUT
from test_native_storage import append, command, MEMBERS, CLUSTER, SMALL_SEGMENT
from raftdb.raft import Node
from raftdb.storage import Storage
from raftdb.platform_support import atomic_replace

node = Node("a", MEMBERS, CLUSTER, Storage(sys.argv[1], segment_size=SMALL_SEGMENT))
write = journal.write_frame
seen = 0
def torn(file, kind, data):
    global seen
    if kind == PUT and data[4:5] == b"R":
        seen += 1
        if seen == 2:
            file.write(b"RDB2")
            file.flush()
            os.fsync(file.fileno())
            os._exit(86)
    write(file, kind, data)

with patch.object(journal, "write_frame", torn):
    append(node, command("update", "UPDATE items SET note = 'new'"))
""".replace("import os, sys", f"import os, sys\nsys.path.insert(0, {str(ROOT / 'tests')!r})"))
        data = Relational(self.directory / "relational", SMALL_SEGMENT)
        try:
            self.assertEqual(data.applied, 2)
            self.assertEqual([row["note"] for row in data.tables["items"].rows.values()], ["old", "old"])
            self.assertIsNone(data.result("update"))
        finally:
            data.store.close()
        recovered = self.open_node()
        try:
            self.assertEqual(recovered.log.commit, 3)
            self.assertEqual(recovered.applied, 3)
            self.assertEqual([row["note"] for row in recovered.db.tables["items"].rows.values()], ["new", "new"])
            self.assertEqual(recovered.db.result("update")[2], {"ok": True, "affected": 2})
        finally:
            self.close_node(recovered)
        reopened = self.open_node()
        self.addCleanup(self.close_node, reopened)
        self.assertEqual(reopened.db.result("update")[1], 3)

    def test_crash_after_statement_sync_reuses_checkpoint_and_outcome(self):
        node = self.open_node()
        append(node, command("create", "CREATE TABLE items (id INTEGER PRIMARY KEY)"))
        self.close_node(node)
        self.crash(f"""
import os, sys
from unittest.mock import patch
sys.path.insert(0, {str(ROOT / 'tests')!r})
from test_native_storage import append, command, MEMBERS, CLUSTER, SMALL_SEGMENT
from raftdb.kv import KV
from raftdb.raft import Node
from raftdb.storage import Storage
from raftdb.platform_support import atomic_replace

node = Node("a", MEMBERS, CLUSTER, Storage(sys.argv[1], segment_size=SMALL_SEGMENT))
write = KV.write
def store_then_crash(store, changes):
    write(store, changes)
    os._exit(86)
with patch.object(KV, "write", store_then_crash):
    append(node, command("insert", "INSERT INTO items VALUES (1)"))
""")
        recovered = self.open_node()
        self.addCleanup(self.close_node, recovered)
        self.assertEqual(recovered.applied, 2)
        self.assertEqual(list(recovered.db.tables["items"].rows), [1])
        self.assertEqual(recovered.db.result("insert")[2], {"ok": True, "affected": 1})

    def test_crash_before_and_after_compaction_publication(self):
        for boundary in ("before", "after"):
            with self.subTest(boundary=boundary):
                root = self.directory / boundary
                store = KV(root, SMALL_SEGMENT)
                store.write({b"stable": b"present", b"gone": b"removed"})
                store.write({b"gone": None})
                store.close()
                before = (root / "CURRENT").read_text()
                self.crash("""
import os, sys
from pathlib import Path
from unittest.mock import patch
from raftdb.kv import KV
from raftdb.platform_support import atomic_replace

root = Path(sys.argv[1]) / sys.argv[2]
store = KV(root, 90)
def interrupted(source, destination):
    if sys.argv[2] == "after":
        atomic_replace(source, destination)
    os._exit(86)
with patch("raftdb.journal.atomic_replace", interrupted):
    store.compact()
""", boundary)
                self.assertEqual((root / "CURRENT").read_text() != before, boundary == "after")
                recovered = KV(root, SMALL_SEGMENT)
                try:
                    self.assertEqual(recovered.get(b"stable"), b"present")
                    self.assertIsNone(recovered.get(b"gone"))
                    self.assertEqual(list(root.glob("gen-*")), [recovered.journal.directory])
                finally:
                    recovered.close()

    def test_crash_before_and_after_local_snapshot_publication(self):
        for boundary in ("before", "after"):
            with self.subTest(boundary=boundary):
                root = self.directory / boundary
                node = self.open_node(root)
                append(node, command("create", "CREATE TABLE items (id INTEGER PRIMARY KEY)"),
                       command("insert", "INSERT INTO items VALUES (1)"))
                self.close_node(node)
                self.crash(f"""
import os, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, {str(ROOT / 'tests')!r})
from test_native_storage import MEMBERS, CLUSTER, SMALL_SEGMENT
from raftdb.raft import Node
from raftdb.storage import Storage
from raftdb.platform_support import atomic_replace

node = Node("a", MEMBERS, CLUSTER, Storage(Path(sys.argv[1]) / sys.argv[2], segment_size=SMALL_SEGMENT))
def interrupted(source, destination):
    if sys.argv[2] == "after":
        atomic_replace(source, destination)
    os._exit(86)
with patch("raftdb.journal.atomic_replace", interrupted):
    node.make_snapshot()
""", boundary)
                recovered = self.open_node(root)
                try:
                    self.assertEqual(recovered.log.base, 2 if boundary == "after" else 0)
                    self.assertEqual(recovered.applied, 2)
                    self.assertEqual(list(recovered.db.tables["items"].rows), [1])
                    self.assertEqual(recovered.maintenance("verify")[0], 200)
                finally:
                    self.close_node(recovered)

    def test_crash_after_installing_raft_snapshot_before_relational_switch(self):
        source = self.open_node(self.directory / "source")
        append(source, command("create", "CREATE TABLE items (id INTEGER PRIMARY KEY)"),
               command("insert", "INSERT INTO items VALUES (4)"))
        info = source.make_snapshot()
        path = source.storage.snapshot_path
        self.close_node(source)
        self.crash(f"""
import os, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, {str(ROOT / 'tests')!r})
from test_native_storage import MEMBERS, CLUSTER, SMALL_SEGMENT
from raftdb.raft import Node
from raftdb.relational import Relational
from raftdb.storage import Storage
from raftdb.platform_support import atomic_replace

node = Node("a", MEMBERS, CLUSTER, Storage(Path(sys.argv[1]) / "target", segment_size=SMALL_SEGMENT))
with patch.object(Relational, "restore", side_effect=lambda *args: os._exit(86)):
    node.install_snapshot({{"source": "b", "term": 1, "snapshot": {info!r},
                           "offset": 0, "data": Path(sys.argv[2]).read_bytes()}})
""", path)
        target = self.open_node(self.directory / "target")
        self.addCleanup(self.close_node, target)
        self.assertEqual(target.log.base, 2)
        self.assertEqual(target.applied, 2)
        self.assertEqual(list(target.db.tables["items"].rows), [4])
        self.assertEqual(target.db.result("insert")[1], 2)

    def test_raft_journal_reclaims_obsolete_hard_state_with_no_commits(self):
        node = self.open_node()
        original = node.log.journal.directory
        try:
            for term in range(1, 540):
                node.request_vote({"source": "b", "term": term, "last_index": 0, "last_term": 0})
            self.assertFalse(original.exists())
            self.assertLess(node.log.record_count, 512)
            self.assertEqual((node.term, node.voted_for, node.commit), (539, "b", 0))
        finally:
            self.close_node(node)
        reopened = self.open_node()
        self.addCleanup(self.close_node, reopened)
        self.assertEqual((reopened.term, reopened.voted_for, reopened.commit), (539, "b", 0))

    def test_legacy_import_replays_original_dialect_and_preserves_log_tip(self):
        path = self.directory / "raft.json"
        legacy = {
            "identity": {"version": 1, "cluster": CLUSTER, "node": "a"},
            "term": 2, "voted_for": "b", "commit": 4,
            "log": [
                {"term": 0}, command("create", "CREATE TABLE items (id INTEGER PRIMARY KEY)"),
                command("insert", "INSERT INTO items VALUES (1)"),
                command("unsupported", "DROP TABLE items"),
                command("read", "SELECT * FROM items"),
                {"term": 2, "request_id": "unconfirmed", "sql": "DELETE FROM items"},
            ],
        }
        path.write_text(json.dumps(legacy))
        original = path.read_bytes()
        node = self.open_node()
        try:
            self.assertEqual((node.term, node.voted_for, node.commit, node.log.last_index), (2, "b", 4, 5))
            self.assertEqual(node.db.result("unsupported")[2]["code"], "sql_error")
            self.assertEqual(node.db.result("read")[2]["rows"], [[1]])
            self.assertIsNone(node.db.result("unconfirmed"))
            self.assertTrue(node.log[5]["legacy"])
        finally:
            self.close_node(node)
        reopened = self.open_node()
        try:
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(reopened.db.result("read")[2]["rows"], [[1]])
        finally:
            self.close_node(reopened)
        shutil.rmtree(self.directory / "raft")
        with self.assertRaisesRegex(CorruptionError, "Native Raft generation pointer is missing"):
            self.open_node()


if __name__ == "__main__":
    unittest.main()
