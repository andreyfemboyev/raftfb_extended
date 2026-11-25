"""Bitcask-style key directory with durable, multi-record transactions."""

from dataclasses import dataclass
import struct

from .journal import CorruptionError, Generations, read_frame, write_frame
from .platform_support import sync_file


BEGIN, PUT, DELETE, END = 16, 17, 18, 19
KEY_SIZE = struct.Struct(">I")


@dataclass(frozen=True, slots=True)
class Record:
    key: bytes
    value: bytes | None


def encode(record):
    data = KEY_SIZE.pack(len(record.key)) + record.key
    if record.value is None:
        return DELETE, data
    return PUT, data + record.value


def decode(kind, data):
    if kind not in (PUT, DELETE) or len(data) < KEY_SIZE.size:
        raise CorruptionError("Invalid key/value record")
    end = KEY_SIZE.size + KEY_SIZE.unpack_from(data)[0]
    if not KEY_SIZE.size < end <= len(data) or (kind == DELETE and end != len(data)):
        raise CorruptionError("Invalid key/value record lengths")
    value = data[end:] if kind == PUT else None
    return Record(data[KEY_SIZE.size:end], value)


def snapshot_records(path):
    """Read the one complete PUT-only transaction in a portable snapshot."""
    with path.open("rb") as file:
        if read_frame(file) != (BEGIN, b""):
            raise CorruptionError("Missing snapshot transaction header")
        while True:
            frame = read_frame(file)
            if frame == (END, b""):
                break
            if frame is None or frame[0] != PUT:
                raise CorruptionError("Invalid snapshot transaction")
            yield decode(*frame)
        if read_frame(file) is not None:
            raise CorruptionError("Extra records after snapshot transaction")


class KV:
    def __init__(self, directory, segment_size, *, repair=True):
        self.generations = Generations(directory, segment_size, existing=not repair)
        self.journal = self.generations.open()
        self.index = {}
        try:
            self.replay(self.journal, self.index, repair=repair)
            if repair:
                self.generations.retire(self.journal)
        except BaseException:
            self.journal.close()
            raise

    @staticmethod
    def stage(index, changes):
        for key, location in changes.items():
            if location is None:
                index.pop(key, None)
            else:
                index[key] = location

    @classmethod
    def replay(cls, journal, index, *, repair=False):
        pending = None
        for location, kind, data in journal.frames(repair=repair):
            if kind == BEGIN and not data:
                # A restarted writer can leave an abandoned transaction in older segments.
                pending = {}
            elif pending is None:
                raise CorruptionError("Key/value record outside a transaction")
            elif kind == END and not data:
                cls.stage(index, pending)
                pending = None
            else:
                record = decode(kind, data)
                if record.key in pending:
                    raise CorruptionError("Duplicate key in a storage transaction")
                pending[record.key] = location if kind == PUT else None

    @staticmethod
    def transaction(journal, records):
        changes = {}
        journal.append(BEGIN, b"")
        for record in records:
            if record.key in changes:
                raise CorruptionError("Duplicate key in a storage transaction")
            location = journal.append(*encode(record))
            changes[record.key] = location if record.value is not None else None
        journal.append(END, b"")
        journal.sync()
        return changes

    def write(self, changes):
        locations = self.transaction(self.journal, (Record(*pair) for pair in changes.items()))
        self.stage(self.index, locations)

    def get(self, key):
        location = self.index.get(key)
        if location is None:
            return None
        record = decode(*self.journal.read(location))
        if record.key != key or record.value is None:
            raise CorruptionError("Key directory does not match its record")
        return record.value

    def keys(self, prefix):
        return (key for key in self.index if key.startswith(prefix))

    def records(self):
        for key in self.index:
            yield Record(key, self.get(key))

    def snapshot(self, path):
        with path.open("xb") as file:
            write_frame(file, BEGIN, b"")
            for record in self.records():
                write_frame(file, *encode(record))
            write_frame(file, END, b"")
            sync_file(file)

    def replace(self, records):
        new = self.generations.create()
        try:
            index = self.transaction(new, records)
            self.generations.publish(new)
        except BaseException:
            new.close()
            raise
        self.journal.close()
        self.journal, self.index = new, index
        self.generations.retire(new)

    def compact(self):
        self.replace(self.records())

    def verify(self):
        actual = {}
        self.replay(self.journal, actual)
        if actual != self.index:
            raise CorruptionError("Journal replay does not match the key directory")

    def close(self):
        self.journal.close()
