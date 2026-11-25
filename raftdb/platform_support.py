"""Operating-system services for the engine, CLI and local process supervisor."""

import os
from pathlib import Path
import subprocess
from uuid import uuid4
import warnings


WINDOWS = os.name == "nt"

if WINDOWS:
    import ctypes
    from ctypes import wintypes
    import msvcrt

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)

    class Overlapped(ctypes.Structure):
        _fields_ = [
            ("internal", ctypes.c_size_t), ("internal_high", ctypes.c_size_t),
            ("offset", wintypes.DWORD), ("offset_high", wintypes.DWORD), ("event", wintypes.HANDLE),
        ]

    def win_function(name, result, *arguments):
        function = getattr(kernel, name)
        function.argtypes = arguments
        function.restype = result
        return function

    lock_file = win_function(
        "LockFileEx", wintypes.BOOL, wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD,
        wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(Overlapped),
    )
    move_file = win_function(
        "MoveFileExW", wintypes.BOOL, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD,
    )
    create_file = win_function(
        "CreateFileW", wintypes.HANDLE, wintypes.LPCWSTR, wintypes.DWORD,
        wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
    )
    flush_buffers = win_function("FlushFileBuffers", wintypes.BOOL, wintypes.HANDLE)
    close_handle = win_function("CloseHandle", wintypes.BOOL, wintypes.HANDLE)
    open_process = win_function(
        "OpenProcess", wintypes.HANDLE, wintypes.DWORD, wintypes.BOOL, wintypes.DWORD,
    )
    get_memory = win_function(
        "K32GetProcessMemoryInfo", wintypes.BOOL, wintypes.HANDLE, wintypes.LPVOID, wintypes.DWORD,
    )

    class MemoryCounters(ctypes.Structure):
        _fields_ = [("size", wintypes.DWORD), ("faults", wintypes.DWORD)] + [
            (name, ctypes.c_size_t) for name in (
                "peak", "working", "pool_paged_peak", "pool_paged", "pool_nonpaged_peak",
                "pool_nonpaged", "pagefile", "pagefile_peak",
            )
        ]
else:
    import fcntl


class ExperimentalDurabilityWarning(RuntimeWarning):
    """The filesystem cannot provide the POSIX directory-sync guarantee."""


def lock_directory(path, *, create):
    file = path.open("a+b" if create else "rb")
    try:
        if WINDOWS:
            # Lock the first byte, even when the lock file is still empty.
            if not lock_file(msvcrt.get_osfhandle(file.fileno()), 3, 0, 1, 0, ctypes.byref(Overlapped())):
                error = ctypes.get_last_error()
                if error == 33:
                    raise BlockingIOError(11, "Replica directory is already locked", path)
                raise ctypes.WinError(error)
        else:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        file.close()
        raise
    return file


def sync_file(file):
    file.flush()
    os.fsync(file.fileno())


def sync_directory(directory):
    if not WINDOWS:
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return

    # Win32 does not guarantee that directory handles support FlushFileBuffers.
    # Namespace publication additionally uses same-volume write-through moves.
    handle = create_file(str(directory), 0x40000000, 7, None, 3, 0x02000000, None)
    if handle == ctypes.c_void_p(-1).value:
        error = ctypes.get_last_error()
    else:
        try:
            if flush_buffers(handle):
                return
            error = ctypes.get_last_error()
        finally:
            close_handle(handle)
    if error not in (1, 5, 6, 50, 87):
        raise ctypes.WinError(error)
    warnings.warn(
        "Windows directory flush is unavailable. Storage power-loss durability is experimental.",
        ExperimentalDurabilityWarning,
    )


def atomic_replace(source, target, *, replace=True):
    if WINDOWS:
        flags = 0x8 | (0x1 if replace else 0)  # WRITE_THROUGH and optional REPLACE_EXISTING
        if not move_file(str(source), str(target), flags):
            raise ctypes.WinError(ctypes.get_last_error())
    elif replace:
        os.replace(source, target)
    else:
        os.rename(source, target)


def create_directory(path):
    if not WINDOWS:
        path.mkdir(mode=0o700)
        return
    staging = path.with_name(f".raftdb-mkdir-{uuid4().hex}")
    staging.mkdir()
    try:
        atomic_replace(staging, path, replace=False)
    finally:
        if staging.exists():
            staging.rmdir()


def atomic_write(path, write):
    staging = path.with_name(f".raftdb-write-{uuid4().hex}")
    try:
        with staging.open("xb") as file:
            write(file)
            sync_file(file)
        atomic_replace(staging, path)
        sync_directory(path.parent)
    finally:
        staging.unlink(missing_ok=True)


def app_data_dir():
    if WINDOWS:
        return Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local") / "RaftDBX"
    return Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local" / "share") / "raftdbx"


def history_path():
    if WINDOWS:
        return app_data_dir() / "history"
    return Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state") / "raftdb" / "history"


def start_process(command, *, private_stdin=False, **options):
    return subprocess.Popen(
        command, stdin=subprocess.PIPE if private_stdin else subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if WINDOWS else 0, **options,
    )


def stop_process(process, *, timeout=8):
    if process.stdin is not None:
        process.stdin.close()
    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        raise TimeoutError("Replica did not shut down gracefully and was stopped")


def process_memory(pid):
    if WINDOWS:
        handle = open_process(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return None
        try:
            counters = MemoryCounters()
            counters.size = ctypes.sizeof(counters)
            if get_memory(handle, ctypes.byref(counters), counters.size):
                return counters.working, counters.peak
        finally:
            close_handle(handle)
        return None
    try:
        values = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines())
        return tuple(int(values[key].split()[0]) * 1024 for key in ("VmRSS", "VmHWM"))
    except (OSError, KeyError, ValueError):
        return None


class ShellHistory:
    def __init__(self, path):
        self.path = Path(path).expanduser()
        self.entries = []
        try:
            import readline
        except ImportError:
            readline = None
        self.readline = readline

    def load(self):
        if self.readline is not None:
            try:
                self.readline.read_history_file(self.path)
            except FileNotFoundError:
                pass
            self.readline.set_history_length(1000)
            self.readline.set_auto_history(False)

    def add(self, sql):
        self.entries.append(sql)
        if self.readline is not None:
            self.readline.add_history(sql)

    def save(self):
        if not self.entries:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path.touch(mode=0o600)
        if self.readline is not None:
            self.readline.append_history_file(len(self.entries), self.path)
        else:
            with self.path.open("a", encoding="utf-8", newline="\n") as file:
                file.writelines(entry + "\n" for entry in self.entries)
