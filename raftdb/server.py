"""HTTP endpoints for Raft RPCs, SQL requests, and node status."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import base64
import json
import os
import re
import traceback

from .raft import BATCH_SIZE, MAX_REQUEST_ID, MAX_SQL_BYTES, SNAPSHOT_CHUNK
from .storage import unsigned, valid_snapshot


MAX_BODY = 2 * 1024 * 1024
ID = re.compile(r"[a-zA-Z0-9_.:-]{1," + str(MAX_REQUEST_ID) + r"}\Z")


def valid_request_id(value):
    return isinstance(value, str) and ID.fullmatch(value) is not None


def valid_sql(value):
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        return len(value.encode("utf-8")) <= MAX_SQL_BYTES
    except UnicodeError:
        return False


def valid_entry(entry, term):
    if not isinstance(entry, dict) or not unsigned(entry.get("term")):
        return False
    if not 0 < entry["term"] <= term:
        return False
    if entry.keys() == {"term"}:
        return True
    required = {"term", "request_id", "sql"}
    command = entry.keys() == required or (
        entry.keys() == required | {"legacy"} and entry["legacy"] is True
    )
    return command and valid_request_id(entry["request_id"]) and valid_sql(entry["sql"])


def valid_raft_message(node, path, data):
    source = data.get("source")
    term = data.get("term")
    if (
        data.get("cluster") != node.cluster or not isinstance(source, str)
        or source not in node.peers or not unsigned(term) or term == 0
    ):
        return False
    if path == "/raft/vote":
        return (
            unsigned(data.get("last_index")) and unsigned(data.get("last_term"))
            and data["last_term"] <= term
        )
    if path == "/raft/snapshot":
        info, offset, raw = data.get("snapshot"), data.get("offset"), data.get("data")
        if (
            not valid_snapshot(info) or info["term"] > term or not unsigned(offset)
            or not isinstance(raw, str) or len(raw) > 4 * ((SNAPSHOT_CHUNK + 2) // 3)
        ):
            return False
        try:
            chunk = base64.b64decode(raw, validate=True)
        except ValueError:
            return False
        if not 0 < len(chunk) <= SNAPSHOT_CHUNK or offset + len(chunk) > info["size"]:
            return False
        data["data"] = chunk
        return True
    if not all(unsigned(data.get(field)) for field in ("prev_index", "prev_term", "commit")):
        return False
    entries = data.get("entries")
    if not isinstance(entries, list) or len(entries) > BATCH_SIZE or data["prev_term"] > term:
        return False
    last_term = data["prev_term"]
    for entry in entries:
        if not valid_entry(entry, term) or entry["term"] < last_term:
            return False
        last_term = entry["term"]
    return True


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(3)
        super().setup()

    def do_GET(self):
        if self.path == "/status":
            state = self.server.node.status()
            if getattr(self.server, "launch_id", None):
                state["launch_id"] = self.server.launch_id
            self.send_json(200, state)
        elif self.path == "/stats":
            self.dispatch(lambda: (200, self.server.node.stats()))
        else:
            self.error_json(404, "unknown_route", "Unknown route")

    def do_POST(self):
        node = self.server.node
        methods = {
            "/raft/vote": node.request_vote,
            "/raft/append": node.append_entries,
            "/raft/snapshot": node.install_snapshot,
        }
        maintenance = {"/admin/snapshot", "/admin/compact", "/admin/verify"}
        if self.path != "/sql" and self.path not in methods and self.path not in maintenance:
            self.error_json(404, "unknown_route", "Unknown route")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_BODY:
                raise ValueError("Invalid Content-Length")
            data = json.loads(self.rfile.read(length))
            if not isinstance(data, dict):
                raise ValueError("Expected an object")
        except (ValueError, RecursionError, OSError):
            self.error_json(400, "bad_request", "Expected a bounded JSON object")
            return
        if self.path == "/sql":
            request_id, sql = data.get("request_id"), data.get("sql")
            if not valid_request_id(request_id) or not valid_sql(sql):
                self.error_json(400, "bad_request", "Expected a valid request_id and a SQL statement")
                return
            page = data.get("page")
            if page is not None and (
                not isinstance(page, dict) or not unsigned(page.get("offset"))
                or not unsigned(page.get("size")) or not 1 <= page["size"] <= 10_000
            ):
                self.error_json(400, "bad_request", "Expected a valid result page")
                return
            self.dispatch(self.query, request_id, sql, page)
        elif self.path in maintenance:
            if data.get("cluster") != node.cluster:
                self.error_json(400, "bad_request", "Invalid cluster identity")
                return
            self.dispatch(node.maintenance, self.path.rsplit("/", 1)[-1])
        elif valid_raft_message(node, self.path, data):
            self.dispatch(lambda: (200, methods[self.path](data)))
        else:
            self.error_json(400, "bad_request", "Invalid Raft RPC")

    def query(self, request_id, sql, page):
        status, result = self.server.node.propose(request_id, sql)
        if page is not None and "rows" in result:
            offset, size = page["offset"], page["size"]
            rows = result["rows"]
            end = offset + size
            result = {
                **result, "rows": rows[offset:end], "next_offset": end if end < len(rows) else None,
            }
        return status, result

    def dispatch(self, method, *args):
        try:
            status, response = method(*args)
        except Exception:
            traceback.print_exc()
            os._exit(70)
        self.send_json(status, response)

    def error_json(self, status, code, message):
        self.send_json(status, {"ok": False, "code": code, "error": message})

    def send_json(self, status, response):
        body = json.dumps(response, separators=(",", ":")).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    def log_message(self, *args):
        pass


class Server(ThreadingHTTPServer):
    daemon_threads = False
    allow_reuse_address = os.name != "nt"

    def __init__(self, address, node):
        self.node = node
        super().__init__(address, Handler)
