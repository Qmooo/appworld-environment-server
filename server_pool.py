"""
AppWorld Server Pool + Proxy

Launch (cwd must be this directory: backends use `uv run appworld`, which finds the pyproject here):
    APPWORLD_ROOT=<appworld data root> uv run python server_pool.py [--proxy-port 8777] [--min 2] [--max 8] [--base-port 8800]

Agents keep calling http://localhost:8777; the proxy routes each request to an idle backend server.

Routing rules:
    POST /initialize  → assign an idle server to this task_id and forward
    POST /execute     → look up the server for task_id and forward
    POST /close       → forward, then release the server back to the pool
    POST /close_all   → same as above
    other             → forward to any server (e.g. GET /tasks/{task_id}, GET /api_docs)
"""

import argparse
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
from collections import deque
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [pool] %(message)s")
log = logging.getLogger("pool")


# AppWorld data root for backend servers. `appworld serve environment` defaults
# the root to '.' and does NOT read APPWORLD_ROOT itself, so we read it here and
# pass it explicitly via --root to each child.
APPWORLD_ROOT = os.environ.get("APPWORLD_ROOT", ".")


# ---------------------------------------------------------------------------
# Backend server management
# ---------------------------------------------------------------------------

def _wait_for_port(port: int, proc: subprocess.Popen, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def _tail(path: str, lines: int = 8) -> str:
    """Last lines of a backend log, without the ANSI colours rich adds."""
    with open(path, errors="replace") as f:
        text = re.sub(r"\x1b\[[0-9;]*m", "", f.read())
    return "\n".join("    " + line for line in text.strip().splitlines()[-lines:])


def _check_startup(proxy_port: int) -> str | None:
    """Catch the common setup mistakes before spending time on backends."""
    data_dir = os.path.join(os.path.expanduser(APPWORLD_ROOT), "data")
    if not os.path.isdir(data_dir):
        return (f"AppWorld data not found at {os.path.abspath(data_dir)}.\n"
                f"Download it with `uv run appworld download data --root <DATA_ROOT>` "
                f"and start the pool with APPWORLD_ROOT=<DATA_ROOT>.")
    with socket.socket() as sock:
        if sock.connect_ex(("127.0.0.1", proxy_port)) == 0:
            return (f"Port {proxy_port} is already in use; another pool may be running. "
                    f"Check with `curl -s http://localhost:{proxy_port}/pool/stats`.")
    return None


class ManagedServer:
    def __init__(self, port: int):
        self.port = port
        self.url = f"http://127.0.0.1:{port}"
        self._proc: subprocess.Popen | None = None
        self._log = None

    def start(self):
        log.info(f"Starting backend on port {self.port}")
        # Backend stdout/stderr -> per-port log file so child tracebacks
        # (e.g. a 500 on /initialize) are recoverable instead of swallowed.
        log_path = f"/tmp/aw_backend_{self.port}.log"
        self._log = open(log_path, "w")
        log.info(f"Backend port {self.port} logs -> {log_path}")
        self._proc = subprocess.Popen(
            ["uv", "run", "appworld", "serve", "environment",
             "--port", str(self.port), "--no-show-usage",
             "--root", APPWORLD_ROOT],
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        if not _wait_for_port(self.port, self._proc):
            reason = "exited" if self._proc.poll() is not None else "did not come up in time"
            self.stop()
            raise RuntimeError(
                f"Backend port {self.port} {reason}. Last lines of {log_path}:\n"
                + _tail(log_path)
            )
        log.info(f"Backend port {self.port} ready")

    def stop(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        self._proc = None
        if getattr(self, "_log", None):
            self._log.close()
            self._log = None

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None


class ServerPool:
    def __init__(self, min_servers: int, max_servers: int, base_port: int,
                 scale_down_idle_seconds: float = 60.0,
                 lease_ttl_seconds: float = 1800.0):
        self.min_servers = min_servers
        self.max_servers = max_servers
        self.base_port = base_port
        self.scale_down_idle_seconds = scale_down_idle_seconds
        self.lease_ttl_seconds = lease_ttl_seconds

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._idle: deque[ManagedServer] = deque()
        self._busy: dict[str, ManagedServer] = {}   # session key -> server
        self._leased_at: dict[str, float] = {}      # session key -> last-seen time
        self._all: list[ManagedServer] = []
        self._port_counter = 0

    def start(self):
        for _ in range(self.min_servers):
            self._launch_new()
        threading.Thread(target=self._scale_down_loop, daemon=True).start()
        threading.Thread(target=self._reap_leases_loop, daemon=True).start()
        log.info(f"Pool ready ({len(self._all)} servers, min={self.min_servers} "
                 f"max={self.max_servers} lease_ttl={self.lease_ttl_seconds}s)")

    def stop(self):
        with self._lock:
            servers = list(self._all)
        for s in servers:
            s.stop()

    def _next_port(self) -> int:
        p = self.base_port + self._port_counter
        self._port_counter += 1
        return p

    def _launch_new(self) -> ManagedServer:
        """Launch a new server. Must be called WITHOUT holding _lock."""
        port = self._next_port()
        s = ManagedServer(port)
        s.start()
        with self._cond:
            self._all.append(s)
            self._idle.append(s)
            self._cond.notify_all()
        return s

    def acquire(self, key: str) -> ManagedServer:
        """Assign an idle server to a session key. Blocks or scales up if needed.

        ``key`` is the per-run session id (falls back to task_id for legacy
        callers). Keying by session — not task_id — lets concurrent runs of the
        same task_id get separate backend worlds.

        BLOCKS. Callers on the event loop must go through run_in_threadpool —
        calling this from an ``async def`` freezes every route in the proxy,
        including the /close that would free the slot this is waiting for.
        """
        with self._cond:
            # Already assigned?
            if key in self._busy:
                return self._busy[key]

            while True:
                if self._idle:
                    s = self._idle.popleft()
                    self._busy[key] = s
                    self._leased_at[key] = time.time()
                    log.info(f"Assigned port {s.port} to session {key}")
                    return s

                if len(self._all) < self.max_servers:
                    total = len(self._all)
                    self._cond.release()
                    try:
                        log.info(f"All {total} servers busy — scaling up")
                        s = self._launch_new()
                    finally:
                        self._cond.acquire()
                    # _launch_new put it in _idle; loop again to grab it
                    continue

                log.info("Max servers reached, waiting for a free slot...")
                self._cond.wait(timeout=5)

    def release(self, key: str):
        """Return the server assigned to a session key back to the idle pool."""
        with self._cond:
            s = self._busy.pop(key, None)
            self._leased_at.pop(key, None)
            if s is None:
                return
            if s.is_alive():
                self._idle.append(s)
                log.info(f"Released port {s.port} (session {key})")
            else:
                self._all.remove(s)
                log.warning(f"Backend port {s.port} died; removed from pool")
            self._cond.notify_all()

    def get(self, key: str) -> ManagedServer | None:
        """Look up a session's backend, refreshing its lease.

        Every session-routed request lands here, so touching the timestamp makes
        the reaper's TTL mean "idle for TTL" rather than "held for TTL". The
        acquire-time version reaped a live run on 2026-07-30: it sat in a single
        1801s LLM call, sent no AppWorld traffic for 30 minutes, and lost its
        backend mid-task — every later /execute silently landed in a different
        world.
        """
        with self._lock:
            s = self._busy.get(key)
            if s is not None:
                self._leased_at[key] = time.time()
            return s

    def any_server(self) -> ManagedServer | None:
        with self._lock:
            if self._idle:
                return self._idle[0]
            if self._busy:
                return next(iter(self._busy.values()))
        return None

    @property
    def stats(self):
        now = time.time()
        with self._lock:
            return {
                "total": len(self._all),
                "idle": len(self._idle),
                "busy": len(self._busy),
                "max": self.max_servers,
                "lease_ttl_seconds": self.lease_ttl_seconds,
                "session_assignments": {k: v.port for k, v in self._busy.items()},
                # Seconds since this session's last request — the signal for a
                # leaked slot. A client that died never sends /close, so its
                # lease sits at busy forever and the pool silently loses
                # capacity until it hits max.
                "lease_idle_seconds": {k: round(now - t, 1)
                                       for k, t in self._leased_at.items()},
            }

    def _reap_leases_loop(self):
        """Release leases idle longer than the TTL.

        A crashed client never calls /close, so without this the pool loses one
        backend per crash. Observed 2026-07-30: ports 8810/8813 had been busy
        since a previous day's run, leaving only 4 of 6 backends usable — five
        concurrent /initialize then hit max and deadlocked the proxy.

        Idle, not held: see ServerPool.get. A slow run must not be mistaken for
        a dead one, so the TTL has to exceed the longest gap a live client can
        leave between AppWorld calls — one stalled LLM turn was 1801s.
        """
        while True:
            time.sleep(min(60.0, max(5.0, self.lease_ttl_seconds / 10)))
            now = time.time()
            with self._lock:
                stale = [k for k, t in self._leased_at.items()
                         if now - t > self.lease_ttl_seconds]
            for key in stale:
                idle = round(now - self._leased_at.get(key, now), 1)
                log.warning(f"Reaping stale lease (session {key}, idle {idle}s > "
                            f"ttl {self.lease_ttl_seconds}s) — client likely died "
                            f"without calling /close")
                self.release(key)

    def _scale_down_loop(self):
        while True:
            time.sleep(self.scale_down_idle_seconds / 2)
            with self._lock:
                excess = len(self._idle) - self.min_servers
                to_stop = []
                for _ in range(excess):
                    if self._idle:
                        s = self._idle.pop()
                        self._all.remove(s)
                        to_stop.append(s)
            for s in to_stop:
                log.info(f"Scale-down: stopping idle backend port {s.port}")
                s.stop()


# ---------------------------------------------------------------------------
# Proxy FastAPI app
# ---------------------------------------------------------------------------

pool: ServerPool = None  # set in main()

app = FastAPI(title="AppWorld Proxy")


def _to_response(resp: httpx.Response):
    content_type = resp.headers.get("content-type", "")
    if "text/html" in content_type:
        from fastapi.responses import HTMLResponse
        return HTMLResponse(status_code=resp.status_code, content=resp.text)
    try:
        return JSONResponse(status_code=resp.status_code, content=resp.json())
    except Exception:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(status_code=resp.status_code, content=resp.text)


def _route_key(body: dict) -> str | None:
    """Routing key for the pool: per-run session_id, falling back to task_id."""
    if not isinstance(body, dict):
        return None
    return body.get("session_id") or body.get("task_id")


def _strip_session(body: bytes) -> bytes:
    """Remove the pool-only ``session_id`` field before forwarding to a backend.

    The backend AppWorld server does not expect ``session_id`` and may reject
    it (422); it is purely a pool-level routing field.
    """
    if not body:
        return body
    try:
        data = json.loads(body)
    except Exception:
        return body
    if isinstance(data, dict) and "session_id" in data:
        data.pop("session_id")
        return json.dumps(data).encode()
    return body


async def _forward(server: ManagedServer, request: Request) -> JSONResponse:
    body = _strip_session(await request.body())
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "content-length")}
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.request(
            method=request.method,
            url=f"{server.url}{request.url.path}",
            content=body,
            headers=headers,
            params=dict(request.query_params),
        )
    return _to_response(resp)


@app.post("/initialize")
async def proxy_initialize(request: Request):
    body = await request.json()
    if not body.get("task_id"):
        raise HTTPException(status_code=400, detail="task_id is required")
    key = _route_key(body)
    # pool.acquire() blocks: it waits on a condition variable when every backend
    # is leased, and it shells out to launch a new one when scaling up. Calling
    # it directly from this coroutine froze the whole proxy on 2026-07-30 — five
    # concurrent /initialize took the pool to max, the last acquire() spun in the
    # event loop, and the /close that would have freed a slot could never be
    # served. Off-loop it goes.
    server = await run_in_threadpool(pool.acquire, key)
    # Forward without the pool-only session_id field (backend doesn't expect it).
    forward_body = {k: v for k, v in body.items() if k != "session_id"}
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            f"{server.url}/initialize",
            json=forward_body,
            headers={k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")},
        )
    return _to_response(resp)


async def _proxy_close(request: Request, path: str):
    """Shared close/close_all handler: idempotent + leak-free.

    Routes by session key. If no active server (already closed), returns a
    200 no-op instead of 404 so a duplicate/late teardown can't crash a run.
    The server is released in a finally so a backend never leaks even if the
    forwarded close itself errors.
    """
    body = await request.json()
    key = _route_key(body)
    server = pool.get(key) if key else None
    if server is None:
        return JSONResponse(status_code=200, content={"output": "no active server; already closed"})
    try:
        return await _forward(server, request)
    finally:
        pool.release(key)


@app.post("/close")
async def proxy_close(request: Request):
    return await _proxy_close(request, "/close")


@app.post("/close_all")
async def proxy_close_all(request: Request):
    return await _proxy_close(request, "/close_all")


@app.post("/{path:path}")
async def proxy_post(path: str, request: Request):  # noqa: ARG001
    """Generic POST proxy — routes by session_id (fallback task_id) in body."""
    try:
        body = await request.json()
        key = _route_key(body)
    except Exception:
        key = None

    server = pool.get(key) if key else pool.any_server()
    if server is None:
        raise HTTPException(status_code=503, detail="No backend server available")
    return await _forward(server, request)


@app.get("/pool/stats")
async def get_stats():
    return pool.stats


@app.get("/{path:path}")
async def proxy_get(path: str, request: Request):  # noqa: ARG001
    server = pool.any_server()
    if server is None:
        raise HTTPException(status_code=503, detail="No backend server available")
    return await _forward(server, request)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    global pool

    parser = argparse.ArgumentParser(description="AppWorld proxy + server pool")
    parser.add_argument("--proxy-port", type=int, default=8777, help="Port this proxy listens on")
    parser.add_argument("--min", type=int, default=2, help="Min backend servers")
    parser.add_argument("--max", type=int, default=8, help="Max backend servers")
    parser.add_argument("--base-port", type=int, default=8800, help="First backend port")
    parser.add_argument("--scale-down", type=float, default=60.0,
                        help="Seconds of idle before scaling down")
    parser.add_argument("--lease-ttl", type=float, default=1800.0,
                        help="Seconds a session may hold a backend before its "
                             "lease is reaped (guards against clients that die "
                             "without calling /close)")
    args = parser.parse_args()

    pool = ServerPool(
        min_servers=args.min,
        max_servers=args.max,
        base_port=args.base_port,
        scale_down_idle_seconds=args.scale_down,
        lease_ttl_seconds=args.lease_ttl,
    )
    problem = _check_startup(args.proxy_port)
    if problem:
        log.error(problem)
        sys.exit(1)
    try:
        pool.start()
    except RuntimeError as e:
        log.error(e)
        pool.stop()
        sys.exit(1)
    log.info(f"AppWorld server ready at http://localhost:{args.proxy_port} (Ctrl+C to stop)")

    try:
        uvicorn.run(app, host="0.0.0.0", port=args.proxy_port)
    except KeyboardInterrupt:
        pass
    finally:
        pool.stop()


if __name__ == "__main__":
    main()
