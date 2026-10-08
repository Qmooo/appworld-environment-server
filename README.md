# appworld_server

AppWorld proxy + backend server pool. Clients call one URL (default `http://localhost:8777`);
`/initialize` leases an idle backend per `session_id` (falling back to
`task_id`), `/close` returns it; leases older than `--lease-ttl` (default 1800s) are reclaimed automatically.

## Install (once)

```bash
uv sync                                   # build .venv from uv.lock (Python 3.11)
uv run appworld install                   # unpack the app sources bundled in the AppWorld package
uv run appworld download data --root <DATA_ROOT>   # creates <DATA_ROOT>/data/ (~190MB)
```

`<DATA_ROOT>` is any directory you choose for the data; we suggest `~/appworld_data`.

## Launch

Run it in a terminal of its own and leave that terminal open:

```bash
# run from this directory: backends are `uv run appworld serve`, which needs this pyproject
export APPWORLD_ROOT=<DATA_ROOT>          # same directory as `appworld download data`, e.g. ~/appworld_data
uv run python server_pool.py --proxy-port 8777 --min 2 --max 4
```

It is ready when it prints `AppWorld server ready at http://localhost:8777`. If it cannot start (no data under
`APPWORLD_ROOT`, port already in use, a backend that crashes), it prints the reason and exits. Ctrl+C stops it.
`curl -s http://localhost:8777/pool/stats` shows the pool; it is clean only when `busy` is 0.

To run it in the background instead, redirect to a file and watch that file for the same lines:
`nohup uv run python server_pool.py --proxy-port 8777 --min 2 --max 4 > pool.log 2>&1 &`, then `tail -f pool.log`.

- `APPWORLD_ROOT` is required: `appworld serve environment` doesn't read it; the pool reads it and passes
  it down via `--root`. Unset falls back to `.`, where no data is found.
- Backend logs: `/tmp/aw_backend_<port>.log`; backends are numbered upward from `--base-port` (default 8800).
- Listens on `0.0.0.0` so other machines can share the pool; there is no auth.
- Other flags: `--scale-down` (idle seconds before shrinking back to `--min`), `--lease-ttl`; see `python server_pool.py -h`.
