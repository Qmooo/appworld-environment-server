# appworld_server

AppWorld proxy + backend server pool. Clients call one URL (default `http://localhost:8777`);
`/initialize` leases an idle backend per `session_id` (falling back to
`task_id`), `/close` returns it; leases older than `--lease-ttl` (default 1800s) are reclaimed automatically.

## Install (once)

```bash
uv sync                                   # build .venv from uv.lock (Python 3.11)
uv run appworld install                   # unpack the app sources bundled in the AppWorld package
uv run appworld download data --root <DATA_ROOT>   # creates <DATA_ROOT>/data/ (~290MB)
```

`.venv/` and data are not version-controlled.

## Launch

```bash
# run from this directory: backends are `uv run appworld serve`, which needs this pyproject
APPWORLD_ROOT=<DATA_ROOT> nohup uv run python server_pool.py \
    --proxy-port 8777 --min 2 --max 4 > pool.log 2>&1 &
curl -s http://localhost:8777/pool/stats  # clean only when busy is 0
```

- `APPWORLD_ROOT` is required: `appworld serve environment` doesn't read it; the pool reads it and passes
  it down via `--root`. Unset falls back to `.`, where no data is found.
- Backend logs: `/tmp/aw_backend_<port>.log`; backends are numbered upward from `--base-port` (default 8800).
- Listens on `0.0.0.0` so other machines can share the pool; there is no auth.
- Other flags: `--scale-down` (idle seconds before shrinking back to `--min`), `--lease-ttl`; see `python server_pool.py -h`.
