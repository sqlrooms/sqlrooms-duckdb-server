# SQLRooms DuckDB Server

[![PyPi](https://img.shields.io/pypi/v/sqlrooms-duckdb-server.svg)](https://pypi.org/project/sqlrooms-duckdb-server/)

A Python-based DuckDB server for [SQLRooms](https://sqlrooms.org), built on FastAPI. The server runs a local DuckDB instance and supports queries over HTTP or Web Sockets, returning data in either [Apache Arrow](https://arrow.apache.org/) or JSON format. The server supports query cancellation.

> **Note:** This server was initially created as a fork of [Mosaic DuckDB Server](https://github.com/uwdata/mosaic/tree/main/packages/server/duckdb-server), with additional features and improvements.

> **Note:** This package provides a local DuckDB server. To instead use SQLRooms with DuckDB-WASM in the browser, stick to the default [`WasmDuckDbConnector`](https://sqlrooms.org/api/duckdb/interfaces/WasmDuckDbConnector.html).

## Installation and usage

We recommend running the server in an isolated environment with [uvx](https://docs.astral.sh/uv/). For example, to directly run the server, use (database path required):

```bash
uvx sqlrooms-duckdb-server --db-path /absolute/path/to/my.db
```

Alternatively, you can install the server with `pip install sqlrooms-duckdb-server`. Then you can start the server with `sqlrooms-duckdb-server --db-path /absolute/path/to/my.db`.

### Command-line Arguments

The server accepts the following command-line arguments:

- `--port`: Specify the port to listen on (default: 3000)
- `--db-path`: Specify the path to the DuckDB database file (required; no default)

Example:

```bash
uvx sqlrooms-duckdb-server --port 3456 --db-path my_database.db
```

### Use as a library (embed in your Python app)

You can embed the server inside your own Python application. Two common options:

1) Quick start with the built-in bootstrapper:

```python
from pkg.__main__ import serve

if __name__ == "__main__":
    # Creates the DuckDB connection, disk cache, and runs the ASGI server
    serve(db_path="/absolute/path/to/my.db", port=3000)
```

2) Full control over the ASGI app and server lifecycle:

```python
from diskcache import Cache
from pkg.server import create_app
from pkg.db_async import init_global_connection
import uvicorn

def main():
    # Initialize the global DuckDB connection once at startup
    init_global_connection("/absolute/path/to/my.db")

    # Create a cache for query results
    cache = Cache()

    # Build the FastAPI app that exposes the SQLRooms endpoints
    app = create_app(cache)

    # Run with uvicorn (or mount `app` into your larger ASGI application)
    uvicorn.run(app, host="0.0.0.0", port=3000, log_level="info")

if __name__ == "__main__":
    main()
```

### Add custom endpoints

The server uses FastAPI under the hood. You can extend the app with additional routes.

Simple example:

```python
from diskcache import Cache
from pkg.server import create_app
from pkg.db_async import init_global_connection

# Startup
init_global_connection("/absolute/path/to/my.db")
cache = Cache()
app = create_app(cache)

# Add your custom route(s)
@app.get("/hello")
async def hello():
    return {"hello": "world"}
```

Running a DuckDB query inside your endpoint (using the shared connection and thread pool):

```python
from pkg import db_async

# Add an endpoint that runs a DuckDB query
@app.get("/row-count")
async def row_count():
    # Execute synchronous DuckDB work off the event loop
    def _count(cur):
        return cur.execute("select count(*) from my_table").fetchone()[0]

    count = await db_async.run_db_task(_count)
    return {"table": "my_table", "row_count": count}
```

### Extend the built-in `handle_query` with a custom command

You can intercept or add new `type` commands handled by the root `/` endpoint (HTTP/WS) using a `custom_handler` function supplied to `create_app`.

Contract for `custom_handler(handler, cache, query, query_id)`:
- Return `True` if you fully handled the request and already responded via `handler`.
- Or return a dict shaped like `{"type": "done" | "json" | "arrow", "data"?: any}` and the server will send it.
- Return `None`/`False` to fall back to the built-in commands.

Example: add a `type: "ping"` command that returns JSON over HTTP/WS.

```python
from diskcache import Cache
from pkg.server import create_app
from pkg.db_async import init_global_connection

def custom_handler(handler, cache, query, query_id):
    if query.get("type") == "ping":
        # Either respond directly using handler...
        return {"type": "json", "data": "{\"ok\": true}"}
    return False  # let built-ins handle others

init_global_connection("/absolute/path/to/my.db")
cache = Cache()
app = create_app(cache, custom_handler=custom_handler)
```

## Developer Setup

We use [uv](https://docs.astral.sh/uv/) to manage our development setup.

Start the server with `uv run sqlrooms-duckdb-server --db-path /absolute/path/to/my.db`. The server will not restart when the code changes.

Run `uv run ruff check --fix` and `uv run ruff format` to lint the code.

To run the tests, use `uv run pytest`.

To set up a local certificate for SSL, use https://github.com/FiloSottile/mkcert.

## API

The server supports queries via HTTP GET and POST, and WebSockets. The GET endpoint is useful for debugging. For example, you can query it with [this url](<http://localhost:3000/?query={"sql":"select 1","type":"json"}>).

Each endpoint takes a JSON object with a command in the `type`. The server supports the following commands.

### `exec`

Executes the SQL query in the `sql` field.

### `arrow`

Executes the SQL query in the `sql` field and returns the result in Apache Arrow format.

### `json`

Executes the SQL query in the `sql` field and returns the result in JSON format.

## Publishing

Run the build with `uv build`. Then publish with `uvx twine upload --skip-existing ./dist/*`. We publish using tokens so when asked, set the username to `__token__` and then use your token as the password. Alternatively, create a [`.pypirc` file](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#create-an-account).
