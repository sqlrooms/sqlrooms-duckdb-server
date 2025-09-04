# SQLRooms DuckDB Server

[![PyPi](https://img.shields.io/pypi/v/sqlrooms-duckdb-server.svg)](https://pypi.org/project/sqlrooms-duckdb-server/)

A Python-based server that runs a local DuckDB instance and supports queries over HTTP or Web Sockets, returning data in either [Apache Arrow](https://arrow.apache.org/) or JSON format. The server supports query cancellation.

> **Note:** This server was initially created as a fork of [Mosaic DuckDB Server](https://github.com/uwdata/mosaic/tree/main/packages/server/duckdb-server), with additional features and improvements.

> **Note:** This package provides a local DuckDB server. To instead use SQLRooms with DuckDB-WASM in the browser, stick to the default [`WasmDuckDbConnector`](https://sqlrooms.org/api/duckdb/interfaces/WasmDuckDbConnector.html).

## Installation and usage

We recommend running the server in an isolated environment with [uvx](https://docs.astral.sh/uv/). For example, to directly run the server, use:

```bash
uvx sqlrooms-duckdb-server
```

Alternatively, you can install the server with `pip install sqlrooms-duckdb-server`. Then you can start the server with `sqlrooms-duckdb-server`.

### Command-line Arguments

The server accepts the following command-line arguments:

- `--port`: Specify the port to listen on (default: 3000)
- `--db-path`: Specify the path to the DuckDB database file (default: ./kepler-desktop.db)

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

    # Build the Falcon ASGI app that exposes the SQLRooms endpoints
    app = create_app(cache)

    # Run with uvicorn (or mount `app` into your larger ASGI application)
    uvicorn.run(app, host="0.0.0.0", port=3000, log_level="info")

if __name__ == "__main__":
    main()
```

### Add custom endpoints

The server uses Falcon ASGI under the hood. You can extend the app with additional routes/resources.

Simple example:

```python
from diskcache import Cache
from pkg.server import create_app
from pkg.db_async import init_global_connection
from falcon import Request, Response

class HelloResource:
    async def on_get(self, req: Request, resp: Response):
        resp.media = {"hello": "world"}

# Startup
init_global_connection("/absolute/path/to/my.db")
cache = Cache()
app = create_app(cache)

# Add your custom route(s)
app.add_route("/hello", HelloResource())
```

Running a DuckDB query inside your endpoint (using the shared connection and thread pool):

```python
from pkg import db_async
from falcon import Request, Response

class RowCountResource:
    async def on_get(self, req: Request, resp: Response):
        # Execute synchronous DuckDB work off the event loop
        def _count(cur):
            return cur.execute("select count(*) from my_table").fetchone()[0]

        count = await db_async.run_db_task(_count)
        resp.media = {"table": "my_table", "row_count": count}

# Register it
app.add_route("/row-count", RowCountResource())
```

## Developer Setup

We use [uv](https://docs.astral.sh/uv/) to manage our development setup.

Start the server with `uv run sqlrooms-duckdb-server`. The server will not restart when the code changes.

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

Run the build with `uv build`. Then publish with `uvx twine upload --skip-existing ../../dist/*`. We publish using tokens so when asked, set the username to `__token__` and then use your token as the password. Alternatively, create a [`.pypirc` file](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#create-an-account).
