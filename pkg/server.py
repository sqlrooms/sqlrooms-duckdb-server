import logging
import time
from functools import partial
from pathlib import Path
import os
import threading
import shutil
import asyncio
import concurrent.futures
from typing import Optional, Callable, Any

import ujson
import json
import falcon.asgi
from falcon import Request, Response, WebSocketDisconnected
from falcon.asgi import WebSocket

from pkg.query import run_duckdb

logger = logging.getLogger(__name__)


# Flag to track if shutdown has been requested
shutdown_requested = False

from . import db_async

def make_error_response(code, message):
    error_body = {
        "success": False,
        "error": {
            "message": message,
        }
    }
    return json.dumps(error_body)

# Wait briefly for .wal file to disappear automatically after checkpoint
def _wait_for_wal_disappear(db_path: Optional[str], timeout_sec: float = 1.0, interval_sec: float = 0.05) -> None:
    try:
        if not db_path:
            return
        wal_path = f"{db_path}.wal"
        start = time.time()
        # Wait up to timeout for DuckDB to remove the WAL after FORCE CHECKPOINT
        while os.path.exists(wal_path) and (time.time() - start) < timeout_sec:
            time.sleep(interval_sec)
    except Exception:
        # Best-effort; ignore any issues here
        pass

 

 

class Handler:
    def done(self):
        raise Exception("NotImplementedException")
    def arrow(self, _buffer):
        raise Exception("NotImplementedException")
    def json(self, _data):
        raise Exception("NotImplementedException")
    def error(self, _error):
        raise Exception("NotImplementedException")

class WebSocketHandler(Handler):
    def __init__(self, ws):
        self.ws = ws
    def done(self):
        pass
    async def arrow(self, buffer):
        await self.ws.send_data(buffer)
    async def json(self, data):
        await self.ws.send_text(data)
    async def error(self, error):
        await self.ws.send_text(json.dumps({"error": str(error)}))

class HTTPHandler(Handler):
    def __init__(self, resp):
        self.resp = resp
    def done(self):
        self.resp.text = ""
    def arrow(self, buffer):
        self.resp.content_type = "application/octet-stream"
        self.resp.data = buffer
        if hasattr(self.resp, 'text'):
            self.resp.text = None
    def json(self, data):
        self.resp.content_type = "application/json"
        self.resp.text = data
        if hasattr(self.resp, 'data'):
            self.resp.data = None
    def error(self, error):
        self.resp.status = falcon.HTTP_400
        self.resp.content_type = "application/json"
        self.resp.text = make_error_response("QUERY_ERROR", str(error))
        if hasattr(self.resp, 'data'):
            self.resp.data = None

def deactivate_backend(cache) -> None:
    """Temporarily deactivate the backend for a connection change.
    - Block new queries
    - Cancel active queries and close their cursors
    - Clear cache
    - FORCE CHECKPOINT and close current GLOBAL_CON
    """
    global shutdown_requested
    # Block new queries while we switch connections
    shutdown_requested = True

    # Cancel/close any active queries
    db_async.cancel_all_queries()

    # Best-effort: clear cache to avoid stale results
    if cache:
        try:
            logger.info("Clearing cache before reconnection...")
            cache.clear()
            logger.info("Cache cleared")
        except Exception as e:
            logger.warning(f"Failed to clear cache (ignored): {e}")

    # Flush pending changes to disk and close current connection
    if db_async.GLOBAL_CON:
        try:
            logger.info("Forcing checkpoint before closing current connection...")
            db_async.GLOBAL_CON.execute("FORCE CHECKPOINT")
            _wait_for_wal_disappear(db_async.DATABASE_PATH)
        except Exception as e:
            logger.warning(f"FORCE CHECKPOINT failed (continuing): {e}")
        try:
            logger.info("Closing current DuckDB connection...")
            db_async.GLOBAL_CON.close()
            logger.info("Closed current DuckDB connection")
        except Exception as e:
            logger.warning(f"Error closing current connection (continuing): {e}")

def activate_backend(new_database_path: str) -> None:
    """Activate the backend by opening a connection to the provided database and resume queries."""
    global shutdown_requested
    logger.info(f"Re-initializing global DuckDB connection to {new_database_path}")
    db_async.init_global_connection(new_database_path)
    logger.info("Global connection re-initialized to new project file")
    # Resume accepting queries
    shutdown_requested = False

async def handle_query(handler: Handler, cache, query, query_id: Optional[str] = None, custom_handler: Optional[Callable[..., Any]] = None):
    global shutdown_requested
    # Use client-provided query_id if present
    if query_id is None:
        query_id = query.get("queryId") or db_async.generate_query_id()
    logger.debug(f"query={query} (query_id: {query_id})")
    # Check if shutdown has been requested - don't process new queries
    if shutdown_requested:
        logger.warning("Rejecting query because shutdown has been requested")
        await handler.error("Server is shutting down") if hasattr(handler.error, '__await__') else handler.error("Server is shutting down")
        return
    start = time.time()
    try:
        command = query["type"]
        logger.info(f"Processing command: {command} (query_id: {query_id})")
        if "sql" in query:
            sql = query["sql"]
            if len(sql) > 200:
                logger.debug(f"SQL query first 200 chars: {sql[:200]}... (query_id: {query_id})")
            else:
                logger.debug(f"Full SQL query: {sql} (query_id: {query_id})")
        # First, allow a custom handler to intercept/handle commands
        if custom_handler is not None:
            try:
                maybe_result = custom_handler(handler, cache, query, query_id)
                if asyncio.iscoroutine(maybe_result):
                    maybe_result = await maybe_result
                # Contract:
                # - If returns True -> already responded using handler, stop here
                # - If returns a dict with "type" -> send like run_duckdb result and stop
                # - Otherwise (None/False) -> fall through to built-ins
                if maybe_result is True:
                    return
                if isinstance(maybe_result, dict) and isinstance(maybe_result.get("type"), str):
                    rtype = maybe_result["type"]
                    if rtype == "done":
                        handler.done()
                    elif rtype == "arrow":
                        data = maybe_result.get("data")
                        await handler.arrow(data) if hasattr(handler.arrow, '__await__') else handler.arrow(data)
                    elif rtype == "json":
                        data = maybe_result.get("data")
                        await handler.json(data) if hasattr(handler.json, '__await__') else handler.json(data)
                    else:
                        raise ValueError(f"Unknown custom handler result type: {rtype}")
                    return
            except Exception as e:
                logger.exception(f"Error in custom handler for command '{command}' (query_id: {query_id}): {str(e)}")
                await handler.error(e) if hasattr(handler.error, '__await__') else handler.error(e)
                return

        # Handle saveProjectAs separately since it needs to modify the global connection
        if command == "saveProjectAs":
            source_path = query.get("sourcePath")
            target_path = query.get("targetPath")
            if not source_path or not target_path:
                raise ValueError("Missing sourcePath or targetPath for saveProjectAs command")

            logger.info(f"Starting Save Project As from {source_path} to {target_path}")
            # If paths are identical, nothing to do
            if os.path.abspath(source_path) == os.path.abspath(target_path):
                logger.info("Source and target paths are the same; nothing to do")
                handler.done()
                return
            loop = asyncio.get_running_loop()

            try:
                # Prepare: block queries, cancel actives, clear cache, checkpoint and close
                deactivate_backend(cache)

                # Ensure target directory exists
                try:
                    target_dir = os.path.dirname(target_path)
                    if target_dir and not os.path.exists(target_dir):
                        os.makedirs(target_dir, exist_ok=True)
                except Exception as e:
                    logger.warning(f"Failed to ensure target directory exists: {e}")

                # Copy database file to new location
                logger.info(f"Copying database file to new location: {target_path}")
                await loop.run_in_executor(None, lambda: shutil.copy2(source_path, target_path))
                logger.info(f"Copy completed to {target_path}")

                # Reconnect to new database path and resume queries
                activate_backend(target_path)

                # Success response
                handler.done()
            except Exception as e:
                # If anything failed, try to re-open the old DB to remain usable
                try:
                    if source_path:
                        logger.info("Attempting to restore connection to original database after failure...")
                        activate_backend(source_path)
                        logger.info("Restored connection to original database")
                except Exception as restore_e:
                    logger.error(f"Failed to restore original database connection: {restore_e}")
                raise
        else:
            # For all other commands, delegate to run_duckdb which handles db_async encapsulation
            try:
                result = await run_duckdb(cache, query, query_id=query_id)
                if result["type"] == "done":
                    handler.done()
                elif result["type"] == "arrow":
                    await handler.arrow(result["data"]) if hasattr(handler.arrow, '__await__') else handler.arrow(result["data"])
                elif result["type"] == "json":
                    await handler.json(result["data"]) if hasattr(handler.json, '__await__') else handler.json(result["data"])
            except concurrent.futures.CancelledError:
                logger.info(f"Query {query_id} was cancelled")
                await handler.error("Query was cancelled") if hasattr(handler.error, '__await__') else handler.error("Query was cancelled")
                return
            except Exception as e:
                logger.exception(f"Error processing command '{command}' (query_id: {query_id}): {str(e)}")
                await handler.error(e) if hasattr(handler.error, '__await__') else handler.error(e)
                return
    except KeyError as e:
        err_msg = f"Missing required key in query: {str(e)}"
        logger.exception(err_msg)
        await handler.error(err_msg) if hasattr(handler.error, '__await__') else handler.error(err_msg)
    except Exception as e:
        logger.exception(f"Error processing query: {str(e)}")
        await handler.error(e) if hasattr(handler.error, '__await__') else handler.error(e)
    total = round((time.time() - start) * 1_000)
    logger.info(f"DONE. Query took {total} ms.")

class DuckDBResource:
    def __init__(self, cache, custom_handler: Optional[Callable[..., Any]] = None):
        self.cache = cache
        self.custom_handler = custom_handler
    async def on_post(self, req: Request, resp: Response):
        try:
            data = await req.media
            # Use client-provided queryId if present
            query_id = data.get("queryId") or db_async.generate_query_id()
            # Create a custom handler that can add the query ID to the response
            class QueryTrackingHandler(HTTPHandler):
                def __init__(self, resp, query_id):
                    super().__init__(resp)
                    self.query_id = query_id
                def done(self):
                    self.resp.set_header('X-Query-ID', self.query_id)
                    super().done()
                def arrow(self, buffer):
                    self.resp.set_header('X-Query-ID', self.query_id)
                    super().arrow(buffer)
                def json(self, data):
                    self.resp.set_header('X-Query-ID', self.query_id)
                    super().json(data)
                def error(self, error):
                    self.resp.set_header('X-Query-ID', self.query_id)
                    super().error(error)
            handler = QueryTrackingHandler(resp, query_id)
            await handle_query(handler, self.cache, data, query_id, custom_handler=self.custom_handler)
        except Exception as e:
            logger.exception(f"Error handling POST request: {str(e)}")
            resp.status = falcon.HTTP_400
            resp.content_type = "application/json"
            resp.text = make_error_response("REQUEST_ERROR", str(e))
    async def on_get(self, req: Request, resp: Response):
        try:
            query_string = req.get_param("query")
            if not query_string:
                resp.status = falcon.HTTP_400
                resp.text = "Missing 'query' parameter"
                return
            try:
                data = json.loads(query_string)
                handler = HTTPHandler(resp)
                await handle_query(handler, self.cache, data, custom_handler=self.custom_handler)
            except json.JSONDecodeError as e:
                logger.exception(f"Invalid JSON in query parameter: {str(e)}")
                resp.status = falcon.HTTP_400
                resp.text = f"Invalid JSON in 'query' parameter: {str(e)}"
        except Exception as e:
            logger.exception(f"Error handling GET request: {str(e)}")
            resp.status = falcon.HTTP_400
            resp.content_type = "application/json"
            resp.text = make_error_response("REQUEST_ERROR", str(e))
    async def on_options(self, req: Request, resp: Response):
        resp.status = falcon.HTTP_200
    async def on_websocket(self, req: Request, ws: WebSocket):
        await ws.accept()
        try:
            while True:
                message = await ws.receive_text()
                try:
                    query = ujson.loads(message)
                    handler = WebSocketHandler(ws)
                    await handle_query(handler, self.cache, query, custom_handler=self.custom_handler)
                except Exception as e:
                    logger.exception("Error processing WebSocket message")
                    await ws.send_text(json.dumps({"error": str(e)}))
        except WebSocketDisconnected:
            logger.info("WebSocket disconnected")

class CancelQueryResource:
    def __init__(self, cache):
        self.cache = cache
    async def on_post(self, req: Request, resp: Response):
        try:
            data = await req.media
            query_id = data.get("queryId")
            if not query_id:
                resp.status = falcon.HTTP_400
                resp.content_type = "application/json"
                resp.text = make_error_response("MISSING_QUERY_ID", "Missing queryId in request")
                return
            logger.info(f"Received cancellation request for query {query_id}")
            success = db_async.cancel_query(query_id)
            if success:
                resp.status = falcon.HTTP_200
                resp.content_type = "application/json"
                resp.text = json.dumps({"success": True, "message": f"Query {query_id} cancelled successfully"})
            else:
                resp.status = falcon.HTTP_404
                resp.content_type = "application/json"
                resp.text = make_error_response("QUERY_NOT_FOUND", f"Query {query_id} not found or already completed")
        except Exception as e:
            logger.exception(f"Error cancelling query: {str(e)}")
            resp.status = falcon.HTTP_500
            resp.content_type = "application/json"
            resp.text = make_error_response("CANCEL_ERROR", str(e))

class ShutdownResource:
    def __init__(self, cache):
        self.cache = cache
    async def on_post(self, req: Request, resp: Response):
        global shutdown_requested
        try:
            logger.info("Received shutdown request, preparing for graceful shutdown")
            try:
                # Mark for shutdown but don't close yet
                shutdown_requested = True
                # Cancel all active queries
                db_async.cancel_all_queries()
                # Clear the cache
                if self.cache:
                    logger.info("Clearing cache...")
                    self.cache.clear()
                    logger.info("Cache cleared successfully")
                # Schedule the server to stop in a short while
                def delayed_shutdown():
                    logger.info("Performing delayed shutdown...")
                    if db_async.GLOBAL_CON:
                        try:
                            # Force checkpoint to ensure all data is written to main DB file, not WAL
                            logger.info("Forcing checkpoint to flush WAL to main database file...")
                            db_async.GLOBAL_CON.execute("FORCE CHECKPOINT")
                            logger.info("Checkpoint completed successfully")
                            # Give DuckDB a brief moment to finalize WAL cleanup
                            _wait_for_wal_disappear(db_async.DATABASE_PATH)
                            logger.info("Closing global DuckDB connection...")
                            db_async.GLOBAL_CON.close()
                            logger.info("DuckDB connection closed successfully")

                            # Best-effort: remove any remaining WAL file
                            try:
                                if db_async.DATABASE_PATH:
                                    wal_path = f"{db_async.DATABASE_PATH}.wal"
                                    if os.path.exists(wal_path):
                                        logger.info(f"Removing remaining WAL file at shutdown: {wal_path}")
                                        try:
                                            os.remove(wal_path)
                                            logger.info("WAL file removed")
                                        except Exception as wal_e:
                                            logger.warning(f"Could not remove WAL on shutdown: {wal_e}")
                            except Exception as e2:
                                logger.error(f"WAL cleanup error (ignored): {e2}")
                        except Exception as e:
                            logger.exception(f"Error during connection cleanup: {str(e)}")
                    # Shut down shared executor to release threads
                    db_async.shutdown_executor(wait=False)
                    logger.info("Exiting process gracefully...")
                    time.sleep(0.5)
                    os._exit(0)
                threading.Timer(1.0, delayed_shutdown).start()
                resp.status = falcon.HTTP_200
                resp.content_type = "application/json"
                resp.text = json.dumps({"success": True, "message": "Graceful shutdown initiated"})
            except Exception as e:
                logger.exception(f"Error during graceful shutdown: {str(e)}")
                resp.status = falcon.HTTP_500
                resp.content_type = "application/json"
                resp.text = make_error_response("SHUTDOWN_ERROR", str(e))
        except Exception as e:
            logger.exception(f"Unexpected error during shutdown: {str(e)}")
            resp.status = falcon.HTTP_500
            resp.content_type = "application/json"
            resp.text = make_error_response("SERVER_ERROR", str(e))

class ConnectionManagementResource:
    """Resource for managing DuckDB connection during file operations on Windows since duckdb will lock the file to prevent any file operations"""

    async def on_post(self, req: Request, resp: Response):
        # manipulate connection via db_async module to ensure shared state
        try:
            data = await req.get_media()
            action = data.get("action")

            if action == "close":
                if db_async.GLOBAL_CON:
                    logger.info("Closing DuckDB connection to release file lock...")
                    db_async.GLOBAL_CON.close()
                    db_async.GLOBAL_CON = None
                    logger.info("DuckDB connection closed successfully")
                    resp.status = falcon.HTTP_200
                    resp.content_type = "application/json"
                    resp.text = json.dumps({"success": True, "message": "Connection closed"})
                else:
                    resp.status = falcon.HTTP_200
                    resp.content_type = "application/json"
                    resp.text = json.dumps({"success": True, "message": "Connection already closed"})

            elif action == "reopen":
                db_path = data.get("dbPath")
                if not db_path:
                    resp.status = falcon.HTTP_400
                    resp.content_type = "application/json"
                    resp.text = make_error_response("MISSING_PATH", "dbPath is required for reopening connection")
                    return

                if db_async.GLOBAL_CON:
                    logger.info("Closing existing connection before reopening...")
                    db_async.GLOBAL_CON.close()
                    db_async.GLOBAL_CON = None

                logger.info(f"Reopening DuckDB connection to {db_path}...")
                db_async.init_global_connection(db_path)
                logger.info("DuckDB connection reopened successfully")

                resp.status = falcon.HTTP_200
                resp.content_type = "application/json"
                resp.text = json.dumps({"success": True, "message": "Connection reopened"})
            else:
                resp.status = falcon.HTTP_400
                resp.content_type = "application/json"
                resp.text = make_error_response("INVALID_ACTION", f"Invalid action: {action}")

        except Exception as e:
            logger.exception(f"Error in connection management: {str(e)}")
            resp.status = falcon.HTTP_500
            resp.content_type = "application/json"
            resp.text = make_error_response("CONNECTION_ERROR", str(e))

# For backward compatibility for callers importing from pkg.server
init_global_connection = db_async.init_global_connection

def create_app(cache, *, custom_handler: Optional[Callable[..., Any]] = None):
    app = falcon.asgi.App(cors_enable=True)
    app.add_route('/', DuckDBResource(cache, custom_handler))
    app.add_route('/cancel', CancelQueryResource(cache))
    app.add_route('/shutdown', ShutdownResource(cache))
    app.add_route('/connection', ConnectionManagementResource())

    return app

def server(cache, port=4000):
    import uvicorn
    app = create_app(cache)
    logger.info(f"Falcon DuckDB Server listening at ws://localhost:{port} and http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
