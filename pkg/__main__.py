import logging
import sys
import os
import argparse
import traceback
import time
import stat
import platform
import subprocess

import duckdb
from diskcache import Cache

from pkg.server import server, init_global_connection

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def _unlock_path_if_needed(path: str) -> None:
    """Best-effort: clear readonly/immutable flags and relax permissions so the file can be removed.
    - On macOS, clear the user immutable flag (uchg) if present via os.chflags or `chflags nouchg` fallback
    - Ensure file mode allows deletion by owner (chmod 0o666 best-effort)
    """
    try:
        # Relax permissions
        try:
            os.chmod(path, 0o666)
        except Exception:
            pass

        if platform.system() == 'Darwin':
            # Try native chflags first
            try:
                current_flags = os.stat(path).st_flags if hasattr(os, 'stat') else 0
                # Clear immutable/user immutable flags if set
                if hasattr(stat, 'UF_IMMUTABLE') and (current_flags & getattr(stat, 'UF_IMMUTABLE')):
                    os.chflags(path, current_flags & ~getattr(stat, 'UF_IMMUTABLE'))
                if hasattr(stat, 'SF_IMMUTABLE') and (current_flags & getattr(stat, 'SF_IMMUTABLE')):
                    os.chflags(path, current_flags & ~getattr(stat, 'SF_IMMUTABLE'))
            except Exception:
                # Fallback to shell chflags
                try:
                    subprocess.run(['chflags', 'nouchg', path], check=False)
                    subprocess.run(['chflags', 'noschg', path], check=False)
                except Exception:
                    pass
    except Exception as e:
        logger.debug(f"_unlock_path_if_needed failed for {path}: {e}")

def _safe_remove(path: str) -> None:
    """Attempt to remove a file, trying to unlock first if permission denied."""
    try:
        os.remove(path)
        logger.info(f"Removed file: {path}")
    except PermissionError:
        logger.warning(f"Permission denied removing {path}, attempting to unlock and retry...")
        try:
            _unlock_path_if_needed(path)
            os.remove(path)
            logger.info(f"Removed file after unlock: {path}")
        except Exception as e:
            logger.error(f"Failed to remove {path} after unlock attempt: {e}")
            raise
    except Exception as e:
        # Re-raise so caller can decide to continue or abort
        logger.debug(f"_safe_remove exception for {path}: {e}")
        raise

def _cleanup_stale_wal(db_path: str) -> None:
    """If a stale DuckDB WAL exists, attempt to remove or quarantine it before connecting.
    This tries to mitigate cases where a previous unclean shutdown left a .wal behind, which can
    prevent opening the DB on next launch.
    """
    wal_path = f"{db_path}.wal"
    try:
        if os.path.exists(wal_path):
            # Prefer renaming first to avoid interfering with any racing process
            quarantine = f"{wal_path}.quarantine.{int(time.time())}"
            try:
                os.rename(wal_path, quarantine)
                logger.warning(f"Quarantined stale WAL file: {wal_path} -> {quarantine}")
                # After rename, try to delete the quarantine copy best-effort
                try:
                    _safe_remove(quarantine)
                except Exception:
                    # Leave quarantined copy if removal fails
                    logger.warning(f"Left quarantined WAL at {quarantine}")
            except Exception:
                # If rename fails, try direct removal
                _safe_remove(wal_path)
    except Exception as e:
        logger.error(f"Error handling potential stale WAL '{wal_path}': {e}")

def serve(db_path=None, port=3000):
    start_time = time.time()
    
    # Print startup diagnostic information
    logger.info("="*50)
    logger.info("Python backend starting with:")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Command arguments: db_path={db_path}, port={port}")
    logger.info(f"Startup timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    def log_timing(step_name):
        elapsed = time.time() - start_time
        logger.info(f"TIMING: {step_name} completed at {elapsed:.3f} seconds")
    
    log_timing("Initial startup")
    
    if not db_path:
        logger.error("No database path provided. Please specify a path using --db-path.")
        sys.exit(1)
    
    logger.info(f"Using DuckDB from {db_path}")
    logger.info(f"Using port {port}")
    
    # Check if the database directory exists
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        logger.warning(f"Database directory does not exist: {db_dir}")
        try:
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"Created database directory: {db_dir}")
        except Exception as e:
            logger.exception(f"Error creating database directory: {e}")
            sys.exit(1)
    else:
        logger.info(f"Database directory exists: {db_dir}")
    
    log_timing("Directory check")

    # Check if the file exists but is not a valid DuckDB database
    try:
        if os.path.exists(db_path) and os.path.getsize(db_path) == 0:
            logger.warning(f"Found empty database file at {db_path}, will create a new one")
            os.remove(db_path)
    except Exception as e:
        logger.exception(f"Error checking database file: {e}")
        # If there's any error checking the file, try to remove it
        try:
            os.remove(db_path)
            logger.info(f"Removed potentially corrupted database file: {db_path}")
        except Exception:
            logger.exception(f"Failed to remove database file: {db_path}")
    
    log_timing("Database file check")

    # Connect to DuckDB (this will create a new DB if it doesn't exist)
    try:
        logger.info(f"Attempting to connect to DuckDB at {db_path}")
        # Proactively clean up a stale WAL if one is present
        _cleanup_stale_wal(db_path)

        duckdb_start = time.time()
        # Initialize the global connection instead of creating a local one
        init_global_connection(db_path)
        duckdb_time = time.time() - duckdb_start
        logger.info(f"TIMING: DuckDB connection took {duckdb_time:.3f} seconds")
        
        cache_start = time.time()
        cache = Cache()
        cache_time = time.time() - cache_start
        logger.info(f"TIMING: Cache initialization took {cache_time:.3f} seconds")
        
        logger.info(f"DuckDB connection established successfully")
        logger.info(f"Caching in {cache.directory}")
        
        log_timing("Backend initialization complete")
        
        # Start the server
        logger.info(f"Starting server on port {port}")
        server_start = time.time()
        server(cache, port)
        server_time = time.time() - server_start
        logger.info(f"TIMING: Server startup took {server_time:.3f} seconds")
    except duckdb.duckdb.IOException as e:
        # If we get an IO error, the file might be corrupted
        logger.error(f"Database file error: {e}")
        try:
            # If error mentions WAL specifically, prefer cleaning WAL first
            error_text = str(e).lower()
            wal_path = f"{db_path}.wal"
            handled = False
            if '.wal' in error_text or wal_path.lower() in error_text:
                logger.warning("Error indicates WAL issue; attempting WAL cleanup and reconnect...")
                try:
                    _cleanup_stale_wal(db_path)
                    init_global_connection(db_path)
                    cache = Cache()
                    logger.info("Recovered by removing stale WAL. Starting server...")
                    server(cache, port)
                    handled = True
                except Exception as wal_e:
                    logger.error(f"WAL cleanup path failed: {wal_e}")

            if not handled:
                # Try to remove DB and any WAL next to it, then create a new one
                try:
                    if os.path.exists(wal_path):
                        _safe_remove(wal_path)
                except Exception as wal_rm_e:
                    logger.warning(f"Could not remove WAL during recovery: {wal_rm_e}")
                _safe_remove(db_path)
                logger.info(f"Removed invalid database file: {db_path}")
                init_global_connection(db_path)  # This will create a new one
                cache = Cache()
                logger.info(f"Created new database file: {db_path}")
                server(cache, port)
        except Exception as e2:
            logger.exception(f"Fatal error, could not recover: {e2}")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during startup: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    overall_start = time.time()
    logger.info(f"TIMING: Process started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    parser = argparse.ArgumentParser(description='DuckDB Server')
    parser.add_argument('--db-path', type=str, help='Path to the DuckDB database file')
    parser.add_argument('--port', type=int, default=3000, help='Port to listen on')
    args = parser.parse_args()
    
    args_time = time.time() - overall_start
    logger.info(f"TIMING: Argument parsing took {args_time:.3f} seconds")
    
    try:
        serve(args.db_path, args.port)
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
