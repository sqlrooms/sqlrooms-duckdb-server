import logging
from typing import List
import json
import concurrent.futures

import falcon.asgi
from falcon import Request, Response
import mapbox_vector_tile
import duckdb

from .server import make_error_response
from .db_async import run_db_task, generate_query_id, GLOBAL_CON
from .cache import get_key

logger = logging.getLogger(__name__)


def _quote_ident(identifier: str) -> str:
    """Safely quote a SQL identifier. Supports schema-qualified names (e.g., schema.table)."""
    parts = identifier.split(".")
    safe_parts: List[str] = []
    for part in parts:
        if not part or not part.replace("_", "").isalnum():
            raise ValueError(f"Invalid identifier: {identifier}")
        safe_parts.append(f'"{part}"')
    return ".".join(safe_parts)



class DynamicTileResource:
    """Return a Mapbox Vector Tile for a given table/geometry column and XYZ tile."""

    def __init__(self, cache):
        self.cache = cache

    async def on_get(self, req: Request, resp: Response, tableName: str, columnName: str, z: int, x: int, y: int):
        if GLOBAL_CON is None:
            resp.status = falcon.HTTP_500
            resp.content_type = "application/json"
            resp.text = make_error_response("NO_DB", "DuckDB connection is not initialized")
            return

        try:
            q_table = _quote_ident(tableName)
            q_column = _quote_ident(columnName)
        except ValueError as ve:
            resp.status = falcon.HTTP_400
            resp.content_type = "application/json"
            resp.text = make_error_response("INVALID_IDENTIFIER", str(ve))
            return

        try:
            # Use DuckDB Spatial ST_TileEnvelope to derive tile bbox and filter
            iz, ix, iy = int(z), int(x), int(y)

            # Optionally ensure an R-Tree index exists to speed up ST_Intersects
            # (index on the same CRS as the column used in WHERE, here CRS84/WGS84)
            idx_name = f"idx_rtree_{tableName.replace('.', '_')}_{columnName}"

            def _ensure_rtree_index(cur: duckdb.DuckDBPyConnection):
                try:
                    cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON {q_table} USING RTREE ({q_column})')
                except Exception as e:
                    # Non-fatal: continue without index
                    logger.debug(f"RTREE index creation skipped/failed for {q_table}.{q_column}: {e}")

            # Run index creation best-effort (non-blocking if already exists)
            try:
                await run_db_task(_ensure_rtree_index, query_id=generate_query_id())
            except concurrent.futures.CancelledError:
                raise

            # Build SQL: intersect against a buffered tile envelope (in pixels -> meters), simplify by zoom, and clip
            buffer_px = 20
            sql = f"""
                WITH bbox AS (
                  SELECT ST_TileEnvelope({iz}, {ix}, {iy}) AS b3857
                ), params AS (
                  SELECT
                    b3857,
                    (ST_XMax(b3857) - ST_XMin(b3857)) / 256.0 AS meters_per_px
                  FROM bbox
                ), buffered AS (
                  SELECT ST_Buffer(b3857, meters_per_px * {buffer_px}) AS bbuf
                  FROM params
                ), envelope AS (
                  SELECT ST_Transform(bbuf, 'EPSG:3857', 'CRS84') AS env4326
                  FROM buffered
                ), candidates AS (
                  SELECT ST_Transform({q_column}, 'CRS84', 'EPSG:3857') AS g3857
                  FROM {q_table}, envelope
                  WHERE ST_Intersects({q_column}, envelope.env4326)
                  USING SAMPLE reservoir(50000 ROWS)
                  REPEATABLE (4321)
                ), simplified AS (
                  SELECT ST_SimplifyPreserveTopology(g3857, meters_per_px * 0.75) AS gs
                  FROM candidates, params
                ), clipped AS (
                  SELECT ST_Intersection(gs, bbuf) AS gc
                  FROM simplified, buffered
                )
                SELECT ST_AsText(gc) AS wkt
                FROM clipped
                WHERE NOT ST_IsEmpty(gc) 
                --UNION ALL SELECT ST_AsText(ST_Transform(env4326, 'CRS84', 'EPSG:3857')) AS wkt FROM envelope
            """
            # Cache key based on SQL and logical command scope
            cache_key = get_key(sql, "tile-mvt")

            # Serve from cache if available
            if getattr(self, "cache", None):
                cached = self.cache.get(cache_key)
                if cached is not None:
                    logger.debug("Cache hit for dynamic tile")
                    resp.content_type = "application/vnd.mapbox-vector-tile"
                    resp.data = cached
                    if hasattr(resp, "text"):
                        resp.text = None
                    return
            # Execute query via shared helper and register X-Query-ID
            query_id = generate_query_id()
            if hasattr(resp, "set_header"):
                resp.set_header("X-Query-ID", query_id)

            def _execute_with_cursor(cur: duckdb.DuckDBPyConnection):
                return cur.execute(sql).fetchall()

            try:
                rows = await run_db_task(_execute_with_cursor, query_id=query_id)
            except concurrent.futures.CancelledError:
                # Client disconnected: already interrupted in run_db_task
                raise

            # Compute Mercator bounds via DuckDB
            sql_bounds = f"""
                SELECT
                  ST_XMin(bbox) AS minx,
                  ST_YMin(bbox) AS miny,
                  ST_XMax(bbox) AS maxx,
                  ST_YMax(bbox) AS maxy
                FROM (
                  SELECT ST_TileEnvelope({iz}, {ix}, {iy}) AS bbox
                ) t
            """

            def _execute_bounds(cur: duckdb.DuckDBPyConnection):
                return cur.execute(sql_bounds).fetchone()

            bounds_row = await run_db_task(_execute_bounds, query_id=query_id)
            if bounds_row and all(v is not None for v in bounds_row):
                bounds_merc = (float(bounds_row[0]), float(bounds_row[1]), float(bounds_row[2]), float(bounds_row[3]))
            else:
                print("Fallback to global WebMercator if something went wrong")
                # Fallback to global WebMercator if something went wrong
                bounds_merc = (-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244)

            print(f"tile x,y,z: {ix}, {iy}, {iz}, bounds_merc: {bounds_merc}, num rows: {len(rows)}, Query:\n{sql}")

            # Convert to MVT layer
            features = [
                {
                    "geometry": row[0],  # WKT geometry string
                    "properties": {},
                }
                for row in rows
                if row and row[0]
            ]

            # Transformer to Web Mercator and quantization bounds to tile bounds
            default_options = {
                "quantize_bounds": bounds_merc,
            }

            tile_pbf = mapbox_vector_tile.encode(
                [
                    {
                        "name": tableName,
                        "features": features,
                    }
                ],
                default_options=default_options,
            )
            # Persist to cache
            if getattr(self, "cache", None) is not None:
                try:
                    self.cache[cache_key] = tile_pbf
                except Exception:
                    # Best-effort caching
                    pass

            resp.content_type = "application/vnd.mapbox-vector-tile"
            resp.data = tile_pbf
            if hasattr(resp, "text"):
                resp.text = None
        except Exception as e:
            logger.exception("Error generating dynamic tile")
            resp.status = falcon.HTTP_500
            resp.content_type = "application/json"
            resp.text = make_error_response("TILE_ERROR", str(e))



class DynamicTileMetadataResource:
    """Return empty metadata for a given table/geometry column."""

    def __init__(self):
        pass

    async def on_get(self, req: Request, resp: Response, tableName: str, columnName: str):
        try:
            q_table = _quote_ident(tableName)
            q_column = _quote_ident(columnName)
        except ValueError as ve:
            resp.status = falcon.HTTP_400
            resp.content_type = "application/json"
            resp.text = make_error_response("INVALID_IDENTIFIER", str(ve))
            return

        try:
            if GLOBAL_CON is None:
                resp.status = falcon.HTTP_500
                resp.content_type = "application/json"
                resp.text = make_error_response("NO_DB", "DuckDB connection is not initialized")
                return

            sql = f"""
                WITH envelope AS (
                  SELECT ST_Envelope({q_column}) AS envelope
                  FROM {q_table}
                )
                SELECT
                  MIN(ST_XMin(envelope)) AS minx,
                  MIN(ST_YMin(envelope)) AS miny,
                  MAX(ST_XMax(envelope)) AS maxx,
                  MAX(ST_YMax(envelope)) AS maxy
                FROM envelope
            """
            cur: duckdb.DuckDBPyConnection = GLOBAL_CON.cursor()
            row = cur.execute(sql).fetchone()
            cur.close()

            if not row or any(v is None for v in row):
                # No geometries or unable to compute extent; return empty object
                resp.content_type = "application/json"
                resp.text = "{}"
                if hasattr(resp, "data"):
                    resp.data = None
                return

            minx, miny, maxx, maxy = map(float, row)
            center_lon = (minx + maxx) / 2.0
            center_lat = (miny + maxy) / 2.0

            # Retrieve non-geometry columns and their types
            try:
                if "." in tableName:
                    schema_name, plain_table_name = tableName.split(".", 1)
                else:
                    schema_name, plain_table_name = "main", tableName

                cur_cols: duckdb.DuckDBPyConnection = GLOBAL_CON.cursor()
                col_rows = cur_cols.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = ? AND table_name = ?
                    ORDER BY ordinal_position
                    """,
                    [schema_name, plain_table_name],
                ).fetchall()
                cur_cols.close()

                fields = [
                    {"name": name, "type": dtype}
                    for name, dtype in col_rows
                    if name and name.lower() != columnName.lower()
                ]
            except Exception:
                fields = []

            metadata = {
                "center": f"{center_lon:.6f},{center_lat:.6f},0",
                "bounds": f"{minx:.6f},{miny:.6f},{maxx:.6f},{maxy:.6f}",
                "name": f"{tableName}.{columnName}",
                "fields": fields,
            }

            resp.content_type = "application/json"
            resp.text = json.dumps(metadata)
            if hasattr(resp, "data"):
                resp.data = None
        except Exception as e:
            logger.exception("Error generating dynamic tile metadata")
            resp.status = falcon.HTTP_500
            resp.content_type = "application/json"
            resp.text = make_error_response("METADATA_ERROR", str(e))
