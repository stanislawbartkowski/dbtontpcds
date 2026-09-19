#!/usr/bin/env python3
"""Load TPC-DS pipe-delimited .dat files into the tpc_raw schema in Databricks.

Mirrors scripts/load_raw_data_spark.py for the Databricks target set up by
scripts/create_raw_schema_databricks.py: each .dat file is gzipped into a
temporary directory (the staging endpoint drops the connection on
multi-hundred-MB uploads, and COPY INTO's CSV reader decompresses .gz
transparently), uploaded into a Unity Catalog volume
(`<catalog>.tpc_raw.raw_stage`) via the SQL connector's staging endpoint
(with retries), then bulk-loaded with COPY INTO. Each table's column names
and types are read back from the catalog (so they always match what the
create script created, including its TIME -> STRING adjustment) and drive
explicit casts in the COPY INTO select list. The trailing `|` dsdgen emits
at the end of every row simply parses as one extra empty column, which the
select list never references, so the file contents are staged as-is.

Each table is TRUNCATEd first and COPY INTO runs with 'force' = 'true'
(otherwise it skips files it has already loaded once), so it's safe to
re-run.

Cloud object stores reject a single PUT over 5 GiB (S3's hard per-object
limit for a non-multipart upload, which is what the SQL connector's
staging PUT does), so a gzipped .dat file bigger than that is split into
several MAX_CHUNK_BYTES-sized parts, each uploaded and staged as its own
file under a per-table subdirectory; COPY INTO then reads the whole
directory as one source.

Connection settings come from the same environment variables dbt uses
(source .env first): DBT_DATABRICKS_HOST, DBT_DATABRICKS_HTTP_PATH,
DBT_DATABRICKS_TOKEN, DBT_CATALOG.

Usage: ./load_raw_data_databricks.py <dat_directory>
  dat_directory     Directory containing the *.dat files (e.g. DSGen-software-code-4.0.0/dat)
"""
import gzip
import os
import sys
import tempfile
import time

SCHEMA = "tpc_raw"
VOLUME = "raw_stage"
PUT_ATTEMPTS = 3
READ_BLOCK_BYTES = 8 * 1024 * 1024
MAX_CHUNK_BYTES = 4 * 1024**3  # stay safely under S3's 5 GiB single-PUT limit

TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "dbgen_version",
    "household_demographics", "income_band", "inventory", "item", "promotion", "reason",
    "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse",
    "web_page", "web_returns", "web_sales", "web_site",
]


class _CountingWriter:
    """Tracks bytes written to the underlying (compressed) file object."""

    def __init__(self, fileobj):
        self._fileobj = fileobj
        self.bytes_written = 0

    def write(self, data: bytes) -> int:
        self.bytes_written += len(data)
        return self._fileobj.write(data)

    def flush(self) -> None:
        self._fileobj.flush()


def compress_in_chunks(src_path: str, stage_dir: str, table: str) -> list[str]:
    """Gzips src_path into one or more <= MAX_CHUNK_BYTES compressed parts.

    Each part boundary is aligned to a source line (row) boundary: COPY INTO
    parses every chunk file as an independent CSV source, so cutting a chunk
    mid-row would split that row into two malformed, misaligned records in
    two different files.
    """
    chunk_paths = []
    chunk_index = 0

    def open_chunk():
        path = os.path.join(stage_dir, f"{table}_{chunk_index:04d}.dat.gz")
        chunk_paths.append(path)
        raw = open(path, "wb")
        counter = _CountingWriter(raw)
        return raw, counter, gzip.GzipFile(fileobj=counter, mode="wb", compresslevel=1)

    with open(src_path, "rb") as src:
        raw_file, counter, gz = open_chunk()
        leftover = b""
        while True:
            block = src.read(READ_BLOCK_BYTES)
            if not block:
                break
            data = leftover + block
            split_at = data.rfind(b"\n") + 1  # 0 if no newline in data yet
            gz.write(data[:split_at])
            leftover = data[split_at:]
            gz.flush()  # force zlib to emit buffered output so bytes_written is accurate
            if split_at and counter.bytes_written >= MAX_CHUNK_BYTES:
                gz.close()
                raw_file.close()
                chunk_index += 1
                raw_file, counter, gz = open_chunk()
        if leftover:
            gz.write(leftover)
        gz.close()
        raw_file.close()

    return chunk_paths


def table_columns(cursor, table: str) -> list[tuple[str, str]]:
    cursor.execute(f"DESCRIBE TABLE {table}")
    columns = []
    for name, data_type, _comment in cursor.fetchall():
        if not name or name.startswith("#"):
            break
        columns.append((name, data_type))
    return columns


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <dat_directory>", file=sys.stderr)
        sys.exit(1)
    dat_dir = os.path.abspath(sys.argv[1])

    if not os.path.isdir(dat_dir):
        print(f"Error: dat directory not found: {dat_dir}", file=sys.stderr)
        sys.exit(1)

    dat_paths = {}
    for table in TABLES:
        dat_path = os.path.join(dat_dir, f"{table}.dat")
        if not os.path.isfile(dat_path):
            print(f"Error: missing .dat file for table '{table}': {dat_path}", file=sys.stderr)
            sys.exit(1)
        dat_paths[table] = dat_path

    try:
        host = os.environ["DBT_DATABRICKS_HOST"]
        http_path = os.environ["DBT_DATABRICKS_HTTP_PATH"]
        token = os.environ["DBT_DATABRICKS_TOKEN"]
    except KeyError as missing:
        print(f"Missing environment variable {missing} - source .env first.", file=sys.stderr)
        sys.exit(1)
    catalog = os.environ.get("DBT_CATALOG", "main")

    from databricks import sql

    with tempfile.TemporaryDirectory(prefix="tpcds_gz_") as stage_dir, sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
        staging_allowed_local_path=stage_dir,
    ) as conn, conn.cursor() as cursor:
        cursor.execute(f"USE {catalog}.{SCHEMA}")
        cursor.execute(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")
        volume_dir = f"/Volumes/{catalog}/{SCHEMA}/{VOLUME}"

        for table in TABLES:
            print(f"Compressing {table}.dat...", flush=True)
            chunk_paths = compress_in_chunks(dat_paths[table], stage_dir, table)

            staged_dir = f"{volume_dir}/{table}/"
            try:
                cursor.execute(f"REMOVE '{staged_dir}'")
            except sql.exc.ServerOperationError:
                pass  # nothing staged from a previous run

            for i, chunk_path in enumerate(chunk_paths, start=1):
                chunk_name = os.path.basename(chunk_path)
                print(f"Uploading {chunk_name} ({i}/{len(chunk_paths)})...", flush=True)
                for attempt in range(1, PUT_ATTEMPTS + 1):
                    try:
                        cursor.execute(f"PUT '{chunk_path}' INTO '{staged_dir}{chunk_name}' OVERWRITE")
                        break
                    except sql.exc.RequestError:
                        if attempt == PUT_ATTEMPTS:
                            raise
                        print(f"  upload failed (attempt {attempt}/{PUT_ATTEMPTS}), retrying...", flush=True)
                        time.sleep(5)
                os.remove(chunk_path)

            select_list = ", ".join(
                f"CAST(_c{i} AS {data_type}) AS {name}"
                for i, (name, data_type) in enumerate(table_columns(cursor, table))
            )
            print(f"Loading {catalog}.{SCHEMA}.{table}...", flush=True)
            cursor.execute(f"TRUNCATE TABLE {table}")
            cursor.execute(
                f"""
                COPY INTO {table}
                FROM (SELECT {select_list} FROM '{staged_dir}')
                FILEFORMAT = CSV
                FORMAT_OPTIONS ('sep' = '|', 'header' = 'false')
                COPY_OPTIONS ('force' = 'true')
                """
            )
            rows = cursor.fetchone()
            print(f"  {rows.num_inserted_rows} rows", flush=True)

    print(f"Loaded {len(TABLES)} tables into schema '{catalog}.{SCHEMA}'.")


if __name__ == "__main__":
    main()
