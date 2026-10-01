#!/usr/bin/env bash
# Start the Spark Connect server (sc://localhost:15002) with a persistent
# Hive warehouse under $HOME. Settings can be overridden via env vars:
#   SPARK_DRIVER_MEMORY  driver heap; local mode runs everything in the
#                        driver, so this is the server's memory limit
#                        (default: 6g - Spark's own default of 1g is too
#                        small for SCALE 10/100)
#   SPARK_LOCAL_DIR      shuffle/spill scratch dir (default: $HOME/spark-tmp).
#                        Kept out of /tmp because systemd-tmpfiles deletes
#                        old files there, which breaks every shuffle on a
#                        long-running server.
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-6g}"
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-$HOME/spark-tmp}"
export SPARK_LOG_DIR="${SPARK_LOG_DIR:-$HOME/spark-logs}"
export SPARK_PID_DIR="${SPARK_PID_DIR:-$HOME/spark-pid}"

mkdir -p "$SPARK_LOCAL_DIR" "$SPARK_LOG_DIR" "$SPARK_PID_DIR"

"$SPARK_HOME/sbin/start-connect-server.sh" \
  --packages org.apache.spark:spark-connect_2.13:4.2.0 \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --conf spark.local.dir="$SPARK_LOCAL_DIR" \
  --conf spark.sql.warehouse.dir="$HOME/spark-warehouse" \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=$HOME/spark-metastore_db;create=true" \
  --conf spark.driver.extraJavaOptions="-Djdk.lang.Process.launchMechanism=FORK"
