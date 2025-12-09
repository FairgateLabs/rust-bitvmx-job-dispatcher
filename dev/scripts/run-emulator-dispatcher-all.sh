#!/bin/bash

# NOTE: This script is intended to be run from the root of the repository.
# It starts multiple instances of the bitvmx-emulator-dispatcher for testing purposes.
# Each instance will run on a different port and use a separate storage path.
# Update `ports` and `OP_COUNT` as needed.

# Ensure cleanup of bitvmx-client processes on script exit
pkill -f bitvmx-emulator-dispatcher || true


# Date pieces
timestamp_date=$(date +%y%m%d)
timestamp_time=$(date +%H%M)

LOGS_DIR="logs/${timestamp_date}/${timestamp_time}"
rm -rf "$LOGS_DIR"
mkdir -p "$LOGS_DIR"
rm -rf temp-runs runs

echo "Running Job Dispatcher Emulators..."

# Ports to start operator instances on
ports=(22222 33333 44444 55554)
OP_COUNT=4

for i in $(seq 1 $OP_COUNT); do
  RUST_BACKTRACE=full cargo run --release --bin bitvmx-emulator-dispatcher -- \
  --port ${ports[i-1]} \
  --storage-path "temp-runs/${i}/storage_job.db" 2>&1 \
  | sed -u -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})*)?[mGKHF]//g" > "$LOGS_DIR/dispatcher_emulator_${i}.log" &
done
