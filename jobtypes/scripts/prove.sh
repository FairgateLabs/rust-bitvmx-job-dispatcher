#!/bin/sh
# Proves a RISC Zero guest: a stark first, then the groth16 snark that wraps it.
#
# Usage: prove.sh <input> <elf> <output_dir>
#
# It runs from the directory of the prover repository, so the binaries are found relative to it. It writes
# <output_dir>/stark_proof.bin, <output_dir>/stark.json and <output_dir>/output.json, which is the result the
# dispatcher reads.
#
# Environment (optional):
#   PREFETCH_DIR  where to look for the files of the groth16 prover (default /var/lib/containerd).

set -u

INPUT=$1
ELF=$2
OUT=$3

BIN=../rust-bitvmx-zk-proof/target/release

# Size of each read of the prefetch, in MiB, and how many reads run at the same time.
CHUNK_MIB=64
PARALLEL_READS=128
export CHUNK_MIB

# A volume restored from an AMI snapshot fetches each block from S3 the first time it is read, at about 5 MB/s
# for a single reader. The groth16 prover reads the proving key (2.2 GB), the constraint system and the
# stark_verify binary from its docker image, so on a new instance the snark would wait about 8 minutes for the
# disk. Reading those files first, in parallel chunks, takes about 80 seconds.
# The files are found by name: nothing happens on images without them.
prefetch_groth16_files() {
    chunk_bytes=$((CHUNK_MIB * 1024 * 1024))

    # The command of xargs is in single quotes on purpose: $0, $1 and CHUNK_MIB expand in the shell that xargs
    # starts for each chunk.
    # shellcheck disable=SC2016
    find "${PREFETCH_DIR:-/var/lib/containerd}" -type f \
        \( -name stark_verify_final.pk.dmp -o -name stark_verify.cs -o -name stark_verify \) \
        -printf '%s %p\n' 2>/dev/null |
        while read -r size file; do
            # One line "<file> <chunk number>" per chunk, rounding the number of chunks up.
            chunks=$(((size + chunk_bytes - 1) / chunk_bytes))
            i=0
            while [ "$i" -lt "$chunks" ]; do
                echo "$file $i"
                i=$((i + 1))
            done
        done |
        xargs -r -P "$PARALLEL_READS" -L 1 \
            sh -c 'dd if="$0" of=/dev/null bs=1M skip=$(($1 * CHUNK_MIB)) count="$CHUNK_MIB" 2>/dev/null'
}

# `host` is the CPU build. `host-gpu` is the cuda build, present only on GPU images, and much faster for the
# stark. The snark always runs with `host`: the cuda groth16 prover crashes.
STARK_BIN=$BIN/host-gpu
[ -x "$STARK_BIN" ] || STARK_BIN=$BIN/host
SNARK_BIN=$BIN/host

# A failed prefetch only makes the snark slower.
prefetch_groth16_files || true

# Each step writes its own json: prove-snark overwrites the json it writes, so a failed snark must not be able
# to overwrite the stark result it reads.
"$STARK_BIN" prove-stark \
    --input "$INPUT" \
    --elf "$ELF" \
    --output "$OUT/stark_proof.bin" \
    --json "$OUT/stark.json" &&
    "$SNARK_BIN" prove-snark \
        --input "$OUT/stark_proof.bin" \
        --json "$OUT/output.json" \
        --json-input "$OUT/stark.json"
