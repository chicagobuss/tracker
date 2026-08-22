#!/usr/bin/env bash
# Behavioural tests for scripts/blobpack.py, run against the file backend in a
# scratch directory. No docker, no S3, no repo state touched.
#
#   scripts/blobpack_test.sh
#
# Each case is a property the backup depends on and that was, at some point,
# actually broken: a backup that reports success must be restorable, and a
# restore must refuse rather than write partial or wrong content.
set -uo pipefail
cd "$(dirname "$0")/.."
BP="$PWD/scripts/blobpack.py"
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
PASS=0; FAIL=0

ok()   { PASS=$((PASS+1)); echo "  ok    $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  FAIL  $1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want $3, got $2)"; fi; }

seed() { # seed <dir> <count>
  mkdir -p "$1/sha256"
  python3 - "$1" "$2" <<'PY'
import hashlib, sys
d, n = sys.argv[1], int(sys.argv[2])
for i in range(n):
    b = (f"blob-{i}-" + "x" * (37 * i)).encode()
    open(f"{d}/sha256/{hashlib.sha256(b).hexdigest()}", "wb").write(b)
PY
}
keys() { grep -v '^#' "$1/INDEX" | awk '{print $1}'; }
bp()   { STORAGE_TYPE=file BLOB_DIR="$1" uv run --quiet "$BP" "${@:2}"; }

# Flip a byte inside a member's PAYLOAD. Corrupting a fixed offset can land in a
# tar header or in padding, where nothing a hash covers actually changes.
rot() {
  python3 - "$1" <<'PYR'
import glob, sys, tarfile
p = glob.glob(f"{sys.argv[1]}/blobs-*.tar")[0]
with tarfile.open(p) as tf:
    m = max((x for x in tf.getmembers() if x.isfile() and x.size > 0), key=lambda x: x.size)
    off = m.offset_data
d = bytearray(open(p, "rb").read()); d[off] ^= 0xFF
open(p, "wb").write(bytes(d))
print(f"corrupted {m.name[:20]}... at payload offset {off}")
PYR
}


echo "blobpack behavioural tests"

# --- a fold is idempotent, and a round trip reproduces every byte -------------
S="$WORK/store"; P="$WORK/packs"; O="$WORK/out"; mkdir -p "$P" "$O"
seed "$S" 40
bp "$S" fold "$P" --all >/dev/null
OUT=$(bp "$S" fold "$P" --all)
case "$OUT" in *"0 new"*) ok "second fold is a no-op" ;; *) bad "second fold is a no-op" ;; esac
check "index holds every blob" "$(keys "$P" | wc -l | tr -d ' ')" "40"
keys "$P" > "$WORK/k.txt"
bp "$O" emit "$P" "$WORK/k.txt" >/dev/null
check "round trip restores every blob" "$(find "$O" -type f | wc -l | tr -d ' ')" "40"
BAD=$(python3 - "$O" <<'PY'
import hashlib, os, sys
print(sum(1 for r, _, fs in os.walk(sys.argv[1]) for f in fs
          if hashlib.sha256(open(os.path.join(r, f), "rb").read()).hexdigest() != f))
PY
)
check "restored bytes match their own hashes" "$BAD" "0"

# --- an interrupted fold must not duplicate members on retry -----------------
python3 - "$P" <<'PYX'
import hashlib, sys
open(f"{sys.argv[1]}/INDEX", "w").write("#blobpack-index-v3 sha256=" + hashlib.sha256(b"").hexdigest() + "\n")
PYX
bp "$S" fold "$P" --all >/dev/null 2>&1
check "retry after a lost index succeeds" "$?" "0"
check "retry after a lost index re-indexes every blob" "$(keys "$P" | wc -l | tr -d ' ')" "40"
CUR=$(grep '^#pack ' "$P/INDEX" | awk '{print $2}')
check "retry after a lost index does not duplicate" \
  "$(tar tf "$P/$CUR" | sort | uniq -d | wc -l | tr -d ' ')" "0"
bp "$S" verify "$P" >/dev/null 2>&1
check "retry after a lost index leaves packs verifiable" "$?" "0"

# --- an inherited pack must be validated, and clean growth must still work ---
S2="$WORK/s2"; P2="$WORK/p2"; mkdir -p "$P2"; seed "$S2" 5
bp "$S2" fold "$P2" --all >/dev/null
# Positive control first: without it, a mutant that rejects EVERY incremental
# fold would pass the corruption case below purely by always failing.
seed "$S2" 6
bp "$S2" fold "$P2" --all >/dev/null 2>&1
check "a clean incremental fold succeeds" "$?" "0"
check "a clean incremental fold indexes the new blob" "$(keys "$P2" | wc -l | tr -d ' ')" "6"

rot "$P2" >/dev/null
IDX_BEFORE=$(sha256sum "$P2/INDEX" | awk '{print $1}')
PACKS_BEFORE=$(ls "$P2"/blobs-*.tar | sort | tr '\n' ' ')
seed "$S2" 7
bp "$S2" fold "$P2" --all >/dev/null 2>&1
check "fold refuses to carry a corrupt member forward" "$?" "1"
check "  and leaves the index untouched" "$(sha256sum "$P2/INDEX" | awk '{print $1}')" "$IDX_BEFORE"
check "  and leaves the pack set untouched" "$(ls "$P2"/blobs-*.tar | sort | tr '\n' ' ')" "$PACKS_BEFORE"

# --- verify catches what a restore would trip over ---------------------------
S3="$WORK/s3"; P3="$WORK/p3"; mkdir -p "$P3"; seed "$S3" 6
bp "$S3" fold "$P3" --all >/dev/null
bp "$S3" verify "$P3" >/dev/null 2>&1; check "clean packs verify clean" "$?" "0"
python3 - "$P3" <<'PY'
import hashlib, sys
p = f"{sys.argv[1]}/INDEX"
lines = open(p).read().splitlines()
body = [l for l in lines[1:]] + ["sha256/" + "0" * 64 + " " + [l.split()[1] for l in lines if l.startswith("#pack ")][0]]
b = "\n".join(body) + "\n"
open(p, "w").write("#blobpack-index-v3 sha256=" + hashlib.sha256(b.encode()).hexdigest() + "\n" + b)
PY
bp "$S3" verify "$P3" >/dev/null 2>&1; check "verify catches an index entry with no member" "$?" "1"
echo "sha256/$(printf '0%.0s' $(seq 64))" > "$WORK/ghost.txt"
O3="$WORK/o3"; mkdir -p "$O3"
bp "$O3" emit "$P3" "$WORK/ghost.txt" >/dev/null 2>&1
check "emit refuses a key whose bytes are absent" "$?" "1"
check "a refused emit writes nothing" "$(find "$O3" -type f | wc -l | tr -d ' ')" "0"

# --- a pack rewritten to the same length must not read as unchanged ----------
S4="$WORK/s4"; P4="$WORK/p4"; mkdir -p "$P4"; seed "$S4" 8
bp "$S4" fold "$P4" --all >/dev/null
rot "$P4" >/dev/null
# Capture, then match. Piping into `grep -q` under `set -o pipefail` makes grep
# exit at the first hit, SIGPIPEs the producer, and reports pipeline failure.
OUT=$(bp "$S4" verify "$P4" 2>&1); ST=$?
case "$OUT" in
  *"PACK DIGEST MISMATCH"*) ok "verify reports a same-length pack rewrite" ;;
  *) bad "verify reports a same-length pack rewrite" ;;
esac
# A warning-only verifier would pass the check above, so require the status too.
check "verify FAILS on a same-length pack rewrite" "$ST" "1"
bp "$S4" coverage "$P4" >/dev/null 2>&1
check "coverage rejects a same-length pack rewrite" "$?" "1"

# --- legacy packs from the original append implementation --------------------
P5="$WORK/p5"; mkdir -p "$P5"
python3 - "$P5" "$WORK" <<'PY'
import hashlib, io, sys, tarfile
p, w = sys.argv[1], sys.argv[2]
blobs = {}
for i in range(3):
    d = f"legacy-{i}".encode(); blobs["sha256/" + hashlib.sha256(d).hexdigest()] = d
with tarfile.open(f"{p}/blobs-20260820-001.tar", "w") as tf:
    for n, d in sorted(blobs.items()):
        ti = tarfile.TarInfo(n); ti.size = len(d); tf.addfile(ti, io.BytesIO(d))
    n = sorted(blobs)[0]; d = blobs[n]          # the duplicate a crashed append left
    ti = tarfile.TarInfo(n); ti.size = len(d); tf.addfile(ti, io.BytesIO(d))
body = "".join(f"{k} blobs-20260820-001.tar\n" for k in sorted(blobs))
open(f"{p}/INDEX", "w").write("#blobpack-index-v3 sha256=" + hashlib.sha256(body.encode()).hexdigest() + "\n" + body)
open(f"{w}/legacy.txt", "w").write("\n".join(sorted(blobs)) + "\n")
PY
bp "$WORK/x" verify "$P5" >/dev/null 2>&1; check "verify flags a duplicated member" "$?" "1"
O5="$WORK/o5"; mkdir -p "$O5"
bp "$O5" emit "$P5" "$WORK/legacy.txt" >/dev/null 2>&1
check "a duplicated member still restores" "$?" "0"
check "  and restores every key" "$(find "$O5" -type f | wc -l | tr -d ' ')" "3"
rm -f "$P5/INDEX"
bp "$WORK/x" reindex "$P5" >/dev/null 2>&1
check "reindex rebuilds a deleted index" "$?" "0"
check "  recovering every key" "$(keys "$P5" | wc -l | tr -d ' ')" "3"
check "  collapsing the duplicate to one entry" \
  "$(keys "$P5" | sort | uniq -d | wc -l | tr -d ' ')" "0"
bp "$WORK/x" coverage "$P5" >/dev/null 2>&1
check "  and the rebuilt index passes coverage" "$?" "0"
O5B="$WORK/o5b"; mkdir -p "$O5B"
bp "$O5B" emit "$P5" "$WORK/legacy.txt" >/dev/null 2>&1
check "  and restores from the rebuilt index" "$(find "$O5B" -type f | wc -l | tr -d ' ')" "3"

# --- concurrent folds must not lose each other's keys ------------------------
# Divergent key sets, deliberately: given identical input both writers can each
# publish the same complete set, so a key count cannot reveal a lost union and
# the test passes even with the lock removed. Statuses are captured too — a bare
# `wait` does not establish that either job succeeded.
S6="$WORK/s6"; P6="$WORK/p6"; mkdir -p "$P6"; seed "$S6" 200
ls "$S6/sha256" | sed 's|^|sha256/|' | sort > "$WORK/all6.txt"
head -100 "$WORK/all6.txt" > "$WORK/a6.txt"
tail -100 "$WORK/all6.txt" > "$WORK/b6.txt"
# BLOBPACK_TEST_PAUSE holds A inside its read-modify-write window so B is
# guaranteed to arrive during it. Without the barrier both folds usually run to
# completion one after the other and the test passes even with no lock at all.
STORAGE_TYPE=file BLOB_DIR="$S6" BLOBPACK_TEST_PAUSE=3 \
  uv run --quiet "$BP" fold "$P6" < "$WORK/a6.txt" >/dev/null 2>&1 & PA=$!
sleep 1
bp "$S6" fold "$P6" < "$WORK/b6.txt" >/dev/null 2>&1 & PB=$!
wait $PA; RA=$?
wait $PB; RB=$?
check "concurrent fold A succeeds" "$RA" "0"
check "concurrent fold B succeeds" "$RB" "0"
check "concurrent folds index the UNION of both inputs" "$(keys "$P6" | wc -l | tr -d ' ')" "200"
MISSING=$(comm -23 "$WORK/all6.txt" <(keys "$P6" | sort) | wc -l | tr -d ' ')
check "  losing none of either writer's keys" "$MISSING" "0"
bp "$S6" verify "$P6" >/dev/null 2>&1; check "concurrent folds leave packs verifiable" "$?" "0"
bp "$S6" coverage "$P6" >/dev/null 2>&1; check "concurrent folds leave packs covered" "$?" "0"

# --- rollover ----------------------------------------------------------------
S7="$WORK/s7"; P7="$WORK/p7"; mkdir -p "$P7"; seed "$S7" 60
STORAGE_TYPE=file BLOB_DIR="$S7" BACKUP_PACK_MAX_BYTES=8000 uv run --quiet "$BP" fold "$P7" --all >/dev/null
[ "$(ls "$P7"/blobs-*.tar | wc -l)" -gt 1 ] && ok "the size cap rolls packs over" || bad "the size cap rolls packs over"
keys "$P7" > "$WORK/k7.txt"; O7="$WORK/o7"; mkdir -p "$O7"
bp "$O7" emit "$P7" "$WORK/k7.txt" >/dev/null
check "a multi-pack restore is complete" "$(find "$O7" -type f | wc -l | tr -d ' ')" "60"

# --- the coverage gate, including the case that used to pass on nothing ------
S8="$WORK/s8"; P8="$WORK/p8"; mkdir -p "$P8"; seed "$S8" 6
bp "$S8" fold "$P8" --all >/dev/null
bp "$S8" coverage "$P8" >/dev/null 2>&1; check "coverage passes on intact packs" "$?" "0"
: > "$WORK/empty.txt"
rot "$P8" >/dev/null
# The defect: coverage driven by a key list hashed only the packs that list
# named. A delete racing the dump can empty that list, so nothing was checked
# and a corrupt generation was certified. Coverage must fail here regardless.
bp "$S8" coverage "$P8" "$WORK/empty.txt" >/dev/null 2>&1
check "coverage fails on a corrupt pack even with an empty key list" "$?" "1"
bp "$S8" coverage "$P8" >/dev/null 2>&1
check "coverage fails on a corrupt pack" "$?" "1"
rm -f "$P8"/blobs-*.tar
bp "$S8" coverage "$P8" >/dev/null 2>&1; check "coverage fails on a missing pack" "$?" "1"

echo
echo "$PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
