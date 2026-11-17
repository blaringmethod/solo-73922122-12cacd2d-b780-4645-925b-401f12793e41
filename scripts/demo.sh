#!/usr/bin/env bash
# End-to-end demonstrations against a running docker-compose stack:
#   1. disconnect/crash recovery (witness accepts, archive dies before
#      registering, restart reconciles from the witness, no double archive)
#   2. original corruption detected by the witness-driven verification page
#
# Usage:  ./scripts/demo.sh
set -euo pipefail

ARCHIVE_URL="${ARCHIVE_URL:-http://127.0.0.1:8000}"
WITNESS_URL="${WITNESS_URL:-http://127.0.0.1:8001}"
BATCH="BATCH-DEMO-$(date +%H%M%S)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

line() { printf '\n========== %s ==========\n' "$1"; }

line "0. service health"
curl -fsS "$WITNESS_URL/health" | python3 -m json.tool
curl -fsS "$ARCHIVE_URL/api/batches" >/dev/null && echo "archive reachable"

line "1. arm the crash marker (next upload dies after witness acceptance)"
curl -fsS -X POST "$ARCHIVE_URL/api/admin/arm-crash" | python3 -m json.tool

printf 'sample_id,reading\ns1,12.4\ns2,13.1\n' > "$TMP/readings.csv"
line "2. upload $TMP/readings.csv to batch $BATCH (connection will be cut)"
set +e
curl -sS -m 8 -X POST "$ARCHIVE_URL/api/batches/$BATCH/files" \
    -F filename=readings.csv -F "file=@$TMP/readings.csv"
echo
echo "(curl exit $? — expected: the archive container exited on purpose)"
set -e

line "3. wait for the restart policy to bring the archive back"
for _ in $(seq 1 20); do
  if curl -fsS "$ARCHIVE_URL/api/batches" >/dev/null 2>&1; then break; fi
  sleep 1
done

line "4. the witness already holds the receipt; the restart reconciles it"
curl -fsS "$WITNESS_URL/batches/$BATCH" | python3 -m json.tool
curl -fsS "$ARCHIVE_URL/api/batches/$BATCH" | python3 -m json.tool \
    | grep -E '"status"|"registered_count"'

line "5. confirm the witness has exactly ONE record (no duplicate re-archive)"
curl -fsS "$WITNESS_URL/receipts" | python3 -c "
import json,sys
items = json.load(sys.stdin)['items']
hits = [i for i in items if i['batch_id']=='$BATCH']
print(f'records for $BATCH: {len(hits)} (expected 1)')
assert len(hits) == 1"

line "6. full verification BEFORE tampering — should pass"
curl -fsS "$ARCHIVE_URL/api/verify" | python3 -c "
import json,sys
r = json.load(sys.stdin)
print('witness_reachable =', r['witness_reachable'], '| passed =', r['passed'])"

line "7. corrupt the stored original inside the archive volume"
docker exec research-archive sh -c \
  "printf 'CORRUPTED' >> /data/originals/$BATCH/readings.csv"
echo "appended bytes to /data/originals/$BATCH/readings.csv"

line "8. verification AFTER tampering — must report tampered_original"
curl -fsS "$ARCHIVE_URL/api/verify" | python3 -c "
import json,sys
r = json.load(sys.stdin)
assert r['passed'] is False
for b in r['batches']:
    if b['batch_id'] == '$BATCH':
        for f in b['files']:
            print('problems:', [p['type'] for p in f['problems']])
            assert any(p['type'] == 'tampered_original' for p in f['problems'])
print('tamper detected correctly')"

line "9. download now refuses the corrupted file"
set +e
curl -sS -o /dev/null -w "download HTTP %{http_code} (expected 409)\n" \
  "$ARCHIVE_URL/api/batches/$BATCH/download/readings.csv"
set -e

line "10. batch cooling: freeze the list, zip the originals, clean hot files"
COLD_BATCH="BATCH-COLD-$(date +%H%M%S)"
for n in alpha beta gamma; do
  printf 'sensor,value\n%s,%.2f\n' "$n" "$RANDOM" > "$TMP/$n.csv"
  curl -fsS -X POST "$ARCHIVE_URL/api/batches/$COLD_BATCH/files" \
       -F filename="$n.csv" -F "file=@$TMP/$n.csv" >/dev/null
done
curl -fsS -X POST "$ARCHIVE_URL/api/batches/$COLD_BATCH/cold" \
  | python3 -c "import json,sys; j=json.load(sys.stdin)['job']; print('queued manifest:', [m['filename'] for m in j['manifest']])"
for _ in $(seq 1 30); do
  state=$(curl -fsS "$ARCHIVE_URL/api/batches/$COLD_BATCH/cold" \
          | python3 -c "import json,sys; print(json.load(sys.stdin)['job']['state'])")
  [ "$state" = "completed" ] && break
  [ "$state" = "failed" ] && { echo "cooling FAILED"; exit 1; }
  sleep 1
done
echo "cooling final state: $state"
curl -fsS "$ARCHIVE_URL/api/batches/$COLD_BATCH" | python3 -c "
import json,sys
d=json.load(sys.stdin)
assert d['status']=='complete' and d['cold_count']==3, d
print('catalog storage:', [(f['filename'],f['storage']) for f in d['files']])"
docker exec research-archive sh -c \
  "ls -la /data/cold/$COLD_BATCH.zip && test ! -e /data/originals/$COLD_BATCH" \
  && echo "cold package persisted; hot originals cleaned"
# Single-member read out of the ZIP, byte-identical to the original upload.
curl -fsS "$ARCHIVE_URL/api/batches/$COLD_BATCH/download/alpha.csv" -o "$TMP/alpha.dl"
cmp "$TMP/alpha.csv" "$TMP/alpha.dl" && echo "cold download byte-identical"

line "11. verification covers cold packages and still passes"
curl -fsS "$ARCHIVE_URL/api/verify" | python3 -c "
import json,sys
r=json.load(sys.stdin)
assert r['passed'] is True, r
print('verification passed across hot + cold originals')"

line "DEMO COMPLETE"
echo "Open the web UI: $ARCHIVE_URL  (batch page: $ARCHIVE_URL/batches/$BATCH)"
echo "Witness receipt list: $WITNESS_URL/receipts"
