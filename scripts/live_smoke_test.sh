#!/usr/bin/env bash
# Live smoke test for sf-synth v0.4.0 against a real Snowflake account.
#
# Run from the repo root with the venv active:
#     source .venv/bin/activate
#     bash scripts/live_smoke_test.sh ayush
#
# This will create / drop tables in ABC.AYUSH that all start with the prefix
# SF_SYNTH_TEST_, so the cleanup at the end is exact.

set -uo pipefail

CONN="${1:-ayush}"
CFG=test_features.yaml
PASS=0
FAIL=0
FAILED_STEPS=()

step() {
  local name="$1"; shift
  echo
  echo "================================================================"
  echo "STEP: $name"
  echo "  CMD: $*"
  echo "================================================================"
  if "$@"; then
    PASS=$((PASS+1))
    echo "  --> PASS: $name"
  else
    FAIL=$((FAIL+1))
    FAILED_STEPS+=("$name")
    echo "  --> FAIL: $name"
  fi
}

echo
echo "+++ sf-synth $(sf-synth version 2>&1) live smoke test +++"
echo "Connection: $CONN"
echo "Config:     $CFG"
echo

# ---- 1. validate config against live DDL ----
# (USERS/EVENTS/ORDERS may not exist yet - that is expected on a clean run.)
step "validate config" \
  sf-synth validate "$CFG" --connection "$CONN" || true

# ---- 2. preview without writes ----
step "preview 5 rows of every table (no writes)" \
  sf-synth preview "$CFG" --rows 5 --connection "$CONN"

# ---- 3. baseline generate (replace, parallel=2) ----
step "generate baseline (replace, parallel=2)" \
  sf-synth generate "$CFG" --mode replace --parallel 2 --connection "$CONN"

# ---- 4. counts after baseline ----
step "count after baseline" \
  sf-synth count "$CFG" --connection "$CONN"

# ---- 5. report + profile ----
step "generate with --report run_report.md --profile" \
  sf-synth generate "$CFG" --mode replace --report run_report.md --profile --connection "$CONN"
if [ -f run_report.md ]; then
  echo "  Report written: $(wc -l < run_report.md) lines"
  echo "  --- first 60 lines of report ---"
  head -60 run_report.md
fi

# ---- 6. append mode (USERS only) ----
step "generate --mode append (USERS only - rows accumulate)" \
  sf-synth generate "$CFG" --mode append --tables SF_SYNTH_TEST_USERS --connection "$CONN"
step "count after append (USERS should be 200)" \
  sf-synth count "$CFG" --connection "$CONN"

# ---- 7. fill_to mode (top up to 100) ----
# After append we have 200 rows in USERS but config target is 100. fill_to
# should be a no-op since current >= target.
step "generate --mode fill_to (no-op since current >= target)" \
  sf-synth generate "$CFG" --mode fill_to --tables SF_SYNTH_TEST_USERS --connection "$CONN"
step "count after fill_to (USERS should still be 200)" \
  sf-synth count "$CFG" --connection "$CONN"

# ---- 8. upsert mode ----
step "generate --mode upsert (USERS only - MERGE)" \
  sf-synth generate "$CFG" --mode upsert --tables SF_SYNTH_TEST_USERS --connection "$CONN"
step "count after upsert" \
  sf-synth count "$CFG" --connection "$CONN"

# ---- 9. truncate flag override ----
step "generate --no-truncate (additive, EVENTS only)" \
  sf-synth generate "$CFG" --mode replace --no-truncate --tables SF_SYNTH_TEST_EVENTS --connection "$CONN"

# ---- 10. quiet / verbose ----
step "generate --quiet (suppresses warnings)" \
  sf-synth generate "$CFG" --quiet --tables SF_SYNTH_TEST_EVENTS --connection "$CONN"
step "generate --verbose (logs everything)" \
  sf-synth generate "$CFG" --verbose --tables SF_SYNTH_TEST_EVENTS --connection "$CONN" || true

# ---- 11. final preview to inspect quality ----
step "final preview to inspect generated row quality" \
  sf-synth preview "$CFG" --rows 5 --connection "$CONN"

# ---- 12. clean up ----
step "clean up (drop test tables)" \
  sf-synth clean "$CFG" --drop-tables --yes --connection "$CONN"

echo
echo "================================================================"
echo "SUMMARY"
echo "================================================================"
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
if [ $FAIL -gt 0 ]; then
  echo "  Failed steps:"
  for s in "${FAILED_STEPS[@]}"; do echo "    - $s"; done
  exit 1
fi
echo "  All steps OK."
