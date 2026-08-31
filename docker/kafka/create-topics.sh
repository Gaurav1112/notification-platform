#!/usr/bin/env bash
# =============================================================================
# Creates the 16 platform topics on the local single-node broker.
#
# auto.create.topics.enable is OFF (as in production), so this is mandatory,
# not a convenience. A typo'd topic name must fail loudly at produce time
# rather than quietly materialise a 1-partition topic that works fine until
# the day it does not.
#
# LOCAL PARTITION COUNTS = production count / 6, floor 1.
#   Production totals 288 partitions across 6 brokers (docs/KAFKA.md §2);
#   dividing by 6 gives 48 here and preserves every *ratio*. The ratios are
#   what the code is sensitive to -- relative consumer parallelism, key skew,
#   the 9:1 push.bulk-to-push.tx split. Absolute counts are a broker-sizing
#   concern and are meaningless on one node.
#
# RF=1 and min.insync.replicas=1 throughout, because one broker cannot host a
# second replica. This means the local stack CANNOT reproduce ISR-shrink,
# unclean-leader or min-ISR-blocked-produce behaviour. Those are Testcontainers
# / staging concerns; do not conclude from a green local run that the
# durability configuration is exercised.
#
# Idempotent: --if-not-exists, so `make topics` is safe to re-run.
# =============================================================================
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

DAYS_2=$((2 * 24 * 60 * 60 * 1000))
DAYS_3=$((3 * 24 * 60 * 60 * 1000))
DAYS_7=$((7 * 24 * 60 * 60 * 1000))
DAYS_30=$((30 * 24 * 60 * 60 * 1000))

echo "waiting for broker at ${BOOTSTRAP} ..."
for attempt in $(seq 1 60); do
  if "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
    break
  fi
  if [[ "${attempt}" -eq 60 ]]; then
    echo "broker never became reachable at ${BOOTSTRAP}" >&2
    exit 1
  fi
  sleep 2
done

# create <topic> <partitions> <retention.ms> [extra --config pairs...]
create() {
  local topic="$1" partitions="$2" retention="$3"
  shift 3
  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention}" \
    --config min.insync.replicas=1 \
    --config compression.type=producer \
    "$@" >/dev/null
  printf '  %-34s p=%-3s retention=%s\n' "${topic}" "${partitions}" "$(( retention / 86400000 ))d"
}

echo "creating topics (prod partitions / 6, RF=1) ..."

#      topic                              local  prod  retention
create notification.requested                 2  "${DAYS_7}"   # 12 · keyed tenantId|idempotencyKey
create notification.scheduled                 1  "${DAYS_7}"   #  6 · keyed tenantId|requestId

# Dispatch: per channel AND per lane. A shared topic would let one 10M-record
# bulk campaign head-of-line block a password reset for ~37 seconds.
create notification.dispatch.push.tx          3  "${DAYS_3}"   # 18
create notification.dispatch.push.bulk        9  "${DAYS_3}"   # 54
create notification.dispatch.email.tx         2  "${DAYS_3}"   # 12
create notification.dispatch.email.bulk       6  "${DAYS_3}"   # 36
create notification.dispatch.sms.tx           2  "${DAYS_3}"   # 12 · minISR=3 in prod (OTPs)
create notification.dispatch.sms.bulk         1  "${DAYS_3}"   #  6

create notification.delivery                  4  "${DAYS_7}"   # 24

# notification.status is compact+delete. Compaction keeps the highest OFFSET,
# not the latest state -- the projector must still drop version <= current.
# Never rely on compaction for correctness (docs/KAFKA.md §5).
create notification.status                    8  "${DAYS_7}" \
  --config cleanup.policy=compact,delete \
  --config min.cleanable.dirty.ratio=0.5 \
  --config segment.ms=600000                                  # 48

# Five retry tiers, shared across channels. Safe to share because the retry
# consumer performs no external I/O -- it pauses and republishes -- so
# head-of-line blocking inside a tier is bounded by the tier delay itself.
create notification.retry.5s                  4  "${DAYS_2}"   # 24 · sized for a TOTAL provider outage
create notification.retry.30s                 2  "${DAYS_2}"   # 12
create notification.retry.2m                  1  "${DAYS_2}"   #  6
create notification.retry.10m                 1  "${DAYS_2}"   #  6
create notification.retry.1h                  1  "${DAYS_2}"   #  6

# 30 days: a DLQ record is evidence for an incident review, and incident
# reviews happen weeks late.
create notification.dlq                       1  "${DAYS_30}"  #  6

echo
echo "topics now on the broker:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list | sed 's/^/  /'
echo
echo "total local partitions: $(
  "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe 2>/dev/null \
    | grep -c $'\tPartition: ' || true
)  (production: 288)"
