#!/bin/bash
set -u
cd "$(dirname "$0")/../.."
OUT=${OUT:-evidence/kafka/runs}
REPS=${REPS:-3}
SPEEDS=${SPEEDS:-"fast medium slow"}
DELAYS=${DELAYS:-"none moderate severe"}
KAFKA="docker exec smokewatch-kafka /opt/kafka/bin"
TOPICS="firms.events epa.pm25.events noaa.weather.events"
mkdir -p "$OUT"
SUMMARY="$OUT/summary.csv"
[ -f "$SUMMARY" ] || echo "repetition,speed,delay,scenario,start_utc,seconds,producer_exit,firms,epa_pm25,noaa_weather,total" > "$SUMMARY"

count_topic() {
  $KAFKA/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'
}

reset_topics() {
  for t in $TOPICS; do
    $KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1
  done
  sleep 8
  python src/kafka/create_topics.py >/dev/null 2>&1
  sleep 2
}

for rep in $(seq 1 "$REPS"); do
  for delay in $DELAYS; do
    for speed in $SPEEDS; do
      scenario="${speed}__${delay}"
      log="$OUT/rep${rep}_${scenario}.log"
      reset_topics
      pre=0
      for t in $TOPICS; do pre=$((pre + $(count_topic "$t"))); done
      if [ "$pre" -ne 0 ]; then echo "Topics not empty after reset ($pre). Stopping."; exit 1; fi
      start=$(date -u +%Y-%m-%dT%H:%M:%SZ); t0=$(date +%s)
      python src/kafka/replay_producer.py --input-dir data/replay_input --speed "$speed" --delay "$delay" --scenario "$scenario" > "$log" 2>&1
      status=$?
      t1=$(date +%s)
      f=$(count_topic firms.events); e=$(count_topic epa.pm25.events); n=$(count_topic noaa.weather.events)
      echo "$rep,$speed,$delay,$scenario,$start,$((t1-t0)),$status,$f,$e,$n,$((f+e+n))" | tee -a "$SUMMARY"
    done
  done
done
