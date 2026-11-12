import csv, glob, hashlib, json, math, random, tracemalloc
from statistics import mean

INPUT = "data/replay_input/*.jsonl"
DUP_FRACTION = 0.10
TARGETS = [0.05, 0.01, 0.001]
TRIALS = 5
OUT_DIR = "experiments/bloom_filter"


class Bloom:
    def __init__(self, n, p, salt=""):
        self.m = math.ceil(-n * math.log(p) / (math.log(2) ** 2))
        self.k = max(1, round(self.m / n * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)
        self.salt = salt

    def _idx(self, item):
        h = hashlib.sha256((self.salt + item).encode()).digest()
        a = int.from_bytes(h[:8], "big")
        b = int.from_bytes(h[8:16], "big") | 1
        return [(a + i * b) % self.m for i in range(self.k)]

    def add(self, item):
        for i in self._idx(item):
            self.bits[i >> 3] |= 1 << (i & 7)

    def __contains__(self, item):
        return all(self.bits[i >> 3] >> (i & 7) & 1 for i in self._idx(item))


def load_ids():
    ids = []
    for f in sorted(glob.glob(INPUT)):
        with open(f) as fh:
            for line in fh:
                ids.append(json.loads(line)["event_id"])
    return ids


def make_stream(ids, seed):
    rng = random.Random(seed)
    n = len(ids)
    dup_idx = rng.sample(range(n), int(n * DUP_FRACTION))
    items = [(float(i), 0, ids[i]) for i in range(n)]
    for i in dup_idx:
        items.append((i + 1 + rng.random() * (n - i), 1, ids[i]))
    items.sort(key=lambda t: (t[0], t[1]))
    return [t[2] for t in items], len(dup_idx)


def run_trial(stream, n, p, salt):
    bloom = Bloom(n, p, salt)
    exact = set()
    detected = missed = false_pos = 0
    for x in stream:
        seen = x in bloom
        if x in exact:
            if seen:
                detected += 1
            else:
                missed += 1
        elif seen:
            false_pos += 1
        bloom.add(x)
        exact.add(x)
    return bloom, detected, missed, false_pos


def exact_set_bytes(ids):
    tracemalloc.start()
    s = set(x.encode().decode() for x in ids)
    current, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return current


def main():
    ids = load_ids()
    n = len(ids)
    assert n == len(set(ids)), "event ids are not unique"
    exact_bytes = exact_set_bytes(ids)
    rows = []
    for p in TARGETS:
        results = []
        for t in range(TRIALS):
            stream, n_dup = make_stream(ids, seed=t)
            bloom, det, miss, fp = run_trial(stream, n, p, salt="t%d:" % t)
            results.append((det, miss, fp))
        theory = (1 - math.exp(-bloom.k * n / bloom.m)) ** bloom.k
        mean_fp = mean(r[2] for r in results)
        rows.append({
            "target_fp_rate": p,
            "distinct_events": n,
            "injected_duplicates": n_dup,
            "trials": TRIALS,
            "filter_bits": bloom.m,
            "hash_functions": bloom.k,
            "bloom_bytes": len(bloom.bits),
            "exact_set_bytes": exact_bytes,
            "duplicates_detected_mean": mean(r[0] for r in results),
            "duplicates_missed_total": sum(r[1] for r in results),
            "false_positives_mean": round(mean_fp, 1),
            "measured_fp_rate": round(mean_fp / n, 5),
            "theoretical_fp_rate_full": round(theory, 5),
        })
    with open(OUT_DIR + "/results.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    for r in rows:
        print(r)


main()
