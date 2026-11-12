"""LSH experiment: similar-condition retrieval over county-hour rows.

For every county-hour, find the 10 most similar strictly earlier county-hours by
cosine similarity. Ground truth is exact brute-force cosine top-10 over the same
earlier rows. The LSH method is random-hyperplane (SimHash) signatures with
banding: two rows are candidates if they share a bucket in any band.

Evaluation order follows replay time. All five county rows of an hour are queried
before any of them is inserted into the index, so candidates are strictly earlier.
Queries with fewer than K earlier rows are skipped.

Reproduce:  python experiments/lsh/lsh_experiment.py | tee experiments/lsh/run_output.log
"""
import csv
import os
import time
import tracemalloc

import numpy as np

from features import load_table, standardize

K = 10
ROWS_PER_HOUR = 5
SIGNATURE_BITS = 128
CONFIGS = [(32, 4), (16, 8), (8, 16)]  # (bands, rows per band); b * r = 128
SEEDS = [101, 102, 103, 104, 105]
OUT_DIR = os.environ.get("OUT_DIR", "experiments/lsh")


def unit_rows(x):
    norms = np.linalg.norm(x, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return x / norms


def build_index_kb(allkeys_list, b):
    """Memory of the bucket structure (dicts and lists) for the full index."""
    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]
    buckets = [dict() for _ in range(b)]
    for i, keys in enumerate(allkeys_list):
        for j in range(b):
            buckets[j].setdefault(keys[j], []).append(i)
    used = tracemalloc.get_traced_memory()[0] - base
    tracemalloc.stop()
    return used / 1024.0


def run_once(xn, b, r, seed):
    n, d = xn.shape
    rng = np.random.default_rng(seed)
    planes = rng.standard_normal((d, b * r))
    pow2 = (1 << np.arange(r)).astype(np.int64)
    all_bits = (xn @ planes > 0).astype(np.int64)
    allkeys = (all_bits.reshape(n, b, r) * pow2).sum(axis=2)
    allkeys_list = allkeys.tolist()
    index_kb = build_index_kb(allkeys_list, b)

    buckets = [dict() for _ in range(b)]
    recalls, fracs, t_exact, t_lsh = [], [], [], []
    for start in range(0, n, ROWS_PER_HOUR):
        if start >= K:  # eligible earlier rows = start
            for q in range(start, start + ROWS_PER_HOUR):
                qn = xn[q]
                t0 = time.perf_counter_ns()
                sims = xn[:start] @ qn
                exact = np.argpartition(-sims, K - 1)[:K]
                t1 = time.perf_counter_ns()
                bits = (qn @ planes > 0).astype(np.int64)
                keys = (bits.reshape(b, r) * pow2).sum(axis=1).tolist()
                cand = set()
                for j in range(b):
                    members = buckets[j].get(keys[j])
                    if members:
                        cand.update(members)
                cand_arr = np.fromiter(cand, dtype=np.int64, count=len(cand))
                if len(cand_arr) > K:
                    csims = xn[cand_arr] @ qn
                    top = cand_arr[np.argpartition(-csims, K - 1)[:K]]
                else:
                    top = cand_arr
                t2 = time.perf_counter_ns()
                recalls.append(len(set(exact.tolist()) & set(top.tolist())) / K)
                fracs.append(len(cand) / start)
                t_exact.append(t1 - t0)
                t_lsh.append(t2 - t1)
        for i in range(start, start + ROWS_PER_HOUR):
            keys_i = allkeys_list[i]
            for j in range(b):
                buckets[j].setdefault(keys_i[j], []).append(i)
    return {
        "recall": float(np.mean(recalls)),
        "frac": float(np.mean(fracs)),
        "exact_us": float(np.mean(t_exact)) / 1000.0,
        "lsh_us": float(np.mean(t_lsh)) / 1000.0,
        "index_kb": index_kb,
        "queries": len(recalls),
    }


def main():
    hours, counties, raw = load_table()
    x, _ = standardize(raw)
    n = x.shape[0]
    for i in range(n):
        assert hours[i] == hours[i - i % ROWS_PER_HOUR], "rows must come in blocks of 5 per hour"
    xn = unit_rows(x)
    exact_kb = xn.nbytes / 1024.0
    print("rows:", n, "features:", x.shape[1], "K:", K, "signature bits:", SIGNATURE_BITS)
    print("seeds:", SEEDS)
    print("exact baseline vector storage (KB): %.1f" % exact_kb)

    out = []
    for b, r in CONFIGS:
        runs = [run_once(xn, b, r, s) for s in SEEDS]

        def ms(key):
            a = np.array([run[key] for run in runs])
            return float(a.mean()), float(a.std(ddof=1))

        rec, frac = ms("recall"), ms("frac")
        ex, ls, mem = ms("exact_us"), ms("lsh_us"), ms("index_kb")
        print("%dx%d: queries/run %d | recall@10 %.4f +/- %.4f | candidate fraction %.4f +/- %.4f"
              " | exact %.1f us +/- %.1f | LSH %.1f us +/- %.1f | LSH index %.1f KB +/- %.1f"
              % (b, r, runs[0]["queries"], rec[0], rec[1], frac[0], frac[1],
                 ex[0], ex[1], ls[0], ls[1], mem[0], mem[1]))
        out.append({
            "bands": b, "rows_per_band": r, "signature_bits": b * r,
            "seeds": len(SEEDS), "queries_per_run": runs[0]["queries"],
            "recall_at_10_mean": round(rec[0], 6), "recall_at_10_std": round(rec[1], 6),
            "candidate_fraction_mean": round(frac[0], 6),
            "candidate_fraction_std": round(frac[1], 6),
            "exact_query_us_mean": round(ex[0], 3), "exact_query_us_std": round(ex[1], 3),
            "lsh_query_us_mean": round(ls[0], 3), "lsh_query_us_std": round(ls[1], 3),
            "lsh_index_kb_mean": round(mem[0], 3), "lsh_index_kb_std": round(mem[1], 3),
            "exact_vectors_kb": round(exact_kb, 3),
        })
    path = os.path.join(OUT_DIR, "results.csv")
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0].keys()), lineterminator="\n")
        w.writeheader()
        w.writerows(out)
    print("wrote", path)


if __name__ == "__main__":
    main()
