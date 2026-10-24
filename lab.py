"""Run from the project directory. See README.md for the complete workflow."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import csv
from datetime import datetime, timezone
import hashlib
from itertools import combinations
import json
import math
import multiprocessing
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from source_tools import is_variant, parse_skills

ROOT = Path(__file__).resolve().parent


def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def sha256(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def settings():
    load_dotenv(ROOT / ".env")
    return {
        "endpoint": os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000"),
        "access": os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
        "secret": os.getenv("S3_SECRET_KEY", "rustfsadmin"),
        "region": os.getenv("S3_REGION", "us-east-1"),
        "bucket": os.getenv("S3_BUCKET", "hh-lake"),
        "key": os.getenv("S3_KEY", "raw/it_vacancies.jsonl"),
        "workers": max(1, int(os.getenv("WORKERS", "2"))),
        "chunk_rows": max(1, int(os.getenv("CHUNK_ROWS", "500"))),
    }


def s3_client(cfg):
    return boto3.client("s3", endpoint_url=cfg["endpoint"],
        aws_access_key_id=cfg["access"], aws_secret_access_key=cfg["secret"],
        region_name=cfg["region"], config=Config(
            signature_version="s3v4", s3={"addressing_style": "path"},
            connect_timeout=5, read_timeout=60, retries={"max_attempts": 3}))


def source_info(cfg):
    head = s3_client(cfg).head_object(Bucket=cfg["bucket"], Key=cfg["key"])
    return {"endpoint": cfg["endpoint"], "bucket": cfg["bucket"], "key": cfg["key"],
        "etag": head["ETag"], "bytes": head["ContentLength"],
        "sha256": head.get("Metadata", {}).get("sha256")}


def prepare():
    source = ROOT / "data/hh_it_vacancies.csv"
    target = ROOT / "data/it_vacancies.jsonl"
    csv.field_size_limit(10_000_000)
    count = 0
    seen = set()
    with source.open(encoding="utf-8", newline="") as src, target.open("w", encoding="utf-8") as out:
        for row in csv.DictReader(src):
            if row["id"] in seen:
                raise ValueError(f"Duplicate vacancy ID: {row['id']}")
            seen.add(row["id"])
            skills = parse_skills(row["key_skills"])
            if len(skills) != int(row["count_key_skills"]):
                raise ValueError(f"Wrong skill count: {row['id']}")
            item = {k: row[k] for k in ("id", "name", "area_name", "published_at", "professional_roles_id", "experience")}
            item["key_skills"] = skills
            for name in ("salary_from", "salary_to"):
                item[name] = float(row[name]) if row[name] else None
            item["salary_currency"] = row["salary_currency"] or None
            item["salary_gross"] = {"True": True, "False": False, "": None}[row["salary_gross"]]
            out.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
            count += 1
    result = {"rows": count, "bytes": target.stat().st_size, "sha256": sha256(target),
        "source_csv_sha256": sha256(source), "note": "No variant filtering, no salary imputation, no skill extraction."}
    save_json(ROOT / "data/prepared.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def upload(cfg):
    client = s3_client(cfg)
    try:
        client.head_bucket(Bucket=cfg["bucket"])
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
        kwargs = {"Bucket": cfg["bucket"]}
        if cfg["region"] != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": cfg["region"]}
        client.create_bucket(**kwargs)
    path = ROOT / "data/it_vacancies.jsonl"
    client.upload_file(str(path), cfg["bucket"], cfg["key"], ExtraArgs={
        "ContentType": "application/x-ndjson", "Metadata": {"sha256": sha256(path)}})
    head = source_info(cfg)
    objects = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=cfg["bucket"]):
        objects.extend({"key": o["Key"], "bytes": o["Size"]} for o in page.get("Contents", []))
    proof = {"source": head, "objects": objects, "prepared": read_json(ROOT / "data/prepared.json"),
             "observed_at": datetime.now(timezone.utc).isoformat()}
    save_json(ROOT / "evidence/storage.json", proof)
    print(json.dumps(proof, ensure_ascii=False, indent=2))


def chunks_from_s3(cfg):
    """Stream newline-delimited records; bound parent input to chunk_rows."""
    body = s3_client(cfg).get_object(Bucket=cfg["bucket"], Key=cfg["key"])["Body"]
    try:
        chunk = []
        for line in body.iter_lines(chunk_size=64 * 1024):
            if not line:
                continue
            chunk.append(json.loads(line))
            if len(chunk) == cfg["chunk_rows"]:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
    finally:
        body.close()


def mapper(rows, combine, fail=False):
    """Map + optional local combiner. No reading of local dataset files."""
    counts = {k: Counter() if combine else [] for k in (1, 2, 3)}
    ids, histogram = [], Counter()
    for row in rows:
        if not is_variant(row):
            continue
        skills = sorted({s.strip().lower() for s in row["key_skills"] if s.strip()})
        ids.append(row["id"])
        histogram[len(skills)] += 1
        for k in (1, 2, 3):
            values = combinations(skills, k)
            if combine:
                counts[k].update(values)
            else:
                counts[k].extend(values)
    if fail:
        # Failure after mapping, before committing anything to the reducer.
        raise RuntimeError("Injected mapper failure before reducer commit")
    return counts, ids, histogram, len(rows)


def finish_summary(counts, ids, histogram, source_rows):
    histogram = {str(n): value for n, value in sorted(histogram.items())}
    expected = {str(k): sum(math.comb(int(n), k) * value for n, value in histogram.items() if int(n) >= k) for k in (1, 2, 3)}
    totals = {str(k): sum(counts[k].values()) for k in (1, 2, 3)}
    if totals != expected:
        raise AssertionError(f"Combination invariant failed: {totals} != {expected}")
    return {"source_rows": source_rows, "selected_rows": len(ids),
        "selected_ids_sha256": hashlib.sha256("\n".join(sorted(ids)).encode()).hexdigest(),
        "skill_count_histogram": histogram, "contributions": totals,
        "unique_combinations": {str(k): len(counts[k]) for k in (1, 2, 3)}}


def mapreduce(cfg, combine=True, fail_chunk=None):
    counts, ids, histogram = {k: Counter() for k in (1, 2, 3)}, [], Counter()
    pending, total_rows, retries = {}, 0, 0
    chunks = enumerate(chunks_from_s3(cfg))
    exhausted = False
    # spawn is intentional: reproducible multiprocessing on macOS and Linux.
    with ProcessPoolExecutor(max_workers=cfg["workers"], mp_context=multiprocessing.get_context("spawn")) as pool:
        while pending or not exhausted:
            while not exhausted and len(pending) < 2 * cfg["workers"]:
                try:
                    chunk_id, rows = next(chunks)
                except StopIteration:
                    exhausted = True
                    break
                future = pool.submit(mapper, rows, combine, chunk_id == fail_chunk)
                pending[future] = (chunk_id, rows, 0)
            if not pending:
                continue
            ready, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in ready:
                chunk_id, rows, attempt = pending.pop(future)
                try:
                    partial, selected, hist, nrows = future.result()
                except Exception:
                    if attempt >= 1:
                        raise
                    retries += 1
                    print(f"mapper chunk={chunk_id} failed; retry=1", flush=True)
                    pending[pool.submit(mapper, rows, combine, False)] = (chunk_id, rows, attempt + 1)
                    continue
                # Commit exactly once, only after a successful future.
                for k in (1, 2, 3):
                    counts[k].update(partial[k])
                ids.extend(selected)
                histogram.update(hist)
                total_rows += nrows
    summary = finish_summary(counts, ids, histogram, total_rows)
    summary["mapper_retries"] = retries
    return counts, summary


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def duck_connection(cfg, install=False):
    import duckdb
    con = duckdb.connect(config={"threads": cfg["workers"]})
    if install:
        con.execute("INSTALL httpfs FROM 'https://extensions.duckdb.org'")
    con.execute("LOAD httpfs")
    endpoint = urlsplit(cfg["endpoint"])
    if endpoint.scheme not in ("http", "https") or endpoint.path.strip("/"):
        raise ValueError("S3_ENDPOINT must be http(s)://host:port without a path")
    con.execute(f"""CREATE SECRET local_s3 (
       TYPE S3, PROVIDER CONFIG, KEY_ID {sql_literal(cfg['access'])},
       SECRET {sql_literal(cfg['secret'])}, REGION {sql_literal(cfg['region'])},
       ENDPOINT {sql_literal(endpoint.netloc)}, URL_STYLE 'path',
       USE_SSL {'true' if endpoint.scheme == 'https' else 'false'}
    )""")
    return con


def load_selected(con, uri):
    con.execute("""CREATE TEMP TABLE raw AS SELECT * FROM read_json(?,
      format='newline_delimited', columns={
        id:'VARCHAR', name:'VARCHAR', area_name:'VARCHAR', published_at:'VARCHAR',
        professional_roles_id:'VARCHAR', experience:'VARCHAR', key_skills:'VARCHAR[]',
        salary_from:'DOUBLE', salary_to:'DOUBLE', salary_currency:'VARCHAR', salary_gross:'BOOLEAN'
      })""", [uri])
    con.execute((ROOT / "sql/variant.sql").read_text(encoding="utf-8"))


def duck_counts(cfg):
    with duck_connection(cfg) as con:
        load_selected(con, f"s3://{cfg['bucket']}/{cfg['key']}")
        con.execute((ROOT / "sql/counts.sql").read_text(encoding="utf-8"))
        counts = {k: Counter({tuple(skills): n for skills, n in con.execute(f"SELECT skills,n FROM counts{k}").fetchall()}) for k in (1, 2, 3)}
        ids = [r[0] for r in con.execute("SELECT id FROM selected").fetchall()]
        hist = Counter(dict(con.execute("SELECT len(skills), count(*) FROM selected GROUP BY 1").fetchall()))
        n = con.execute("SELECT count(*) FROM raw").fetchone()[0]
    summary = finish_summary(counts, ids, hist, n)
    summary["mapper_retries"] = 0
    return counts, summary


def write_counts(out, counts):
    out.mkdir(parents=True, exist_ok=True)
    for k in (1, 2, 3):
        with (out / f"counts_k{k}.csv").open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["skills", "n"])
            for key, n in sorted(counts[k].items(), key=lambda x: (-x[1], x[0])):
                writer.writerow([json.dumps(key, ensure_ascii=False), n])


def run_method(cfg, method, out, fail_chunk=None):
    before = source_info(cfg)
    if method == "duckdb":
        counts, summary = duck_counts(cfg)
    else:
        counts, summary = mapreduce(cfg, combine=method != "mr-naive", fail_chunk=fail_chunk)
    if source_info(cfg) != before:
        raise RuntimeError("Source object changed during computation; discard the run")
    summary.update({"method": method, "source": before, "workers": cfg["workers"], "chunk_rows": cfg["chunk_rows"]})
    write_counts(out, counts)
    save_json(out / "summary.json", summary)
    client = s3_client(cfg)
    for file in sorted(out.iterdir()):
        if file.suffix in (".csv", ".json"):
            client.upload_file(str(file), cfg["bucket"], f"results/{out.name}/{file.name}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def verify_dirs(left, right):
    a, b = read_json(left / "summary.json"), read_json(right / "summary.json")
    for key in ("source", "source_rows", "selected_rows", "selected_ids_sha256", "skill_count_histogram", "contributions", "unique_combinations"):
        if a[key] != b[key]:
            raise AssertionError(f"Summary differs: {key}")
    for k in (1, 2, 3):
        # Files have canonical ordering. Full byte comparison covers every key/count.
        if (left / f"counts_k{k}.csv").read_bytes() != (right / f"counts_k{k}.csv").read_bytes():
            raise AssertionError(f"Full counts differ for k={k}: {left} vs {right}")
    return {"equal": True, "left": left.name, "right": right.name,
        "selected_rows": a["selected_rows"], "source": a["source"], "full_comparison_k": [1, 2, 3]}


def measured_child(args, log, sample_memory=True):
    import psutil
    log.parent.mkdir(parents=True, exist_ok=True)
    start, peak = time.perf_counter(), 0
    env = {**os.environ, "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1"}
    with log.open("w", encoding="utf-8") as stream:
        process = subprocess.Popen([sys.executable, str(ROOT / "lab.py"), *args], cwd=ROOT,
            stdout=stream, stderr=subprocess.STDOUT, env=env)
        while process.poll() is None:
            if not sample_memory:
                time.sleep(0.01)
                continue
            rss = 0
            try:
                parent = psutil.Process(process.pid)
                children = parent.children(recursive=True)
            except psutil.NoSuchProcess:
                # A child can finish between poll() and the process lookup.
                time.sleep(0.01)
                continue
            for child in [parent, *children]:
                try:
                    rss += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            peak = max(peak, rss)
            time.sleep(0.01)
        code = process.wait()
    duration = time.perf_counter() - start
    if code:
        raise RuntimeError(f"Command failed (exit={code}). See {log};\n{log.read_text(encoding='utf-8')[-2500:]}")
    if sample_memory and peak == 0:
        raise RuntimeError(f"Could not sample process memory. Do not use this run for profiling: {log}")
    return {"seconds": duration, "peak_rss_mib": peak / 2**20 if sample_memory else None, "exit_code": code}


def benchmark(cfg, repeats, qa=False):
    if repeats < 3:
        raise ValueError("At least three attempts per method are required")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    reference, records, checks = None, [], []
    source = source_info(cfg)
    # Round-robin ordering reduces order bias. No cache-flush privilege is assumed.
    for attempt in range(1, repeats + 1):
        for method in ("mr-naive", "mapreduce", "duckdb"):
            out = ROOT / "results" / f"{stamp}-{method}-{attempt}"
            metrics = measured_child(["_run", "--method", method, "--out", str(out)], out / "stdout.log", sample_memory=not qa)
            if read_json(out / "summary.json")["source"] != source:
                raise RuntimeError("Source changed between benchmark attempts")
            if reference is None:
                reference = out
            checks.append(verify_dirs(reference, out))
            records.append({"method": method, "attempt": attempt, "directory": out.name, **metrics})
            memory_label = f"{metrics['peak_rss_mib']:.1f} MiB" if metrics['peak_rss_mib'] is not None else "not measured (QA)"
            print(f"{method} attempt={attempt}: {metrics['seconds']:.3f}s; peak RSS={memory_label}; full equality OK", flush=True)
    aggregate = {}
    for method in ("mr-naive", "mapreduce", "duckdb"):
        group = [r for r in records if r["method"] == method]
        aggregate[method] = {"median_seconds": statistics.median(r["seconds"] for r in group),
            "median_peak_rss_mib": statistics.median(r["peak_rss_mib"] for r in group) if not qa else None,
            "max_peak_rss_mib": max(r["peak_rss_mib"] for r in group) if not qa else None}
    save_json(ROOT / "results/benchmark.json", {"qa": qa, "source": source, "records": records, "aggregate": aggregate,
        "reference": reference.name, "settings": {"workers": cfg["workers"], "chunk_rows": cfg["chunk_rows"]},
        "machine": {"platform": platform.platform(), "python": sys.version, "cpu_count": os.cpu_count()},
        "metric": "End-to-end subprocess wall time including imports, S3 HEAD/GET, map/reduce or SQL, sorting, local output, and S3 result upload. Peak = sampled sum of RSS for root and live descendants; 10ms sampling; shared pages may be counted twice. No cold-cache claim."})
    save_json(ROOT / "evidence/verification.json", checks)


def failure_test(cfg, qa=False):
    bench = read_json(ROOT / "results/benchmark.json")
    out = ROOT / "results" / f"failure-{time.time_ns()}"
    metrics = measured_child(["_run", "--method", "mapreduce", "--out", str(out), "--fail-chunk", "0"], out / "stdout.log", sample_memory=not qa)
    check = verify_dirs(ROOT / "results" / bench["reference"], out)
    retries = read_json(out / "summary.json")["mapper_retries"]
    if retries != 1:
        raise AssertionError(f"Expected exactly one mapper retry, got {retries}")
    save_json(ROOT / "evidence/mapper_failure.json", {**check, "retries": retries, "metrics": metrics,
        "scope": "Injected exception after mapping chunk 0, before reducer commit. Executor process death and network partition are not tested."})
    print("Mapper failure, retry and full equality: OK")


def salary_value(row):
    # Explicitly analyze only net RUR salaries. No guessed tax or FX conversion.
    if row["salary_currency"] != "RUR" or row["salary_gross"] is not False:
        return None
    values = [row[k] for k in ("salary_from", "salary_to") if row[k] is not None]
    if not values or any(not math.isfinite(x) or x <= 0 for x in values):
        return None
    if len(values) == 2 and row["salary_from"] > row["salary_to"]:
        return None
    return statistics.mean(values)


def combo_stats(rows):
    counts, salaries = Counter(), defaultdict(list)
    for row in rows:
        salary = salary_value(row)
        for k in (1, 2, 3):
            for key in combinations(row["skills"], k):
                counts[key] += 1
                if salary is not None:
                    salaries[key].append(salary)
    return counts, salaries


def table_csv(path, rows, fields):
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v for k, v in row.items()})


def analyze(cfg):
    before = source_info(cfg)
    bench = read_json(ROOT / "results/benchmark.json")
    if before != bench["source"]:
        raise RuntimeError("Source differs from the verified benchmark")
    with duck_connection(cfg) as con:
        load_selected(con, f"s3://{cfg['bucket']}/{cfg['key']}")
        cur = con.execute("SELECT * FROM selected")
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, values)) for values in cur.fetchall()]
    if source_info(cfg) != before:
        raise RuntimeError("Source changed during analysis")
    counts, salaries = combo_stats(rows)
    threshold = max(2, int(os.getenv("MIN_SALARY_N", "5")))
    effects = []
    for skills, frequency in counts.items():
        if len(skills) == 1 or len(salaries[skills]) < threshold:
            continue
        # Most frequent constituent of size k-1, determined from ALL selected vacancies.
        base = min(combinations(skills, len(skills) - 1), key=lambda c: (-counts[c], c))
        single = min(((s,) for s in skills), key=lambda c: (-counts[c], c))
        if len(salaries[base]) < threshold:
            continue
        median, base_median = statistics.median(salaries[skills]), statistics.median(salaries[base])
        effects.append({"skills": list(skills), "k": len(skills), "frequency": frequency,
            "salary_n": len(salaries[skills]), "median_net_rur": median,
            "base_skills": list(base), "base_frequency": counts[base], "base_salary_n": len(salaries[base]),
            "base_median_net_rur": base_median, "delta_rur": median - base_median,
            "delta_percent": (median / base_median - 1) * 100,
            "most_frequent_single": list(single), "single_frequency": counts[single],
            "single_salary_n": len(salaries[single]), "single_median_net_rur": statistics.median(salaries[single])})
    effects.sort(key=lambda r: (-r["delta_rur"], r["skills"]))
    # Describe observed composition differences without asserting causality.
    eligible = [r for r in rows if salary_value(r) is not None]
    compositions = {}
    def composition(skills):
        key = tuple(skills)
        if key not in compositions:
            sub = [r for r in eligible if set(key).issubset(r["skills"])]
            compositions[key] = {
                "experience": dict(Counter(r["experience"] for r in sub)),
                "region": dict(Counter(r["area_name"] for r in sub)),
            }
        return compositions[key]
    for effect in effects:
        effect["composition"] = composition(effect["skills"])
        effect["base_composition"] = composition(effect["base_skills"])
    regions, region_skills = [], []
    chosen = os.getenv("REGIONS", "Москва|Санкт-Петербург|Новосибирск").split("|")
    if len(set(chosen)) != 3:
        raise ValueError("REGIONS must contain three different cities separated by |")
    for region in chosen:
        sub = [row for row in rows if row["area_name"] == region]
        if not sub:
            raise ValueError(f"No variant vacancies for region: {region}")
        paid = [salary_value(row) for row in sub if salary_value(row) is not None]
        regions.append({"region": region, "vacancies": len(sub), "with_skills": sum(bool(r["skills"]) for r in sub),
            "salary_n": len(paid), "median_net_rur": statistics.median(paid) if paid else None})
        local_counts, local_salaries = combo_stats(sub)
        for k in (1, 2, 3):
            top = sorted(((key, n) for key, n in local_counts.items() if len(key) == k), key=lambda x: (-x[1], x[0]))[:5]
            for key, n in top:
                values = local_salaries[key]
                region_skills.append({"region": region, "k": k, "skills": list(key), "frequency": n,
                    "share_percent": n / len(sub) * 100, "salary_n": len(values),
                    "median_net_rur": statistics.median(values) if values else None})
    out = ROOT / "results"
    save_json(out / "analysis.json", {"source": before, "salary_policy": "Only RUR and salary_gross=false; midpoint of two valid positive bounds, or sole available bound. No imputation or FX/tax conversion.",
        "min_salary_n": threshold, "effects": effects, "regions": regions, "region_skills": region_skills,
        "interpretation": "Inclusive base: vacancies with a base skill/pair may also contain the added skill. Associations are not causal effects; experience, employer and region can confound medians."})
    if effects:
        table_csv(out / "salary_effects.csv", effects, list(effects[0]))
    table_csv(out / "regions_summary.csv", regions, list(regions[0]))
    table_csv(out / "regions_skills.csv", region_skills, ["region", "k", "skills", "frequency", "share_percent", "salary_n", "median_net_rur"])
    print(json.dumps({"regions": regions, "eligible_salary_combinations": len(effects)}, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "upload", "setup-duckdb", "benchmark", "failure-test", "analyze", "_run"])
    parser.add_argument("--method", choices=["mr-naive", "mapreduce", "duckdb"], default="mapreduce")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--fail-chunk", type=int)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    cfg = settings()
    if args.command == "prepare":
        prepare()
    elif args.command == "upload":
        upload(cfg)
    elif args.command == "setup-duckdb":
        duck_connection(cfg, install=True).close()
        print("DuckDB httpfs: installed and loaded")
    elif args.command == "benchmark":
        benchmark(cfg, args.repeats)
    elif args.command == "failure-test":
        failure_test(cfg)
    elif args.command == "analyze":
        analyze(cfg)
    else:
        if args.out is None:
            parser.error("_run requires --out")
        run_method(cfg, args.method, args.out, args.fail_chunk)


if __name__ == "__main__":
    main()
