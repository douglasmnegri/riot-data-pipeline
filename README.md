# Riot Data Pipeline

## Overview

This pipeline extracts **Ranked entries** and **Champion Mastery data** from the Riot API using **Apache Airflow**. It is designed to be:

* Config-driven
* Rate-limit aware
* Idempotent and retry-safe
* Easy to extend with new ranks / tiers

The pipeline runs as a **single DAG** to guarantee correct ordering and avoid partial or stale runs.

---

## High-level Flow

1. **Extract Rank Entries**

   * Calls the Riot Ranked API (paginated)
   * Stores full rank entries on disk
   * Extracts and deduplicates all `puuid`s
   * Writes a PUUID JSON file

2. **Extract Champion Mastery**

   * Reads the PUUID file produced in step 1
   * Iterates over players
   * Calls the Champion Mastery API
   * Writes one JSON file per player
   * Respects Riot API rate limits

```
extract_rank_entries  →  extract_champion_mastery_entries
```

---

## Directory Structure

```
data/raw/
├── rank/                 # Raw rank entries
│   └── <queue>_<tier>_<division>_<ts>.json
├── puuids/               # Deduplicated PUUID lists
│   └── <queue>_<tier>_<division>_<ts>.json
└── champion_mastery/     # One file per player
    └── <puuid>.json
```

---

## Configuration-driven Jobs

Rank extraction jobs are defined via JSON config:

```
airflow/dags/rank_entries/configs/rank_entries_jobs.json
```

Each job defines:

* queue
* tier
* division
* task_suffix

Airflow dynamically creates one **rank task + mastery task pair per job**.

---

## Idempotency & Reliability

* Champion mastery files are **written once per PUUID**
* On retries, already-processed players are skipped
* Failures do not re-trigger completed API calls
* Intermediate state is persisted to disk (not XCom)

---

## Rate Limiting

Champion mastery extraction enforces a fixed delay between requests:

* `time.sleep(2)` per request
* Keeps the pipeline within Riot API limits
* Safe for long-running executions

---

## Why a Single DAG

* Prevents race conditions between DAGs
* Guarantees rank data exists before mastery extraction
* Simplifies retries and observability
* Avoids stale or mismatched PUUID files

---

## Summary

This pipeline prioritizes **correctness, simplicity, and safety** over premature parallelism. It is suitable for production use and can scale incrementally as data volume or API constraints evolve.
