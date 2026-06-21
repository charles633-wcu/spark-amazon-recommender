# CSC 467 Big Data Project — Amazon Reviews Recommender (Spark + ALS)

A Spark-based recommendation system built on the **Amazon Reviews 2023** dataset.
Given a user's past product ratings, it recommends products using **ALS
collaborative filtering** (latent-factor matrix factorization).

- **Query:** *Given a user's rating history, what products should we recommend?*
- **Technique (Task 2):** Recommendation via ALS (a CSC 467 course topic).
- **Dataset:** Amazon Reviews 2023, `Video_Games` category (~4.6M reviews → clears
  the >1M-entities requirement).

---

## Dataset & acquisition

Source: **Amazon Reviews 2023**, McAuley Lab, UC San Diego.
- Project site: https://amazon-reviews-2023.github.io/
- HuggingFace mirror: https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023

Download the `Video_Games` category into `data/raw/` (reviews are required;
metadata is optional, used to show product titles):

```bash
# reviews (~776 MB gzip, ~4.6M reviews)
curl -L -o data/raw/Video_Games.jsonl.gz \
  https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Video_Games.jsonl.gz

# metadata (~98 MB, product titles/prices) -- optional
curl -L -o data/raw/meta_Video_Games.jsonl.gz \
  https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Video_Games.jsonl.gz
```

The raw data is **not** included in the submission (too large, regenerable) — it is
git-ignored. Re-download with the commands above.

---

## Project structure

```
csc467_project/
├── README.md                  # this file
├── requirements.txt           # Python deps (pyspark==3.5.1)
├── Dockerfile                 # reproducible Linux Spark environment
├── docker-compose.yml         # convenience runner (build + volumes + memory)
├── setup-pyspark.ps1          # local Windows env setup (alternative to Docker)
├── verify_spark.py            # quick "does Spark start?" check
├── src/
│   ├── utils.py               # SparkSession builder + project paths
│   ├── 01_data_engineering.py # Task 1: clean, reduce, validate -> Parquet
│   └── 02_analytics.py        # Task 2: ALS train, evaluate (RMSE), recommend
├── data/
│   ├── raw/                   # downloaded dataset (git-ignored)
│   └── intermediate/ratings/  # cleaned Parquet from Task 1 (git-ignored)
├── output/                    # stats + figures for the report
├── docs/                      # concept explainers (e.g. iterative-filtering.md)
└── report/                    # the 4-page IEEE report (PDF)
```

---

## Dependencies

- **Docker** (recommended path) — that's it.
- **OR** for the local path: Python 3.11, a JDK 17, and `pyspark==3.5.1`
  (see `requirements.txt`).

---

## How to run

### Option A — Docker (recommended, fully reproducible)

Requires Docker Desktop. Runs identically on Windows/Mac/Linux.

```bash
docker compose build                                       # build once
docker compose run --rm spark python verify_spark.py       # sanity check
docker compose run --rm spark python src/01_data_engineering.py   # Task 1
docker compose run --rm spark python src/02_analytics.py          # Task 2
```

Your `data/` and `output/` folders are mounted into the container, so the cleaned
Parquet and results appear on your host machine.

### Option B — Local Windows (PowerShell)

Uses a conda env (`C:\conda-envs\pyspark-3.5.1`) that bundles Python 3.11 + Java 17
+ PySpark, plus the bundled `hadoop/winutils.exe` in this repo. In each new shell:

```powershell
. .\setup-pyspark.ps1                              # configure env for the session
& $env:PYSPARK_PYTHON verify_spark.py              # sanity check
& $env:PYSPARK_PYTHON src/01_data_engineering.py   # Task 1
& $env:PYSPARK_PYTHON src/02_analytics.py          # Task 2
```

> Note: on Windows a red `ShutdownHookManager ... NoSuchFileException` trace may
> appear *after* a job finishes — it is a harmless Spark-on-Windows cleanup warning
> and does not affect results.

---

## Why this project is containerized (Docker notes)

This project ships with Docker for two reasons: **reproducibility** and **learning**.
Here is what each piece does and why it matters.

### The problem Docker solves here
Running Spark on **Windows** is fiddly: Spark expects a Unix-like environment, so it
needs `winutils.exe` + `hadoop.dll` (a Hadoop shim) and a carefully matched
Python/Java setup. That works, but it is machine-specific and fragile — a grader on a
different OS may not be able to reproduce it.

A **Linux container removes all of that.** Spark runs natively on Linux, so there is
**no winutils, no hadoop.dll, no conda juggling** — just a JDK and `pip install
pyspark`. The container guarantees the exact same Python, Java, and Spark versions
on every machine.

### `Dockerfile` — the recipe for the environment
- Starts from `python:3.11-slim` (matches our Python version).
- Installs **JDK 17** (Spark is a JVM engine and needs Java; Spark 3.5.1 supports 17).
- `pip install`s the pinned `pyspark==3.5.1` from `requirements.txt`.
- Copies the `src/` code in. **Data is *not* baked in** — it is mounted at runtime,
  so the image stays small and the data stays on your machine.

### `docker-compose.yml` — the convenience runner
This project is a **one-shot batch job**, not a long-running web service, so we do
not really need multi-container orchestration. Compose is used here simply to bundle
three things into one short command:
1. **build** the image,
2. **mount** `./data` and `./output` into the container (so results land on the host),
3. set a **JVM memory limit** for the Spark driver.

We then run each script on demand with `docker compose run --rm spark python <script>`.
(`--rm` deletes the throwaway container after each run.)

### `.dockerignore`
Keeps the build fast by excluding the large/irrelevant folders (`data/`,
`school_notes/`, `hadoop/`, etc.) from the build context — otherwise Docker would try
to copy ~800 MB of data on every build.

### What I learned / the takeaway
The key insight is that **the container is a portable, disposable computer** defined
entirely in text (`Dockerfile`). Anyone can reproduce the exact environment with one
command, and platform-specific hacks (the Windows winutils mess) simply disappear
because the container is always Linux. Code lives in the image; data lives in mounted
volumes; the two are kept separate on purpose.

---

## Pipeline summary

1. **Task 1 — Data Engineering** (`01_data_engineering.py`): reads raw reviews, prints
   descriptive stats (counts, sparsity, rating distribution), cleans + reduces to a
   compact `(user, item, rating)` table, **validates** it against data-quality rules,
   and writes Parquet to `data/intermediate/ratings/`.
2. **Task 2 — Data Analytics** (`02_analytics.py`): indexes IDs, trains ALS, evaluates
   with **RMSE** on a held-out split, and generates top-N recommendations per user.

See `docs/` for concept explainers and `report/` for the technical report.
