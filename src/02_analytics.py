"""Task 2 - Data Analytics: ALS recommender with tuning + human-readable titles.

Pipeline:
    1. Load the cleaned ratings Parquet from Task 1.
    2. Index the string user/item ids to integers (ALS requires numeric ids).
    3. Tune ALS over a small (rank, regParam) grid, selecting the lowest-RMSE model.
    4. Save the tuning results table to output/stats/ for the report.
    5. Generate top-N recommendations for a sample of users and attach product
       titles from the metadata file, saving a human-readable CSV.

Run:
    & $env:PYSPARK_PYTHON src/02_analytics.py
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from utils import get_spark, DATA_INTERMEDIATE, DATA_RAW, OUTPUT_STATS

# --- Config -----------------------------------------------------------------
TOP_N = 10
RANKS = [10, 50]            # number of latent factors to try
REG_PARAMS = [0.05, 0.1]    # regularization strengths to try
MAX_ITER = 10
SAMPLE_USERS = 10           # how many users to produce readable recommendations for
META_FILE = DATA_RAW / "meta_Video_Games.jsonl.gz"


def tune(train: DataFrame, test: DataFrame):
    """Train ALS across a small hyperparameter grid and keep the best model.

    For each (rank, regParam) combination, fits ALS on the training split and
    measures RMSE on the test split, tracking the lowest-RMSE configuration.

    Args:
        train: Training ratings (with integer user_id_idx / parent_asin_idx).
        test: Held-out test ratings.

    Returns:
        A tuple ``(results, best)`` where ``results`` is a list of
        ``(rank, regParam, rmse)`` rows and ``best`` is a dict with keys
        ``rank``, ``reg``, ``rmse``, and the trained ``model``.
    """
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    results = []
    best = None
    for rank in RANKS:
        for reg in REG_PARAMS:
            als = ALS(
                userCol="user_id_idx", itemCol="parent_asin_idx", ratingCol="rating",
                rank=rank, maxIter=MAX_ITER, regParam=reg,
                coldStartStrategy="drop", nonnegative=True,
            )
            model = als.fit(train)
            rmse = evaluator.evaluate(model.transform(test))
            print(f"  rank={rank:>3}  regParam={reg:<5}  RMSE={rmse:.4f}")
            results.append((rank, float(reg), round(float(rmse), 4)))
            if best is None or rmse < best["rmse"]:
                best = {"rank": rank, "reg": reg, "rmse": rmse, "model": model}
    return results, best


def add_titles(spark, recs: DataFrame, df: DataFrame) -> DataFrame:
    """Convert index-based recommendations into rows with real product titles.

    ALS recommends integer item indices; this reverses the indexing (idx -> asin
    via the already-indexed ``df``) and joins the metadata file to attach each
    product's title, also restoring the original user_id.

    Args:
        spark: The active SparkSession (used to read the metadata file).
        recs: Output of recommendForUserSubset (user_id_idx + recommendations array).
        df: The indexed ratings, carrying both the string ids and their indices.

    Returns:
        A DataFrame of (user_id, rank, title, score), ordered for readability.
    """
    item_map = df.select(F.col("parent_asin_idx").cast("int").alias("parent_asin_idx"),
                         "parent_asin").distinct()
    user_map = df.select(F.col("user_id_idx").cast("int").alias("user_id_idx"),
                         "user_id").distinct()
    # Explicit schema: the metadata file has a messy nested `details` struct with
    # case-colliding keys that break full schema inference. Reading only the two
    # fields we need sidesteps that entirely.
    meta_schema = StructType([
        StructField("parent_asin", StringType(), True),
        StructField("title", StringType(), True),
    ])
    meta = spark.read.schema(meta_schema).json(str(META_FILE))

    exploded = (recs
        .select("user_id_idx", F.posexplode("recommendations").alias("pos", "rec"))
        .select("user_id_idx",
                (F.col("pos") + 1).alias("rank"),
                F.col("rec.parent_asin_idx").alias("parent_asin_idx"),
                F.round(F.col("rec.rating"), 3).alias("score")))

    return (exploded
        .join(item_map, "parent_asin_idx")
        .join(meta, "parent_asin", "left")
        .join(user_map, "user_id_idx")
        .select("user_id", "rank", "title", "score")
        .orderBy("user_id", "rank"))


def main():
    """Run Task 2: index, split, tune ALS, save tuning table + titled recs."""
    spark = get_spark("amazon-als")

    df = spark.read.parquet(str(DATA_INTERMEDIATE / "ratings"))
    df = (StringIndexer(inputCol="user_id", outputCol="user_id_idx",
                        handleInvalid="skip").fit(df).transform(df))
    df = (StringIndexer(inputCol="parent_asin", outputCol="parent_asin_idx",
                        handleInvalid="skip").fit(df).transform(df))
    df.cache()

    train, test = df.randomSplit([0.8, 0.2], seed=42)
    train.cache()
    test.cache()

    print("=== hyperparameter tuning ===")
    results, best = tune(train, test)
    print(f"BEST: rank={best['rank']}  regParam={best['reg']}  RMSE={best['rmse']:.4f}")

    OUTPUT_STATS.mkdir(parents=True, exist_ok=True)
    (spark.createDataFrame(results, ["rank", "regParam", "rmse"])
        .coalesce(1).write.mode("overwrite").option("header", True)
        .csv(str(OUTPUT_STATS / "tuning_results")))

    print(f"=== top-{TOP_N} recommendations with titles (sample of {SAMPLE_USERS} users) ===")
    some_users = df.select("user_id_idx").distinct().limit(SAMPLE_USERS)
    recs = best["model"].recommendForUserSubset(some_users, TOP_N)
    named = add_titles(spark, recs, df)
    named.show(SAMPLE_USERS * TOP_N, truncate=45)
    (named.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(str(OUTPUT_STATS / "recommendations_sample")))
    print(f"wrote {OUTPUT_STATS / 'tuning_results'} and {OUTPUT_STATS / 'recommendations_sample'}")

    spark.stop()


if __name__ == "__main__":
    main()
