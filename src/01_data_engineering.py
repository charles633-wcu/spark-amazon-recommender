"""Task 1 - Data Engineering.

Pipeline:
    1. Read the raw Amazon Reviews JSONL.
    2. Print descriptive statistics (counts, sparsity, rating distribution).
    3. Clean and reduce to a compact (user, item, rating) table.
    4. Validate the cleaned data against explicit data-quality rules.
    5. Write the result as Parquet for Task 2.

The validation step (4) is how we *know* the cleaned data is correct without
reading millions of rows by eye: we assert invariants (rating range, no nulls,
no duplicates, non-empty) that fail loudly if violated.

Run:
    & $env:PYSPARK_PYTHON src/01_data_engineering.py
"""
from pyspark.sql import DataFrame, functions as F
from utils import get_spark, DATA_RAW, DATA_INTERMEDIATE

# --- Config -----------------------------------------------------------------
CATEGORY = "Video_Games"                      # must be a >1M-review category
RAW_FILE = DATA_RAW / f"{CATEGORY}.jsonl.gz"  # Spark reads .gz directly
MIN_RATINGS = 5                               # drop users/items with fewer ratings


def describe(reviews: DataFrame) -> int:
    """Print descriptive statistics about the raw reviews.

    Computes and prints the review/user/item counts, the matrix sparsity, and
    the rating distribution. These are the numbers reported in the Data
    Engineering section of the report.

    Args:
        reviews: The raw reviews DataFrame (one row per review).

    Returns:
        The total number of raw reviews, used later to report the reduction %.
    """
    n_reviews = reviews.count()
    n_users = reviews.select("user_id").distinct().count()
    n_items = reviews.select("parent_asin").distinct().count()
    print(f"reviews={n_reviews:,}  users={n_users:,}  items={n_items:,}")
    print(f"sparsity={n_reviews / (n_users * n_items):.8f}")
    print("rating distribution:")
    reviews.groupBy("rating").count().orderBy("rating").show()
    return n_reviews


def clean_and_reduce(reviews: DataFrame) -> DataFrame:
    """Clean and shrink the raw reviews into a compact ratings table.

    Steps, each chosen to serve the downstream ALS recommender:
        - Keep only the (user, item, rating, timestamp) columns; dropping the
          review text is the single largest size reduction.
        - Drop rows with nulls and collapse to one rating per (user, item).
        - Filter out users and items with fewer than ``MIN_RATINGS`` ratings.
          ALS cannot learn from a user or item seen only once (cold start), and
          this also removes the long tail of one-time reviewers.

    Note:
        The ``MIN_RATINGS`` filter is applied in a single pass (not iterated to
        convergence), so after the joins a few users/items may fall slightly
        below the threshold. This is acceptable for the project; see validate().

    Args:
        reviews: The raw reviews DataFrame.

    Returns:
        The cleaned, reduced ratings DataFrame.
    """
    ratings = (
        reviews
        .select("user_id", "parent_asin", "rating", "timestamp")  # drop text -> shrink
        .dropna()
        .dropDuplicates(["user_id", "parent_asin"])               # one rating per pair
    )

    u_keep = (ratings.groupBy("user_id").count()
              .filter(F.col("count") >= MIN_RATINGS).select("user_id"))
    i_keep = (ratings.groupBy("parent_asin").count()
              .filter(F.col("count") >= MIN_RATINGS).select("parent_asin"))
    return ratings.join(u_keep, "user_id").join(i_keep, "parent_asin")


def validate(ratings: DataFrame) -> None:
    """Assert data-quality invariants on the cleaned ratings, printing a report.

    This is how a data engineer verifies correctness at scale: instead of
    reading rows, we run rules over the whole DataFrame that each collapse to a
    single pass/fail. Any violation raises AssertionError so the pipeline fails
    loudly rather than writing bad data.

    Hard invariants (must hold, else AssertionError):
        - every rating is within [1, 5]
        - no null values in any column
        - no duplicate (user_id, parent_asin) pairs
        - the table is non-empty

    Informational (printed, not asserted):
        - minimum ratings-per-user / per-item, which may dip below MIN_RATINGS
          because the filter in clean_and_reduce() is single-pass.

    Args:
        ratings: The cleaned ratings DataFrame to validate.

    Raises:
        AssertionError: If any hard invariant is violated.
    """
    print("=== validation ===")
    failures = []

    def check(name: str, ok: bool) -> None:
        """Record and print a single PASS/FAIL line for a named check."""
        print(f"[{'PASS' if ok else 'FAIL'}] {name}")
        if not ok:
            failures.append(name)

    n = ratings.count()
    bad_range = ratings.filter((F.col("rating") < 1) | (F.col("rating") > 5)).count()
    nulls = ratings.filter(
        F.col("user_id").isNull() | F.col("parent_asin").isNull()
        | F.col("rating").isNull() | F.col("timestamp").isNull()
    ).count()
    dupes = n - ratings.dropDuplicates(["user_id", "parent_asin"]).count()

    check("ratings within [1, 5]", bad_range == 0)
    check("no null values", nulls == 0)
    check("no duplicate (user, item) pairs", dupes == 0)
    check(f"non-empty ({n:,} rows)", n > 0)

    min_per_user = ratings.groupBy("user_id").count().agg(F.min("count")).first()[0]
    min_per_item = ratings.groupBy("parent_asin").count().agg(F.min("count")).first()[0]
    print(f"[info] min ratings/user={min_per_user}, min ratings/item={min_per_item} "
          f"(single-pass >={MIN_RATINGS} filter; not iterated)")

    if failures:
        raise AssertionError(f"validation failed: {failures}")
    print("all hard checks passed.")


def main():
    """Run the full Task 1 pipeline: read, describe, clean, validate, write."""
    spark = get_spark("amazon-data-engineering")

    reviews = spark.read.json(str(RAW_FILE))

    n_reviews = describe(reviews)

    ratings = clean_and_reduce(reviews)
    kept = ratings.count()
    print(f"after cleaning/reduction: {kept:,} ratings ({kept / n_reviews:.1%} of raw)")

    validate(ratings)

    ratings.write.mode("overwrite").parquet(str(DATA_INTERMEDIATE / "ratings"))
    print(f"wrote {DATA_INTERMEDIATE / 'ratings'}")

    spark.stop()


if __name__ == "__main__":
    main()
