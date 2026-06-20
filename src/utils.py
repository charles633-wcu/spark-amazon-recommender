"""Shared helpers: SparkSession builder and project paths."""
from pathlib import Path
from pyspark.sql import SparkSession

# Project root = parent of this src/ folder.
ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = ROOT / "data" / "raw"
DATA_INTERMEDIATE = ROOT / "data" / "intermediate"
OUTPUT_STATS = ROOT / "output" / "stats"
OUTPUT_FIGURES = ROOT / "output" / "figures"


def get_spark(app_name: str = "csc467") -> SparkSession:
    """Return a local SparkSession configured for this project."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")  # small for a laptop
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
