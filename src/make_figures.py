"""Generate the report figures (PNG) into output/figures/.

Produces three charts from the project's real results:
    1. fig_rating_distribution.png - the 1-5 star rating counts (Task 1).
    2. fig_data_reduction.png      - raw reviews vs. cleaned ratings (Task 1).
    3. fig_rmse_tuning.png         - RMSE for each ALS hyperparameter combo (Task 2).

Run (no Spark needed, just matplotlib):
    & $env:PYSPARK_PYTHON src/make_figures.py
"""
import matplotlib
matplotlib.use("Agg")  # no display needed; write straight to file
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from utils import OUTPUT_FIGURES

OUTPUT_FIGURES.mkdir(parents=True, exist_ok=True)

# Format large y-axis counts as "1.0M" / "0.5M" instead of matplotlib's "1e6" offset.
_MILLIONS = FuncFormatter(lambda v, _: f"{v / 1e6:.1f}M" if v else "0")


def rating_distribution():
    """Bar chart of how many reviews gave each star rating (1-5)."""
    stars = [1, 2, 3, 4, 5]
    counts = [589519, 249878, 340086, 617251, 2827881]
    plt.figure(figsize=(5, 3))
    ax = plt.gca()
    ax.bar(stars, counts, color="#4C72B0")
    ax.set_title("Rating distribution")
    ax.set_xlabel("Star rating")
    ax.set_ylabel("Number of reviews")
    ax.set_xticks(stars)
    ax.yaxis.set_major_formatter(_MILLIONS)
    plt.tight_layout()
    out = OUTPUT_FIGURES / "fig_rating_distribution.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"wrote {out}")


def data_reduction():
    """Before/after bar chart of the data reduction."""
    labels = ["Raw reviews", "Cleaned ratings"]
    values = [4624615, 953017]
    plt.figure(figsize=(4, 3))
    ax = plt.gca()
    ax.bar(labels, values, color=["#C44E52", "#55A868"])
    ax.set_title("Data reduction")
    ax.set_ylabel("Number of reviews")
    ax.yaxis.set_major_formatter(_MILLIONS)
    ax.set_ylim(0, values[0] * 1.15)  # headroom for the labels
    for i, v in enumerate(values):
        ax.text(i, v, f"{v:,}", ha="center", va="bottom", fontsize=8)
    plt.tight_layout()
    out = OUTPUT_FIGURES / "fig_data_reduction.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"wrote {out}")


def rmse_tuning():
    """Grouped bar chart of RMSE for each (rank, regParam) combination."""
    configs = ["rank=10\nreg=0.05", "rank=10\nreg=0.10",
               "rank=50\nreg=0.05", "rank=50\nreg=0.10"]
    rmse = [1.4325, 1.3301, 1.3388, 1.2767]
    colors = ["#8172B3"] * 3 + ["#55A868"]  # highlight the best
    plt.figure(figsize=(5, 3))
    ax = plt.gca()
    ax.bar(configs, rmse, color=colors)
    ax.set_title("ALS tuning: RMSE by configuration")
    ax.set_ylabel("RMSE (lower is better)")
    ax.set_ylim(1.2, 1.45)
    for i, v in enumerate(rmse):
        ax.text(i, v, f"{v:.3f}", ha="center", va="bottom", fontsize=8)
    plt.tight_layout()
    out = OUTPUT_FIGURES / "fig_rmse_tuning.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"wrote {out}")


if __name__ == "__main__":
    rating_distribution()
    data_reduction()
    rmse_tuning()
    print("all figures written to", OUTPUT_FIGURES)
