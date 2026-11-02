"""Export the Parquet outputs to single CSV files in output/csv/.

Usage:
  python export_csv.py                 # full export of nyc_clean, la_clean, unified
  python export_csv.py --sample 1000   # only the first N rows of each
  python export_csv.py --only unified  # just one dataset
"""
import argparse, glob, os, shutil
from common import *

DATASETS = {"nyc_clean": NYC_CLEAN, "la_clean": LA_CLEAN, "unified": UNIFIED}
CSV_DIR = os.path.join(OUT_DIR, "csv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, help="export only the first N rows")
    ap.add_argument("--only", choices=DATASETS, help="export one dataset")
    args = ap.parse_args()

    s = spark("export_csv")
    os.makedirs(CSV_DIR, exist_ok=True)
    for name in ([args.only] if args.only else DATASETS):
        df = s.read.parquet(DATASETS[name])
        if args.sample:
            df = df.limit(args.sample)
        suffix = f"_sample{args.sample}" if args.sample else ""
        tmp = os.path.join(CSV_DIR, f"_tmp_{name}")
        # One partition -> one CSV file; Spark writes it into a folder, so move it out.
        df.coalesce(1).write.mode("overwrite").csv(tmp, header=True, timestampFormat="yyyy-MM-dd HH:mm:ss")
        dest = os.path.join(CSV_DIR, f"{name}{suffix}.csv")
        shutil.move(glob.glob(os.path.join(tmp, "part-*.csv"))[0], dest)
        shutil.rmtree(tmp)
        print(f"wrote {dest} ({os.path.getsize(dest) / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
