# datasets/

Put the two raw CSV files here. They are **not** in the git repo because they are about 1 GB
in total (git ignores everything in this folder except this README).

## Download

1. Download the course dataset from UCR eLearn (Canvas). You need a UCR login, so the scripts
   can't download it for you:
   https://elearn.ucr.edu/courses/238702/files/26557259/download?download_frd=1
2. If the download is a zip, extract it.
3. Move the two CSVs into this folder so it looks like this:

```
CS236-Project/
└── datasets/
    ├── README.md
    ├── nyc_311_800k.csv   (~713 MB, 800,000 rows)
    └── la_311_800k.csv    (~344 MB, 800,000 rows)
```

The file names must match exactly.

## Other locations

The pipeline looks for the CSVs in this order:
1. The folder in the `CS236_DATA_DIR` environment variable, if it's set.
2. The closest folder named `datasets` that contains `nyc_311_800k.csv`, starting from
   `Dataset-Processing/` and moving up. This folder is checked first, then folders above the repo.

Example: `export CS236_DATA_DIR=/path/to/my/csvs`

## About the data

These files are 800,000-row extracts prepared for the course from the public NYC Open Data
"311 Service Requests" dataset and the Los Angeles Open Data "MyLA311 Service Request Data"
dataset. Use the eLearn files so your results match the report. Downloading from the public
portals gives different rows.
