"""
Extract a dataset from a URL the Week 5 Fantasy Football PPR WR 
"""

import os
import requests
import pandas as pd
from io import StringIO


def extract(
    url="https://raw.githubusercontent.com/danishmaknojia/data/refs/heads/main/WRRankingsWeek5Points.csv",
    url2="https://github.com/danishmaknojia/data/raw/refs/heads/main/WRRankingsWeek5Ranking.csv",
    file_path1="data/WRRankingsWeek5Points.csv",
    file_path2="data/WRRankingsWeek5Ranking.csv",
    directory="data",
    n=12,
):
    """Extracts URLs to their respective file paths with only the first n rows."""
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Download the first file, read it into a DataFrame, truncate to n rows, and save
    with requests.get(url) as r1:
        r1.raise_for_status()  # Check for request errors
        df1 = pd.read_csv(StringIO(r1.text))
        df1.head(n).to_csv(file_path1, index=False)

    # Download the second file, read it into a DataFrame, truncate to n rows, and save
    with requests.get(url2) as r2:
        r2.raise_for_status()  # Check for request errors
        df2 = pd.read_csv(StringIO(r2.text))
        df2.head(n).to_csv(file_path2, index=False)

    return file_path1, file_path2


extract()
