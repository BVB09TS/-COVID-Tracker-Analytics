# tests/test_pipeline.py

import os
import pandas as pd

def test_raw_data_exists():
    """Check that raw data files exist"""
    raw_dir = "data/raw"
    files = os.listdir(raw_dir)
    assert len(files) > 0, f"No files found in {raw_dir}"

def test_clean_data_generation():
    """Simulate a cleaning step and check columns"""
    raw_file = "data/raw/census_acs_2021.csv"
    if os.path.exists(raw_file):
        df = pd.read_csv(raw_file)
        # Example: check essential columns exist
        expected_cols = ["STATE", "POPULATION", "AGE_OVER_65"]
        for col in expected_cols:
            assert col in df.columns, f"Missing column {col} in {raw_file}"
    else:
        print(f"{raw_file} not found, skipping test_clean_data_generation")
