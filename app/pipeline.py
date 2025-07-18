import os
import pandas as pd
from dotenv import load_dotenv

# Load .env variables if present
load_dotenv()

# Base directory of the current script (app directory)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data"))

# Construct full paths with fallback to defaults
RAW_PATH = os.getenv("RAW_PATH", os.path.join(DATA_DIR, "Medicaldataset.csv"))
CLEANED_PATH = os.getenv("CLEANED_PATH", os.path.join(DATA_DIR, "CleanedMedicalData.csv"))

def extract_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    print("Extract: raw shape", df.shape)
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # tiny sanity check
    if df.empty:
        raise ValueError("Transformed dataframe is empty!")

    print("Transform: cleaned shape", df.shape)
    return df

def load_data(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print("Load: saved →", path)

def run_pipeline():
    df_raw     = extract_data(RAW_PATH)
    df_cleaned = transform_data(df_raw)
    load_data(df_cleaned, CLEANED_PATH)
    print("Pipeline finished.")

if __name__ == "__main__":
    run_pipeline()
