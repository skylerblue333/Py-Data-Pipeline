import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract(file_path):
    logging.info(f"Extracting data from {file_path}")
    # Create dummy data if file doesn't exist
    if not os.path.exists(file_path):
        df = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30], 'category': ['A', 'B', 'A']})
        df.to_csv(file_path, index=False)
    return pd.read_csv(file_path)

def transform(df):
    logging.info("Transforming data...")
    df['value_squared'] = df['value'] ** 2
    df['is_high'] = df['value'] > 15
    return df

def load(df, output_path):
    logging.info(f"Loading data to {output_path}")
    df.to_csv(output_path, index=False)
    logging.info("Pipeline complete.")

if __name__ == "__main__":
    input_file = "input_data.csv"
    output_file = "output_data.csv"
    
    raw_data = extract(input_file)
    processed_data = transform(raw_data)
    load(processed_data, output_file)
    print(processed_data.head())