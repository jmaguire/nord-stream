import pandas as pd
from pathlib import Path
import csv

def merge_files(directory):
    merged_df = pd.DataFrame()
    for filepath in Path(directory).glob('*filtered.csv'):
        print(filepath)
        df = pd.read_csv(filepath)
        merged_df = pd.concat([merged_df, df])
    return merged_df

def sort_by_time(df):
    date_format = '%d/%m/%Y %H:%M:%S'
    df['datetime'] = pd.to_datetime(df['# Timestamp'])
    df = df.sort_values(by='datetime', ascending=False)
    df = df.drop('datetime', axis=1)
    return df

def save_analysis(pivot_json, filepath):
    p = [{'MMSI':key, 'points':len(pivot_json[key])} for key in pivot_json]
    with open(filepath, 'w') as f:
        writer = csv.DictWriter(f, ['MMSI','points'])
        writer.writeheader()
        writer.writerows(p)