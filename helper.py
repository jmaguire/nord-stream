import pandas as pd
from pathlib import Path

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
    df.to_csv("june_ais_filtered.csv", index=False)

