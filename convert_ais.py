import algos as algos
import json
import pandas as pd
import argparse
import sys
import time
import simplekml
import numpy as np
from multiprocessing import  Pool
from pathlib import Path

CORES = 8
SAVE_CSV = True
SAVE_KML = True
SAVE_JSON= True
OUTPUT = './output/'
FILETRACKER = 'processed.json'

# General parallel execution of a function func for data frame df
def parallelize_dataframe(df, func, n_cores=CORES):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

# Create point / time data for each ship
def pivot_data(df):
    ship_dict = {}
    for index, row in df.iterrows():
        key = row['MMSI']
        latitude = row['Latitude']
        longitude = row['Longitude']
        entry = {'point': {'latitude': latitude,
                            'longitude': longitude},
                    'timestamp': row['# Timestamp']}
        if key in ship_dict:
            ship_dict[key].append(entry)
        else:
            ship_dict[key] = [entry]
    return ship_dict

# Take a data frame and extract rows in NS1 or NS2
def filter_rows(df):
    points = df.iloc[:,3:5]
    df['Inside NS1'] = points.apply(algos.inside_ns1, axis = 1)
    df['Inside NS2'] = points.apply(algos.inside_ns2, axis = 1)
    return df.loc[(df['Inside NS1'] == 1) | (df['Inside NS2'] == 1)]    

# Dump KML lines per ship
def pivot_data_to_kml(ship_dict):
    kml = simplekml.Kml()
    for key in ship_dict:
        linestring = kml.newlinestring(name=key)
        linestring.coords = [(elem['point']['longitude'], elem['point']['latitude']) for elem in ship_dict[key]]
    return kml


def process_file(filepath, save_kml = SAVE_KML, save_csv = SAVE_CSV, save_json = SAVE_JSON):

    # Read data as a dataframe. This can be converted to dask
    # for files that are too big for RAM
    start_time = time.time()
    df = pd.read_csv(filepath)
    end_time = time.time()
    print("Read file: ", (end_time-start_time), "seconds")

    # Apply filter function to chunks in parallel
    # filters to rows in NS1 and NS2
    start_time = time.time()
    filtered_data = parallelize_dataframe(df, filter_rows) 
    end_time = time.time()
    print("Filtered file parallel: ", (end_time-start_time), "seconds", "size", filtered_data.shape[0])

    # From here on the processing time takes milliseconds..
    # Pivot to group by MMSI and generate KML
    points_data = pivot_data(filtered_data)
    kml_data = pivot_data_to_kml(points_data)

    ## Save shit
    file_stem = OUTPUT + Path(filepath).stem
    if save_kml:
        kml_data.save(file_stem + '_filtered.kml')
    if save_csv:
        filtered_data.to_csv(file_stem + '_filtered.csv')
    if save_json:
        with open(file_stem + '_filtered.json', "w") as f:
            json.dump(points_data, f)

def process_directory(directory, files_processed):
    for filepath in Path(directory).glob('*.csv'):
        if str(filepath) in files_processed:
            continue
        print('Processing', filepath)
        df = pd.read_csv(filepath)
        filtered_data = parallelize_dataframe(df, filter_rows)

        file_stem = OUTPUT + Path(filepath).stem
        filtered_data.to_csv(file_stem + '_filtered.csv')
        
        # Save data each successful loop
        files_processed.append(str(filepath))
        with open(FILETRACKER, 'w') as f:
            json.dump(files_processed, f)

def main():

    files_processed = []
    if Path(FILETRACKER).is_file():
        with open(FILETRACKER, 'r') as f:
            files_processed = json.load(f)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f', '--file',
        help='ais file',
    )
    parser.add_argument(
        '-d', '--directory',
        help='ais directory',
    )
    args = parser.parse_args()

    if args.file:
        process_file(args.file)
    elif args.directory:
        print('Processing directory. We have already processed:',files_processed)
        process_directory(args.directory, files_processed)
    else:
        parser.print_usage()
        return sys.exit(1)
    

    
    
if __name__ == '__main__':
    main()
