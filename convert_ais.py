from algos import is_inside_sm
import json
import pandas as pd
import argparse
import sys
import time
from collections import defaultdict


# NS1
# 55°34'43.52"N,  15°38'23.91"E or 55.578756, 15.639975
# 55°34'53.35"N,  15°50'24.88"E or 55.581486, 15.840244
# 55°30'28.29"N,  15°51'1.13"E or 55.507858, 15.850314
# 55°30'22.80"N,  15°38'31.49"E or 55.506333, 15.642081

# NS2
# 54°53'25.49"N, 15°23'2.84"E or 54.890414, 15.384122
# 54°53'27.87"N, 15°25'52.74"E or 54.891075, 15.431317
# 54°51'47.88"N, 15°26'1.47"E or 54.863300, 15.433742
# 54°51'47.36"N, 15°23'8.64"E or 54.863156, 15.385733

NS1_polygon = [(55.578756, 15.639975), (55.581486, 15.840244),
               (55.507858, 15.850314), (55.506333, 15.642081), (55.578756, 15.639975)]
NS2_polygon = [(54.890414, 15.384122), (54.891075, 15.431317),
               (54.863300, 15.433742), (54.863156, 15.385733), (54.890414, 15.384122)]


def transform_ais_data(df):
    ship_dict = {}
    for index, row in df.iterrows():
        key = row['MMSI']
        latitude = row['Latitude']
        longitude = row['Longitude']
        in_NS1 = is_inside_sm(NS1_polygon, (latitude, longitude))
        in_NS2 = is_inside_sm(NS2_polygon, (latitude, longitude))
        if in_NS1 or in_NS2:
            entry = {'point': {'latitude': latitude,
                               'longitude': longitude,
                               in_NS1: in_NS1,
                               in_NS2: in_NS2},
                     'timestamp': row['# Timestamp']}
            if key in ship_dict:
                ship_dict[key].append(entry)
            else:
                ship_dict[key] = [entry]
        else:
            continue
    return ship_dict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f', '--file',
        help='ais file',
    )
    args = parser.parse_args()

    if not args.file:
        parser.print_usage()
        return sys.exit(1)

    print(args.file)
    start_time = time.time()
    df = pd.read_csv(args.file)
    end_time = time.time()
    print("Read file: ", (end_time-start_time), "seconds")
    start_time = time.time()
    transformed_df = transform_ais_data(df)
    end_time = time.time()
    print("Transformed file: ", (end_time-start_time), "seconds")

    with open("sample.json", "w") as f:
        json.dump(transformed_df, f)

    NS1_polygon.append(NS1_polygon[0])
    NS2_polygon.append(NS1_polygon[0])


if __name__ == '__main__':
    main()
