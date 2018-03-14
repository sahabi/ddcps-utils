from pandas import read_csv
from glob import glob
from itertools import product
import argparse
import pandas as pd
import sys

def mk_files(dirs, f):
    return ['/'.join([r,ff]) for (r,ff) in product(dirs,[f])]

def globber(root_dir):
    dirs = glob(root_dir + '/*')
    return dirs

def merge(df1, df2, on=["layer","build","time"], how="outer"):
    return pd.merge(df1, df2, on=on, how=how)

def read_rename(f_name):
    df = read_csv(f_name)
    build = f_name.split("/")[-4].lower().replace(" ","_")
    layer = f_name.split("/")[-2].lower().replace(" ","_")
    sensor = f_name.split("/")[-1][:-4].lower().replace(" ","_") + ":"
    df = df.rename(columns={c:sensor+c.lower().replace(" ","_")
        if c != "Time" else c.lower() for c in df.columns })
    df.dropna(axis=1, inplace=True)
    df["layer"] = layer.split("_")[-1]
    df["build"] = build
    return df

def aggregate(root_dir):
    sensor_f_names = ['Heaters.csv', 'Quartz Lamp Duty Cycle.csv', 'Thermocouples.csv']
    build_dirs = ['10_11_16_build/layers','11_03_16_build/layers']
    abs_build_dirs = [root_dir + bd for bd in build_dirs]
    dirs = [item for sublist in map(globber, abs_build_dirs) for item in sublist]
    dfs = {f: pd.concat(map(read_rename, mk_files(dirs, f))) for f in sensor_f_names}
    dfs = reduce(merge, dfs.itervalues())
    return dfs

def smart_fillna(dfs):
    return dfs

def main():
    parser = argparse.ArgumentParser(description='Preprocessing the LAMPS build data')
    parser.add_argument("-agg", '--aggregate', dest='agg', metavar='DIR',  nargs=1,
            help="Aggregate all build data a single dataset"
    parser.add_argument("-f", '--fill', dest='fillna', metavar='DIR', nargs='?',
            default="nofill", help='Fill the empty fields')
    parser.add_argument("-o", '--output', dest='output_name', nargs=1, default='out.csv',
            help="Provide a custom name for the output dataset.")
    args = parser.parse_args()
  
    if args.agg is not None:
        df = aggregate(args.agg[0] + "/")
        if args.fillna != 'nofill':
            df = smart_fillna(df)
        else:
            print ("Warning: There are many fields that are empty due to a small"
                   "missalignment of the sensors timestamps. Consider rerunning"
                   "the program with the option -f (--fill)")
    else:
        if args.fillna != 'nofill':
            if isinstance(args.fillna, str):
                df = pd.read_csv(args.fillna)
                df = smart_fillna(df)
            else:
                print("Make sure that the path to the csv file is valid")

if __name__ == "__main__":
    main()
