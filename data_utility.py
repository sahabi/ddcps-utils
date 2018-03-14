from pandas import read_csv
from glob import glob
from itertools import product
import pandas as pd

def mk_files(dirs, f):
    return ['/'.join([r,ff]) for (r,ff) in product(dirs,[f])]

def globber(root_dir):
    dirs = glob(root_dir + '/*')
    return dirs

def merge(df1, df2, on=["layer","build","time"], how="outer"):
    return pd.merge(df1, df2, on=on, how=how)

def read_rename(path=f_name):
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

def aggregate(path=root, sensor_files):

    snsrs_f_names = ['Heaters.csv', 'Quartz Lamp Duty Cycle.csv', 'Thermocouples.csv']
    root_dirs = ['10_11_16_build/layers','11_03_16_build/layers']
    dirs = [item for sublist in map(globber, root_dirs) for item in sublist]
    files = [['/'.join([r,ff]) for (r,ff) in product(dirs,[f])] for f in snsrs_f_names]
    dfs = {f: pd.concat(map(read_rename, mk_files(dirs, f))) for f in files}
    dfs = reduce(merge, dfs.itervalues())
    return dfs

def smart_fillna(dfs):
    return dfs
