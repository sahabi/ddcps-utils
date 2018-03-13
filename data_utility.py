from pandas import read_csv
from glob import glob
from itertools import product
import pandas as pd

def dir_maker(root_dir):
    dirs = glob(root_dir + '/*')
    return dirs

def read_and_rename(path=f_name):
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

def aggregate(path=root, sensor_files=['Heaters.csv', 'Quartz Lamp Duty Cycle.csv', 'Thermocouples.csv']):
    
    file_names = sensor_files    
    root_directories = ['10_11_16_build/layers/','11_03_16_build/layers']
    dirs = [item for sublist in map(dir_maker, root_directories) for item in sublist]
    
    heaters_files = ['/'.join([r,f]) for (r,f) in product(dirs,[file_names[0]])]
    qldc_files = ['/'.join([r,f]) for (r,f) in product(dirs,[file_names[1]])]
    thermo_files = ['/'.join([r,f]) for (r,f) in product(dirs,[file_names[2]])]
    
    heaters_dfs = map(read_and_rename,heaters_files)
    qldc_dfs = map(read_and_rename,qldc_files)
    thermo_dfs = map(read_and_rename,thermo_files)
    
    heaters_df = pd.concat(heaters_dfs)
    qldc_df = pd.concat(qldc_dfs)
    thermo_df = pd.concat(thermo_dfs)
    dfs = pd.merge(heaters_df, qldc_df, on=["layer","build","time"], how='outer')
    dfs = pd.merge(dfs, thermo_df, on=["layer","build","time"], how='outer')
    return dfs

def smart_fillna(dfs):
    return dfs
