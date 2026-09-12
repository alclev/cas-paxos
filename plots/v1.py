import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import os
import sys
import matplotlib.ticker as mticker


# Set the cwd to parent
os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
MEDIA_DST = Path.cwd() / 'plots' / 'v1'

DATA_SRC = Path.cwd() / 'results' / 'v1'
if __name__ == '__main__':
  
  # Mu Squared
  DATA_PATH = DATA_SRC / 'musq'
  
  musq_pipe_df = pd.read_csv(DATA_PATH / "pipe.csv") 
  musq_shard_df = pd.read_csv(DATA_PATH / "shards.csv")
  musq_sysize_df = pd.read_csv(DATA_PATH / "sys_size.csv")
  musq_threads_df = pd.read_csv(DATA_PATH / "threads.csv")
  
  raw_lats = []
  raw_thrus = []
  for line in open(DATA_PATH / "sys_size_lats.raw"):
    raw_lats.append(float(line.strip()))
  for line in open(DATA_PATH / "sys_size_thrus.raw"):
    raw_thrus.append(float(line.strip()))
  
  DATA_PATH = DATA_SRC / 'vesq'
  
  vesq_pipe_df = pd.read_csv(DATA_PATH / "pipe.csv")
  vesq_shard_df = pd.read_csv(DATA_PATH / "shards.csv")
  vesq_sysize_df = pd.read_csv(DATA_PATH / "sys_size.csv")
  
  vesq_lats = []
  vesq_thrus = []
  for line in open(DATA_PATH / "sys_size_lats.raw"):
    vesq_lats.append(float(line.strip()))
  for line in open(DATA_PATH / "sys_size_thrus.raw"):
    vesq_thrus.append(float(line.strip()))
  