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
  
  # raw_lats = []
  # raw_thrus = []
  # for line in open(DATA_PATH / "sys_size_lats.raw"):
  #   raw_lats.append(list(map(float, line.strip().split(','))))
  # for line in open(DATA_PATH / "sys_size_thrus.raw"):
  #   raw_thrus.append(float(line.strip()))
  
  DATA_PATH = DATA_SRC / 'vesq'
  
  vesq_pipe_df = pd.read_csv(DATA_PATH / "pipe.csv")
  vesq_shard_df = pd.read_csv(DATA_PATH / "shards.csv")
  vesq_sysize_df = pd.read_csv(DATA_PATH / "sys_size.csv")
  
  # vesq_lats = []
  # vesq_thrus = []
  # for line in open(DATA_PATH / "sys_size_lats.raw"):
  #   vesq_lats.append(float(line.strip()))
  # for line in open(DATA_PATH / "sys_size_thrus.raw"):
  #   vesq_thrus.append(float(line.strip()))
    
  DATA_PATH = DATA_SRC / 'mu'
  
  mu_pipe_df = pd.read_csv(DATA_PATH / "pipe.csv")
  mu_sysize_df = pd.read_csv(DATA_PATH / "sysize.csv")
  
  DATA_PATH = DATA_SRC / 'velos'
  
  velos_pipe_df = pd.read_csv(DATA_PATH / "pipe.csv")
  velos_sysize_df = pd.read_csv(DATA_PATH / "sysize.csv")
    
  ##################### System throughout vs. system size #####################
  
  musq_thrus = []
  
  for i in range(3, 11):
    sub = musq_sysize_df[musq_sysize_df['system_size'] == i]
    commits = sub['total_commits'].sum()
    time    = sub['work_time_us'].mean()   
    musq_thrus.append(commits / time)
  
  vesq_thrus = []
  for i in range(3,11):
    sub = vesq_sysize_df[vesq_sysize_df['system_size'] == i]
    commits = sub['total_commits'].sum()
    time    = sub['work_time_us'].mean()
    vesq_thrus.append(commits / time)
    
  mu_thrus = []
  total_commits = mu_sysize_df['total_ops'].tolist()
  total_time = mu_sysize_df['total_work_us'].tolist()
  for i in range(len(total_commits)):
    mu_thrus.append(total_commits[i] / total_time[i])
    
  velos_thrus = []
  total_commits = velos_sysize_df['total_commits'].tolist()
  total_time = velos_sysize_df['total_work_us'].tolist()
  for i in range(len(total_commits)):
    velos_thrus.append(total_commits[i] / total_time[i]) 
    
  nodes = [3,4,5,6,7,8,9,10]
    
  plt.figure()
  plt.plot(nodes, musq_thrus, marker='o', label='Mu Squared')
  plt.plot(nodes, vesq_thrus, marker='o', label='Velos Squared')
  plt.plot(nodes, mu_thrus, marker='o', label='Mu')
  plt.plot(nodes, velos_thrus, marker='o', label='Velos')
  plt.xlabel('System Size (Nodes)')
  plt.ylabel('Throughput (Mops/s)')
  plt.title('System Throughput vs. System Size')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'thru_vs_sys_size.png')
  
  # Avg. Latency vs. System Size
  musq_avg_lats = []
  musq_p50_lats = []
  musq_p90_lats = []
  musq_p99_lats = []
  musq_p999_lats = []
  for i in range(3, 11):
    sub = musq_sysize_df[musq_sysize_df['system_size'] == i]
    musq_avg_lats.append(sub['lat_avg_us'].min())
    musq_p50_lats.append(sub['lat_50p_us'].min())
    musq_p90_lats.append(sub['lat_99p_us'].min())
    musq_p999_lats.append(sub['lat_99_9p_us'].min())

  vesq_avg_lats = []
  vesq_p50_lats = []
  vesq_p90_lats = []
  vesq_p99_lats = []
  vesq_p999_lats = []
  for i in range(3, 11):
    sub = vesq_sysize_df[vesq_sysize_df['system_size'] == i]
    vesq_avg_lats.append(sub['lat_avg_us'].min())
    vesq_p50_lats.append(sub['lat_50p_us'].min())
    vesq_p90_lats.append(sub['lat_99p_us'].min())
    vesq_p999_lats.append(sub['lat_99_9p_us'].min())
    
  mu_avg_lats = mu_sysize_df['lat_avg_ns'].tolist()
  mu_p50_lats = mu_sysize_df['lat_50p_ns'].tolist()
  mu_p99_lats = mu_sysize_df['lat_99p_ns'].tolist()
  mu_p999_lats = mu_sysize_df['lat_99_9p_ns'].tolist()
  
  mu_avg_lats = [lat / 1000 for lat in mu_avg_lats]
  mu_p50_lats = [lat / 1000 for lat in mu_p50_lats]
  mu_p99_lats = [lat / 1000 for lat in mu_p99_lats]
  mu_p999_lats = [lat / 1000 for lat in mu_p999_lats]
  
  velos_avg_lats = velos_sysize_df['lat_avg_ns'].tolist()
  velos_p50_lats = velos_sysize_df['lat_50p_ns'].tolist()
  velos_p99_lats = velos_sysize_df['lat_99p_ns'].tolist()
  velos_p999_lats = velos_sysize_df['lat_99_9p_ns'].tolist()
  
  plt.figure()
  plt.plot(nodes, musq_avg_lats, marker='o', label='Mu Squared')
  plt.plot(nodes, vesq_avg_lats, marker='o', label='Velos Squared')
  plt.plot(nodes, mu_avg_lats, marker='o', label='Mu')
  plt.plot(nodes, velos_avg_lats, marker='o', label='Velos')
  plt.xlabel('System Size (Nodes)')
  plt.ylabel('Average Latency (us)')
  plt.title('Average Latency vs. System Size')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'lat_avg_vs_sys_size.png')
  
  plt.figure()
  plt.plot(nodes, musq_p50_lats, marker='o', label='Mu Squared')
  plt.plot(nodes, vesq_p50_lats, marker='o', label='Velos Squared')
  plt.plot(nodes, mu_p50_lats, marker='o', label='Mu')
  plt.plot(nodes, velos_p50_lats, marker='o', label='Velos')
  plt.xlabel('System Size (Nodes)')
  plt.ylabel('50th Percentile Latency (us)')
  plt.title('50th Percentile Latency vs. System Size')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'lat_50p_vs_sys_size.png')  
  
  plt.figure()
  plt.plot(nodes, musq_p90_lats, marker='o', label='Mu Squared')
  plt.plot(nodes, vesq_p90_lats, marker='o', label='Velos Squared')
  plt.plot(nodes, mu_p99_lats, marker='o', label='Mu')
  plt.plot(nodes, velos_p99_lats, marker='o', label='Velos')
  plt.xlabel('System Size (Nodes)')
  plt.ylabel('99th Percentile Latency (us)')
  plt.title('99th Percentile Latency vs. System Size')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'lat_99p_vs_sys_size.png')
  
  plt.figure()
  plt.plot(nodes, musq_p999_lats, marker='o', label='Mu Squared')
  plt.plot(nodes, vesq_p999_lats, marker='o', label='Velos Squared')
  plt.plot(nodes, mu_p999_lats, marker='o', label='Mu')
  plt.plot(nodes, velos_p999_lats, marker='o', label='Velos')
  plt.xlabel('System Size (Nodes)')
  plt.ylabel('99.9th Percentile Latency (us)')
  plt.title('99.9th Percentile Latency vs. System Size')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'lat_99_9p_vs_sys_size.png')
  
  # Latency vs. Throughput
  
  musq_lats = []
  musq_thrus = []
  
  vesq_lats = []
  vesq_thrus = []
  
  mu_lats = []
  mu_thrus = []
  
  velos_lats = []
  velos_thrus = []
  
  for line in open(DATA_SRC / 'musq/sys_size_lats.raw'):
    musq_lats = list(map(float, line.strip().split(',')))
    break
  
  for line in open(DATA_SRC / 'musq/sys_size_thrus.raw'):
    musq_thrus += list(map(float, line.strip().split(',')))

  for line in open(DATA_SRC / 'vesq/sys_size_lats.raw'):
    vesq_lats = list(map(float, line.strip().split(',')))
    break
  for line in open(DATA_SRC / 'vesq/sys_size_thrus.raw'):
    vesq_thrus += list(map(float, line.strip().split(',')))

  for line in open(DATA_SRC / 'mu/mu_sysize_lats.raw'):
    mu_lats = list(map(float, line.strip().split(',')))
    break
  mu_lats = [lat / 1000 for lat in mu_lats]
  
  for line in open(DATA_SRC / 'mu/mu_sysize_thrus.raw'):
    mu_thrus = list(map(float, line.strip().split(',')))
    break
  
  for line in open(DATA_SRC / 'velos/velos_sysize_lats.raw'):
    velos_lats = list(map(float, line.strip().split(',')))
    break
  for line in open(DATA_SRC / 'velos/velos_sysize_thrus.raw'):
    velos_thrus = list(map(float, line.strip().split(',')))
    break
  
  # print(musq_thrus)
  # print()
  # print(vesq_thrus)
  # print()
  # print(mu_thrus)
  # print()
  # print(velos_thrus)

  plt.figure()
  plt.plot(musq_thrus[:19], musq_lats, marker='o', label='Mu Squared')
  plt.plot(vesq_thrus[:19], vesq_lats, marker='o', label='Velos Squared')
  plt.plot(mu_thrus, mu_lats, marker='o', label='Mu')
  plt.plot(velos_thrus, velos_lats, marker='o', label='Velos')
  plt.xlabel('Throughput (Mops/s)')
  plt.ylabel('Average Latency (us)')
  plt.title('Average Latency vs. Throughput')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'lat_vs_thru.png')
  
  # System throughput vs. pipe depth

  musq_pipe_thrus = []
  for i in range(1, 13):
    sub = musq_pipe_df[musq_pipe_df['pipe_depth'] == i]
    commits = sub['total_commits'].sum()
    time    = sub['work_time_us'].min()   
    musq_pipe_thrus.append(commits / time)
    
  vesq_pipe_thrus = []
  for i in range(1, 13):
    sub = vesq_pipe_df[vesq_pipe_df['pipe_depth'] == i]
    commits = sub['total_commits'].sum()
    time    = sub['work_time_us'].min()   
    vesq_pipe_thrus.append(commits / time)
    
  mu_pipe_commits = mu_pipe_df['total_ops'].tolist()
  mu_pipe_time = mu_pipe_df['total_work_us'].tolist()
  mu_pipe_thrus = []
  for i in range(len(mu_pipe_commits)):
    mu_pipe_thrus.append(mu_pipe_commits[i] / mu_pipe_time[i])
    
  velos_pipe_commits = velos_pipe_df['total_commits'].tolist()
  velos_pipe_time = velos_pipe_df['total_work_us'].tolist()
  velos_pipe_thrus = []
  for i in range(len(velos_pipe_commits)):
    velos_pipe_thrus.append(velos_pipe_commits[i] / velos_pipe_time[i])
    
  pipe_depths = list(range(1, 13))
  
  plt.figure()
  plt.plot(pipe_depths, musq_pipe_thrus, marker='o', label='Mu Squared')
  plt.plot(pipe_depths, vesq_pipe_thrus, marker='o', label='Velos Squared')
  plt.plot(pipe_depths, mu_pipe_thrus, marker='o', label='Mu')
  plt.plot(pipe_depths, velos_pipe_thrus, marker='o', label='Velos')
  plt.xlabel('Pipeline Depth')
  plt.ylabel('Throughput (Mops/s)')
  plt.title('System Throughput vs. Pipeline Depth')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'thru_vs_pipe_depth.png')
  
  # Throughput vs. Shard Count

  shards = [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75]
  
  musq_shard_thrus = []
  for i in shards:
    sub = musq_shard_df[musq_shard_df['num_shards'] == i]
    commits = sub['total_commits'].sum()
    time    = sub['work_time_us'].min()   
    musq_shard_thrus.append(commits / time)
  
  vesq_shard_thrus = []
  for i in shards:
    sub = vesq_shard_df[vesq_shard_df['num_shards'] == i]
    commits = sub['total_commits'].sum()
    time    = sub['work_time_us'].min()   
    vesq_shard_thrus.append(commits / time)
  
  
  plt.figure()
  plt.plot(shards, musq_shard_thrus, marker='o', label='Mu Squared')
  plt.plot(shards, vesq_shard_thrus, marker='o', label='Velos Squared')
  plt.xlabel('Number of Shards')
  plt.ylabel('Throughput (Mops/s)')
  plt.title('System Throughput vs. Number of Shards')
  plt.legend()  
  plt.grid()
  plt.savefig(MEDIA_DST / 'thru_vs_shard_count.png')