import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import os
import sys
import matplotlib.ticker as mticker


# Set the cwd to parent
os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
MEDIA_DST = Path.cwd() / 'plots' / 'sys_size'

if __name__ == '__main__':
  
  thru_intervals = pd.read_csv("results/sys_size/mu_squared_thrus.csv") 
  curr_idx = 0
  for n in range(3, 11):
    thru_group = thru_intervals[curr_idx:curr_idx+n]
    curr_idx += n
  
  throughputs = []
  with open("results/sys_size/mu_squared_thrus.csv", 'r') as f:
    lines = f.readlines()
    combined_thrus = [0,0,0,0,0,0,0,0,0,0]
    thru_group = lines[-10:]
    for t in thru_group:
      cleaned = t.strip().split(',')[1:]
      for i in range(len(cleaned)):
        combined_thrus[i] += float(cleaned[i])
        
  mu_results = pd.read_csv("results/sys_size/mu.csv")
  total_commits = mu_results['iterations'][0]
  work_time = mu_results['total_work'][0] / 1e6
  mu_thru = total_commits / work_time
  
  

  with open("results/sys_size/mu_squared_latencies.csv", 'r') as f:
     lats = next(
        [float(x) for x in line.split(',') if x.strip() and float(x) < 10]
        for line in f.read().split('\n') if line.strip()
    )

  curr_idx = 0
  fastpath_lats = []
  for n in range(3, 11):
      fastpath_lats.append(sum(lats[curr_idx:curr_idx+n]) / n)
      curr_idx += n
  ################################ Start Graphs ################################
    
  # Sytem-wide throughput vs. System Size
  x = range(1, len(combined_thrus) + 1)          # explicit: seconds 1..10

  fig, ax = plt.subplots(figsize=(7, 4.5), dpi=150, constrained_layout=True)
  ax.plot(x, combined_thrus, marker='o', ms=5, lw=1.8, color='#1f77b4')
  
  ax.axhline(y=mu_thru, color='red', linestyle='--', linewidth=1)

  ax.set_title('System-wide Throughput Over Time (10 nodes)')
  ax.set_xlabel('Time (s)')
  ax.set_ylabel('Throughput (commits/s)')

  ax.set_xticks(list(x))                          # integer seconds, no 2/4/6/8
  ax.set_ylim(bottom=0)
  ax.yaxis.set_major_formatter(
      mticker.FuncFormatter(lambda v, _: f'{v/1e3:.0f}K'))   # 250K not 250000

  ax.grid(alpha=0.3, linewidth=0.6)
  ax.spines[['top', 'right']].set_visible(False)

  fig.savefig(MEDIA_DST / 'thru_vs_time.png', bbox_inches='tight')
  plt.close(fig)
  
  ###
  
  # Average Fastpath Latency vs. System Size
  plt.figure()
  
  plt.plot(range(3, 11), fastpath_lats, marker='o', ms=5, lw=1.8, color='#1f77b4')
  plt.title('Average Fastpath Latency vs. System Size')
  plt.xlabel('System Size (nodes)')
  plt.ylabel('Latency (us)')

  plt.xticks(list(range(3, 11)))
  # plt.ylim(bottom=0) 
  plt.grid(alpha=0.3, linewidth=0.6)
  plt.gca().spines[['top', 'right']].set_visible(False)

  plt.savefig(MEDIA_DST / 'fastpath_lat_vs_sys_size.png', bbox_inches='tight')
  plt.close()
  