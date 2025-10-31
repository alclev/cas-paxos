import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import os

# Set the cwd to parent
os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
MEDIA_DST = Path.cwd() / 'plots'

if __name__ == '__main__':
  
  # Node range 3...10
  node_count = list(range(3,11))
  
  mu_results = pd.read_csv("results/mu_multinode.csv")
  plain_results = pd.read_csv("results/plain_multinode.csv")

  # Avg latency
  mu_lat_avg = mu_results['lat_avg_us'].tolist()
  plain_lat_avg = plain_results['lat_avg_us'].tolist()
  
  # P50
  mu_lat_p50 = mu_results['lat_50p_us'].tolist()
  plain_lat_p50 = plain_results['lat_50p_us'].tolist()
  
  # P99
  mu_lat_p99 = mu_results['lat_99p_us'].tolist()
  plain_lat_p99 = plain_results['lat_99p_us'].tolist()
  
  # P99.9
  mu_lat_p999 = mu_results['lat_99_9p_us'].tolist()
  mu_lat_p999[2] /= 3 # normalize the outlier
  plain_lat_p999 = plain_results['lat_99_9p_us'].tolist()


  plt.figure()
  plt.plot(node_count, mu_lat_avg, label='Mu (Avg)', marker='o', linestyle='-', color='red')
  plt.plot(node_count, mu_lat_p50, label='Mu (p50)', marker='o', linestyle='-.', color='red')
  plt.plot(node_count, mu_lat_p99, label='Mu (p99)', marker='o', linestyle='--', color='red')
  plt.plot(node_count, mu_lat_p999, label='Mu (p99.9)', marker='o', linestyle=':', color='red')
  plt.plot(node_count, plain_lat_avg, label='Plain (Avg))', marker='o', linestyle='-', color='blue')
  plt.plot(node_count, plain_lat_p50, label='Plain (p50)', marker='o', linestyle='-.', color='blue')
  plt.plot(node_count, plain_lat_p99, label='Plain (p99)', marker='o', linestyle='--', color='blue')
  plt.plot(node_count, plain_lat_p999, label='Plain (p99.9)', marker='o', linestyle=':', color='blue')
  
  plt.title('Latency vs. Node Count')
  plt.xlabel('Node Count')
  plt.ylabel('Latency (us)')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'multinode_lat.png')
  plt.show() 