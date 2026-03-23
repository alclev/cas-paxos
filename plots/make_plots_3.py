import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import os

# Set the cwd to parent
os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
MEDIA_DST = Path.cwd() / 'plots' / 'six_node'

if __name__ == '__main__':
  
  # Node range 3...6
  node_count = list(range(3,7))
  
  cp_results = pd.read_csv("results/six_node/caspaxos.csv")
  mu_results = pd.read_csv("results/six_node/mu.csv")
  velos_results = pd.read_csv("results/six_node/velos.csv")

  # Avg latency
  cp_lat_avg = cp_results['lat_avg_us'].tolist()[:4]
  cp_opt_lat_avg = cp_results['lat_avg_us'].tolist()[4:]
  mu_lat_avg = mu_results['lat_avg_us'].tolist()
  velos_lat_avg = velos_results['lat_avg_us'].tolist()
  
  # P50
  cp_lat_p50 = cp_results['lat_50p_us'].tolist()[:4]
  cp_opt_lat_p50 = cp_results['lat_50p_us'].tolist()[4:]
  mu_lat_p50 = mu_results['lat_50p_us'].tolist()
  velos_lat_p50 = velos_results['lat_50p_us'].tolist()
  
  
  # P99
  cp_lat_p99 = cp_results['lat_99p_us'].tolist()[:4]
  cp_opt_lat_p99 = cp_results['lat_99p_us'].tolist()[4:]
  mu_lat_p99 = mu_results['lat_99p_us'].tolist()
  velos_lat_p99 = velos_results['lat_99p_us'].tolist()
  
  #p99_99
  cp_lat_p99_99 = cp_results['lat_99_9p_us'].tolist()[:4]
  cp_opt_lat_p99_99 = cp_results['lat_99_9p_us'].tolist()[4:]
  mu_lat_p99_99 = mu_results['lat_99_9p_us'].tolist()
  velos_lat_p99_99 = velos_results['lat_99_9p_us'].tolist()
  
  # cp_opt_v2
  cp_results = pd.read_csv("results/ten_node/caspaxos.csv")
  cp_opt_lat_avg_v2 = cp_results['lat_avg_us'].tolist()
  cp_opt_lat_p50_v2 = cp_results['lat_50p_us'].tolist()
  cp_opt_lat_p99_v2 = cp_results['lat_99p_us'].tolist()
  cp_opt_lat_p99_99_v2 = cp_results['lat_99_9p_us'].tolist()
  
  plt.figure()
  plt.plot(node_count, mu_lat_avg, label='Mu', marker='o', linestyle='-', color='orange')
  plt.plot(node_count, velos_lat_avg, label='Velos', marker='o', linestyle='-', color='blue')
  plt.plot(node_count, cp_lat_avg, label='Plain CasPaxos', marker='o', linestyle='-', color='green')
  plt.plot(node_count, cp_opt_lat_avg, label='Optimized CasPaxos', marker='o', linestyle='-', color='red')
  plt.plot(node_count, cp_opt_lat_avg_v2[8:12], label='Optimized CasPaxos v2', marker='o', linestyle='--', color='red')
  plt.title('Average Latency vs. Node Count')
  plt.xlabel('Node Count')
  plt.ylabel('Latency (us)')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'avg_latency.png')
  # plt.show() 
  
  plt.figure()
  plt.plot(node_count, mu_lat_p50, label='Mu', marker='o', linestyle='-', color='orange')
  plt.plot(node_count, velos_lat_p50, label='Velos', marker='o', linestyle='-', color='blue')
  plt.plot(node_count, cp_lat_p50, label='Plain CasPaxos', marker='o', linestyle='-', color='green')
  plt.plot(node_count, cp_opt_lat_p50, label='Optimized CasPaxos', marker='o', linestyle='-', color='red')
  plt.plot(node_count, cp_opt_lat_p50_v2[8:12], label='Optimized CasPaxos v2', marker='o', linestyle='--', color='red')
  plt.title('P50 Latency vs. Node Count')
  plt.xlabel('Node Count')
  plt.ylabel('Latency (us)')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'p50_latency.png')
  # plt.show() 
  
  plt.figure()
  plt.plot(node_count, mu_lat_p99, label='Mu', marker='o', linestyle='-', color='orange')
  plt.plot(node_count, velos_lat_p99, label='Velos', marker='o', linestyle='-', color='blue')
  plt.plot(node_count, cp_lat_p99, label='Plain CasPaxos', marker='o', linestyle='-', color='green')
  plt.plot(node_count, cp_opt_lat_p99, label='Optimized CasPaxos', marker='o', linestyle='-', color='red')
  plt.plot(node_count, cp_opt_lat_p99_v2[8:12], label='Optimized CasPaxos v2', marker='o', linestyle='--', color='red')
  plt.title('P99 Latency vs. Node Count')
  plt.xlabel('Node Count')
  plt.ylabel('Latency (us)')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'p99_latency.png')
  # plt.show() 
  
  plt.figure()
  plt.plot(node_count, mu_lat_p99_99, label='Mu', marker='o', linestyle='-', color='orange')
  plt.plot(node_count, velos_lat_p99_99, label='Velos', marker='o', linestyle='-', color='blue')
  plt.plot(node_count, cp_lat_p99_99, label='Plain CasPaxos', marker='o', linestyle='-', color='green')
  plt.plot(node_count, cp_opt_lat_p99_99, label='Optimized CasPaxos', marker='o', linestyle='-', color='red')
  plt.plot(node_count, cp_opt_lat_p99_99_v2[8:12], label='Optimized CasPaxos v2', marker='o', linestyle='--', color='red')
  plt.title('P99.99 Latency vs. Node Count')
  plt.xlabel('Node Count')
  plt.ylabel('Latency (us)')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'p99_99_latency.png')
  # plt.show() 
  
  plt.figure()
  plt.plot(node_count, cp_lat_avg, label='Plain CasPaxos', marker='o', linestyle='-', color='green')
  plt.plot(node_count, cp_opt_lat_avg, label='Optimized CasPaxos', marker='o', linestyle='-', color='red')
  node_count = list(range(3,11))

  plt.plot(node_count, cp_opt_lat_avg_v2[8:], label='Optimized CasPaxos v2', marker='o', linestyle='--', color='red')
  plt.title('Average Latency vs. Node Count')
  plt.xlabel('Node Count')
  plt.ylabel('Latency (us)')
  plt.legend()
  plt.grid()
  plt.savefig(MEDIA_DST / 'ten_node_latency.png')
  # plt.show() 
