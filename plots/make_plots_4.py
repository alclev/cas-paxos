import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import os

# Set the cwd to parent
os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
MEDIA_DST = Path.cwd() / 'plots' / 'sys_size'

if __name__ == '__main__':
  
  # Node range 3...10
  node_count = list(range(3,11))
  
  cp_results = pd.read_csv("results/sys_size/caspaxos.csv")
  mu_sq_results = pd.read_csv("results/sys_size/mu_squared.csv")
  mu_results = pd.read_csv("results/sys_size/mu.csv")
  
  # Calculate throughputs 
  
  # CasPaxos
  cp_total_ops = cp_results['total_ops'].tolist()
  cp_total_work_us = cp_results['total_work_us'].tolist()
  cp_thru_mops = [ops / work for ops, work in zip(cp_total_ops, cp_total_work_us)]
  
  # MuSquared
  mu_sq_total_ops = 0
  mu_total_work_us = 0
  mu_sq_thru_mops = []
  mu_sq_lat_avg_ns = []
  mu_sq_lat_50p_ns = []
  mu_sq_lat_99p_ns = []
  mu_sq_lat_99_9p_ns = []
  mu_sq_election_lat_ns = []
  for cnt in node_count:
    ops_group = mu_sq_results[mu_sq_results['system_size'] == cnt]['total_ops'].tolist()
    mu_sq_total_ops = sum(ops_group)
    ops_group = mu_sq_results[mu_sq_results['system_size'] == cnt]['total_work_us'].tolist()
    mu_total_work_us = min(ops_group)
    mu_sq_thru_mops.append(mu_sq_total_ops / mu_total_work_us)
    lat_avg_group = mu_sq_results[mu_sq_results['system_size'] == cnt]['lat_avg_ns'].tolist()
    mu_sq_lat_avg_ns.append(min(lat_avg_group))
    lat_50p_group = mu_sq_results[mu_sq_results['system_size'] == cnt]['lat_50p_ns'].tolist()
    mu_sq_lat_50p_ns.append(min(lat_50p_group))
    lat_99p_group = mu_sq_results[mu_sq_results['system_size'] == cnt]['lat_99p_ns'].tolist()
    mu_sq_lat_99p_ns.append(min(lat_99p_group))
    lat_99_9p_group = mu_sq_results[mu_sq_results['system_size'] == cnt]['lat_99_9p_ns'].tolist()
    mu_sq_lat_99_9p_ns.append(min(lat_99_9p_group))
    mu_sq_election_lat_group = mu_sq_results[mu_sq_results['system_size'] == cnt]['election_lat_ns'].tolist()
    mu_sq_election_lat_ns.append(min(mu_sq_election_lat_group))

  # Mu
  mu_total_ops = mu_results['total_ops'].tolist()
  mu_total_work_us = mu_results['total_work_us'].tolist()
  mu_thru_mops = [ops / work for ops, work in zip(mu_total_ops, mu_total_work_us)]
  
  # Plot throughputs
  plt.figure()
  # plt.plot(node_count, cp_thru_mops, marker='o', label='CasPaxos', color='blue')
  plt.plot(node_count, mu_sq_thru_mops, marker='o', label='Mu Squared', color='green')
  plt.plot(node_count, mu_thru_mops, marker='o', label='Mu', color='red')
  plt.xlabel('System Size (Nodes)')
  plt.ylabel('Throughput (MOPS)')
  # plt.title('System Throughput vs System Size')
  plt.xticks(node_count)
  plt.grid()
  plt.legend()
  plt.savefig(MEDIA_DST / 'throughput.png')
  plt.close()
  
  # # Plot election latency
  # cp_election_lat_us = [lat * 1e-3 for lat in cp_results['election_lat_ns'].tolist()]
  # mu_sq_election_lat_us = [lat * 1e-3 for lat in mu_sq_election_lat_ns]
  # mu_election_lat_us = [lat * 1e-3 for lat in mu_results['election_lat_ns'].tolist()]
  
  # plt.figure()
  # # plt.plot(node_count, cp_election_lat_us, marker='o', label='CasPaxos', color='blue')
  # plt.plot(node_count, mu_sq_election_lat_us, marker='o', label='Mu Squared', color='green')
  # # plt.plot(node_count, mu_election_lat_us, marker='o', label='Mu', color='red')
  # plt.xlabel('System Size (Nodes)')
  # plt.ylabel('Election Latency (us)')
  # plt.title('Election Latency vs System Size')
  # plt.xticks(node_count)
  # plt.grid()
  # plt.legend()
  # plt.savefig(MEDIA_DST / 'election_latency.png')
  # plt.close()
  
  # Plot latency avg
  cp_lat_avg = cp_results['lat_avg_ns'].tolist()
  mu_lat_avg = mu_results['lat_avg_ns'].tolist()
  
  # convert to us
  cp_lat_avg = [lat * 1e-3 for lat in cp_lat_avg]
  mu_sq_lat_avg = [lat * 1e-3 for lat in mu_sq_lat_avg_ns]
  mu_lat_avg = [lat * 1e-3 for lat in mu_lat_avg]

  plt.figure()
  # plt.plot(node_count, cp_lat_avg, marker='o', label='CasPaxos', color='blue')
  plt.plot(node_count, mu_sq_lat_avg, marker='o', label='Mu Squared', color='green')
  plt.plot(node_count, mu_lat_avg, marker='o', label='Mu', color='red')
  plt.xlabel('System Size (Nodes)')
  plt.ylabel('Average Latency (us)')
  # plt.title('Average Latency vs System Size')
  plt.xticks(node_count)
  plt.grid()
  plt.legend()
  plt.savefig(MEDIA_DST / 'latency_avg.png')
  plt.close()
  
  # Plot latency 50p
  cp_lat_50p = cp_results['lat_50p_ns'].tolist()
  mu_lat_50p = mu_results['lat_50p_ns'].tolist()
  
  # convert to us
  # cp_lat_50p = [lat * 1e-3 for lat in cp_lat_50p]
  # mu_sq_lat_50p = [lat * 1e-3 for lat in mu_sq_lat_50p_ns]
  # mu_lat_50p = [lat * 1e-3 for lat in mu_lat_50p]
  
  # plt.figure()
  # plt.plot(node_count, cp_lat_50p, marker='o', label='CasPaxos', color='blue')
  # plt.plot(node_count, mu_sq_lat_50p, marker='o', label='Mu Squared', color='green')
  # plt.plot(node_count, mu_lat_50p, marker='o', label='Mu', color='red')
  # plt.xlabel('System Size (Nodes)')
  # plt.ylabel('50th Percentile Latency (us)')
  # plt.title('50th Percentile Latency vs System Size')
  # plt.xticks(node_count)
  # plt.grid()
  # plt.legend()
  # plt.savefig(MEDIA_DST / 'latency_50p.png')
  # plt.close()
  
  # # Plot latency 99p 
  # cp_lat_99p = cp_results['lat_99p_ns'].tolist()
  # mu_lat_99p = mu_results['lat_99p_ns'].tolist()
  
  # # convert to us
  # cp_lat_99p = [lat * 1e-3 for lat in cp_lat_99p]
  # mu_sq_lat_99p = [lat * 1e-3 for lat in mu_sq_lat_99p_ns]
  # mu_lat_99p = [lat * 1e-3 for lat in mu_lat_99p]

  # plt.figure()
  # plt.plot(node_count, cp_lat_99p, marker='o', label='CasPaxos', color='blue')
  # plt.plot(node_count, mu_sq_lat_99p, marker='o', label='Mu Squared', color='green')
  # plt.plot(node_count, mu_lat_99p, marker='o', label='Mu', color='red')
  # plt.xlabel('System Size (Nodes)')
  # plt.ylabel('99th Percentile Latency (us)')
  # plt.title('99th Percentile Latency vs System Size')
  # plt.xticks(node_count)
  # plt.grid()
  # plt.legend()
  # plt.savefig(MEDIA_DST / 'latency_99p.png')
  # plt.close()
  
  # # Plot latency 99.9p
  # cp_lat_99_9p = cp_results['lat_99_9p_ns'].tolist()
  # mu_lat_99_9p = mu_results['lat_99_9p_ns'].tolist()
  
  # # convert to us
  # cp_lat_99_9p = [lat * 1e-3 for lat in cp_lat_99_9p]
  # mu_sq_lat_99_9p = [lat * 1e-3 for lat in mu_sq_lat_99_9p_ns]
  # mu_lat_99_9p = [lat * 1e-3 for lat in mu_lat_99_9p]
  
  # plt.figure()
  # plt.plot(node_count, cp_lat_99_9p, marker='o', label='CasPaxos', color='blue')
  # plt.plot(node_count, mu_sq_lat_99_9p, marker='o', label='Mu Squared', color='green')
  # plt.plot(node_count, mu_lat_99_9p, marker='o', label='Mu', color='red')
  # plt.xlabel('System Size (Nodes)')
  # plt.ylabel('99.9th Percentile Latency (us)')
  # plt.title('99.9th Percentile Latency vs System Size')
  # plt.xticks(node_count)
  # plt.grid()
  # plt.legend()
  # plt.savefig(MEDIA_DST / 'latency_99_9p.png')
  # plt.close()


  # fig, axes = plt.subplots(2, 2, figsize=(14, 10))

  # data = [
  #     (cp_lat_avg, mu_sq_lat_avg, mu_lat_avg, 'Average Latency (us)', 'Average Latency vs System Size'),
  #     (cp_lat_50p, mu_sq_lat_50p, mu_lat_50p, '50th Percentile Latency (us)', 'P50 Latency vs System Size'),
  #     (cp_lat_99p, mu_sq_lat_99p, mu_lat_99p, '99th Percentile Latency (us)', 'P99 Latency vs System Size'),
  #     (cp_lat_99_9p, mu_sq_lat_99_9p, mu_lat_99_9p, '99.9th Percentile Latency (us)', 'P99.9 Latency vs System Size'),
  # ]

  # for ax, (cp, mu_sq, mu, ylabel, title) in zip(axes.flat, data):
  #     ax.plot(node_count, cp, marker='o', label='CasPaxos', color='blue')
  #     ax.plot(node_count, mu_sq, marker='o', label='Mu Squared', color='green')
  #     ax.plot(node_count, mu, marker='o', label='Mu', color='red')
  #     ax.set_xlabel('System Size (Nodes)')
  #     ax.set_ylabel(ylabel)
  #     ax.set_title(title)
  #     ax.set_xticks(node_count)
  #     ax.grid()
  #     ax.legend()

  # plt.tight_layout()
  # plt.savefig(MEDIA_DST / 'latency_combined.png')
  # plt.close()