import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
import pandas as pd
import os

# Set the cwd to parent
os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
MEDIA_DST = Path.cwd() / 'plots'

output_file = MEDIA_DST / 'failover_latency_histogram.png'

if __name__ == '__main__':
  
  results = pd.read_csv("results/failover.csv")
  data = results['failover_time_us'].values
  
  mean_val = np.mean(data)
  median_val = np.median(data)
  std_val = np.std(data)
  p50 = np.percentile(data, 50)
  p95 = np.percentile(data, 95)
  p99 = np.percentile(data, 99)
  min_val = np.min(data)
  max_val = np.max(data)
  
  print(f"Statistics for {len(data)} samples:")
  print(f"  Mean:   {mean_val:.2f} µs")
  print(f"  Median: {median_val:.2f} µs")
  print(f"  Std:    {std_val:.2f} µs")
  print(f"  Min:    {min_val:.2f} µs")
  print(f"  Max:    {max_val:.2f} µs")
  print(f"  p50:    {p50:.2f} µs")
  print(f"  p95:    {p95:.2f} µs")
  print(f"  p99:    {p99:.2f} µs")
  
  # Create figure
  fig, ax = plt.subplots(figsize=(10, 6))
  
  # Determine bin width (Freedman-Diaconis rule)
  q75, q25 = np.percentile(data, [75, 25])
  iqr = q75 - q25
  bin_width = 2 * iqr / (len(data) ** (1/3))
  n_bins = int(np.ceil((max_val - min_val) / bin_width))
  n_bins = max(20, min(n_bins, 100))  # Clamp between 20 and 100 bins
  
  # Plot histogram
  counts, bins, patches = ax.hist(data, bins=n_bins, 
                                    color='steelblue', 
                                    alpha=0.7, 
                                    edgecolor='black',
                                    linewidth=0.5)
  
  # Add vertical lines for percentiles
  ax.axvline(median_val, color='red', linestyle='--', linewidth=2, 
              label=f'Median: {median_val:.1f} µs')
  ax.axvline(p95, color='orange', linestyle='--', linewidth=2, 
              label=f'p95: {p95:.1f} µs')
  ax.axvline(p99, color='darkred', linestyle='--', linewidth=2, 
              label=f'p99: {p99:.1f} µs')
  
  # Labels and title
  ax.set_xlabel('Failover Latency (µs)', fontsize=12, fontweight='bold')
  ax.set_ylabel('Frequency', fontsize=12, fontweight='bold')
  ax.set_title(f'Failover Latency Distribution (n={len(data)})', 
                fontsize=14, fontweight='bold')
  
  # Add statistics text box
  stats_text = f'Mean: {mean_val:.1f} µs\n'
  stats_text += f'Std: {std_val:.1f} µs\n'
  stats_text += f'Min: {min_val:.1f} µs\n'
  stats_text += f'Max: {max_val:.1f} µs'
  
  ax.text(0.98, 0.97, stats_text,
          transform=ax.transAxes,
          fontsize=10,
          verticalalignment='top',
          horizontalalignment='right',
          bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
  
  # Legend
  ax.legend(loc='upper right', fontsize=10, framealpha=0.9)
  
  # Grid
  ax.grid(True, alpha=0.3, linestyle='--')
  
  # Tight layout
  plt.tight_layout()
  
  # Save
  plt.savefig(output_file, dpi=300, bbox_inches='tight')
  print(f"\nPlot saved to: {output_file}")
  
  # Show
  plt.show()
 
 
  # Sort data
  sorted_data = np.sort(data)
  cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
  
  # Calculate percentiles
  p50 = np.percentile(data, 50)
  p95 = np.percentile(data, 95)
  p99 = np.percentile(data, 99)
  
  # Create figure
  fig, ax = plt.subplots(figsize=(10, 6))
  
  output_file = MEDIA_DST / 'failover_latency_cdf.png'
  
  # Plot CDF
  ax.plot(sorted_data, cdf * 100, linewidth=2, color='steelblue')
  
  # Add percentile markers
  ax.axvline(p50, color='red', linestyle='--', linewidth=2, alpha=0.7,
              label=f'p50: {p50:.1f} µs')
  ax.axvline(p95, color='orange', linestyle='--', linewidth=2, alpha=0.7,
              label=f'p95: {p95:.1f} µs')
  ax.axvline(p99, color='darkred', linestyle='--', linewidth=2, alpha=0.7,
              label=f'p99: {p99:.1f} µs')
  
  # Horizontal lines at percentile levels
  ax.axhline(50, color='red', linestyle=':', linewidth=1, alpha=0.5)
  ax.axhline(95, color='orange', linestyle=':', linewidth=1, alpha=0.5)
  ax.axhline(99, color='darkred', linestyle=':', linewidth=1, alpha=0.5)
  
  # Labels
  ax.set_xlabel('Failover Latency (µs)', fontsize=12, fontweight='bold')
  ax.set_ylabel('CDF (%)', fontsize=12, fontweight='bold')
  ax.set_title(f'Failover Latency CDF (n={len(data)})', 
                fontsize=14, fontweight='bold')
  
  # Legend
  ax.legend(loc='lower right', fontsize=10, framealpha=0.9)
  
  # Grid
  ax.grid(True, alpha=0.3, linestyle='--')
  
  # Set y-axis limits
  ax.set_ylim([0, 100])
  
  # Tight layout
  plt.tight_layout()
  
  # Save
  plt.savefig(output_file, dpi=300, bbox_inches='tight')
  print(f"CDF plot saved to: {output_file}")
  
  # Show
  plt.show()