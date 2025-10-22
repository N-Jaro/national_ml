#!/usr/bin/env python3
"""
List all result directories from transfer/robustness analyses
"""

import os
from pathlib import Path
from datetime import datetime

def list_results_directories():
    """List all result directories with timestamps and summary info."""
    
    base_dir = Path(__file__).parent
    results_base = base_dir / "results"
    
    if not results_base.exists():
        print("❌ No results directory found")
        return
    
    # Get all result directories
    result_dirs = [d for d in results_base.iterdir() if d.is_dir()]
    
    if not result_dirs:
        print("📂 No analysis results found")
        return
    
    # Sort by modification time (newest first)
    result_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    
    print("🔍 TRANSFER/ROBUSTNESS ANALYSIS RESULTS")
    print("=" * 60)
    print(f"Found {len(result_dirs)} result directories:\n")
    
    for i, result_dir in enumerate(result_dirs, 1):
        # Get directory info
        dir_name = result_dir.name
        mod_time = datetime.fromtimestamp(result_dir.stat().st_mtime)
        
        # Check what files exist
        transfer_exists = (result_dir / "transfer_analysis_results.json").exists()
        robustness_exists = (result_dir / "robustness_analysis_results.json").exists()
        transfer_viz = (result_dir / "transfer_analysis_4a.png").exists()
        robustness_viz = (result_dir / "robustness_analysis_4b.png").exists()
        summary_exists = (result_dir / "analysis_summary.md").exists()
        
        # Status indicators
        status_items = []
        if transfer_exists:
            status_items.append("4a✅")
        else:
            status_items.append("4a❌")
            
        if robustness_exists:
            status_items.append("4b✅")
        else:
            status_items.append("4b❌")
            
        if transfer_viz:
            status_items.append("viz-4a✅")
        else:
            status_items.append("viz-4a❌")
            
        if robustness_viz:
            status_items.append("viz-4b✅")
        else:
            status_items.append("viz-4b❌")
            
        if summary_exists:
            status_items.append("summary✅")
        else:
            status_items.append("summary❌")
        
        status = " | ".join(status_items)
        
        print(f"{i:2d}. 📁 {dir_name}")
        print(f"    🕐 {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"    📊 {status}")
        print(f"    📂 {result_dir}")
        print()

def main():
    list_results_directories()

if __name__ == "__main__":
    main()