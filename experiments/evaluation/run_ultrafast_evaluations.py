#!/usr/bin/env python3
"""
Ultra-Fast Direct MDMT Evaluation Runner (Minimal Logging)
Optimized for speed with minimal logging overhead
"""

import os
import sys
import argparse
import time
from datetime import datetime

# Add paths for imports
sys.path.append('/u/nathanj/national_ml/experiments')
sys.path.append('/u/nathanj/national_ml/src')
sys.path.append('/u/nathanj/national_ml/experiments/models')
sys.path.append('/u/nathanj/national_ml/experiments/data')
sys.path.append('/u/nathanj/national_ml/experiments/evaluation')

# Minimal logging setup - only critical messages
import logging
logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')

print("🚀 Ultra-Fast MDMT Evaluation (Minimal Logging Mode)")
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Variant configurations
VARIANT_CONFIGS = {
    'alphaearth': {
        'config_file': 'run_configs/alphaearth_runs.txt',
        'evaluator_module': 'ultra_fast_alphaearth_evaluator',
    },
    'dem_alphaearth': {
        'config_file': 'run_configs/dem_alphaearth_runs.txt', 
        'evaluator_module': 'ultra_fast_dem_alphaearth_evaluator',
    },
    'dem_only': {
        'config_file': 'run_configs/dem_only_runs.txt',
        'evaluator_module': 'ultra_fast_dem_only_evaluator',
    },
    'dem_optical': {
        'config_file': 'run_configs/dem_optical_runs.txt',
        'evaluator_module': 'ultra_fast_dem_optical_evaluator',
    },
    'dem_sar': {
        'config_file': 'run_configs/dem_sar_runs.txt',
        'evaluator_module': 'ultra_fast_dem_sar_evaluator',
    },
    'dem_thermal': {
        'config_file': 'run_configs/dem_thermal_runs.txt',
        'evaluator_module': 'ultra_fast_dem_thermal_evaluator',
    },
    'landsat6b': {
        'config_file': 'run_configs/landsat6b_runs.txt',
        'evaluator_module': 'ultra_fast_landsat6b_evaluator',
    }
}

def parse_config_file(config_path):
    """Parse run configuration file."""
    runs = []
    if not os.path.exists(config_path):
        return runs
    
    with open(config_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split('|')
                if len(parts) == 2:
                    run_name, checkpoint_path = parts
                    if os.path.exists(checkpoint_path):
                        runs.append((run_name.strip(), checkpoint_path.strip()))
    return runs

def run_evaluation_direct(evaluator_module, checkpoint_path, huc_list_path, run_name, variant):
    """Run evaluation with minimal logging."""
    try:
        print(f"  Running {variant}/{run_name}...", end=" ", flush=True)
        
        # Import the evaluator module
        evaluator = __import__(evaluator_module)
        
        # Set arguments
        original_argv = sys.argv.copy()
        sys.argv = [
            evaluator_module + '.py',
            '--checkpoint', checkpoint_path,
            '--huc-list', huc_list_path
        ]
        
        start_time = time.time()
        
        try:
            evaluator.main()
            duration = time.time() - start_time
            print(f"✅ {duration:.1f}s")
            return True, duration, "Success"
            
        except SystemExit as e:
            duration = time.time() - start_time
            if e.code == 0:
                print(f"✅ {duration:.1f}s")
                return True, duration, "Success"
            else:
                print(f"❌ Exit {e.code}")
                return False, duration, f"Exit code: {e.code}"
        
        finally:
            sys.argv = original_argv
            
    except Exception as e:
        duration = time.time() - start_time if 'start_time' in locals() else 0
        print(f"❌ Error: {str(e)[:50]}")
        return False, duration, str(e)

def main():
    parser = argparse.ArgumentParser(description='Ultra-fast MDMT evaluations (minimal logging)')
    parser.add_argument('--huc-list', default='test_huc_list.txt')
    parser.add_argument('--variant', choices=list(VARIANT_CONFIGS.keys()))
    parser.add_argument('--quick', action='store_true')
    
    args = parser.parse_args()
    
    # Set HUC list path
    huc_list_path = 'test_huc_small.txt' if args.quick else args.huc_list
    
    if not os.path.exists(huc_list_path):
        print(f"❌ HUC list file not found: {huc_list_path}")
        sys.exit(1)
    
    # Count HUCs
    with open(huc_list_path, 'r') as f:
        huc_count = sum(1 for line in f if line.strip() and not line.startswith('#'))
    
    print(f"📊 Processing {huc_count} HUCs")
    
    start_time = time.time()
    
    # Determine variants to run
    variants_to_run = [args.variant] if args.variant else list(VARIANT_CONFIGS.keys())
    
    print(f"🎯 Running {len(variants_to_run)} variants: {', '.join(variants_to_run)}")
    
    total_successful = 0
    total_runs = 0
    
    # Run evaluations
    for variant in variants_to_run:
        config = VARIANT_CONFIGS[variant]
        print(f"\n🚀 {variant.upper()}:")
        
        runs = parse_config_file(config['config_file'])
        if not runs:
            print(f"  ⚠️  No valid runs found")
            continue
        
        variant_successful = 0
        for run_name, checkpoint_path in runs:
            success, duration, message = run_evaluation_direct(
                config['evaluator_module'], 
                checkpoint_path, 
                huc_list_path, 
                run_name, 
                variant
            )
            
            if success:
                variant_successful += 1
            total_runs += 1
            time.sleep(1)  # Brief pause
        
        total_successful += variant_successful
        print(f"  📈 {variant_successful}/{len(runs)} successful")
    
    # Final summary
    total_time = time.time() - start_time
    print(f"\n🎊 COMPLETED!")
    print(f"📊 Results: {total_successful}/{total_runs} successful ({total_successful/total_runs*100:.1f}%)")
    print(f"⏱️  Total time: {total_time/60:.1f} minutes")
    print(f"📁 Results in: ultra_fast_results/")
    
    # Quick file count
    result_dir = "/u/nathanj/national_ml/experiments/evaluation/ultra_fast_results"
    if os.path.exists(result_dir):
        csv_files = len([f for f in os.listdir(result_dir) if f.endswith('.csv')])
        print(f"📄 CSV files created: {csv_files}")

if __name__ == "__main__":
    main()