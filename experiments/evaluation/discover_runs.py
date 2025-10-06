#!/usr/bin/env python3
"""
Discover and generate run configurations for MDMT experiments.
This script scans the lightning_logs directory and creates run configuration files.
"""

import os
import glob
import re
from pathlib import Path
from datetime import datetime

def find_experiment_runs(lightning_logs_dir: str) -> dict:
    """
    Discover all experiment runs and their best checkpoints.
    """
    experiments = {
        'alphaearth': {'pattern': '*alphaearth_only*', 'prefix': 'mdmt-alphaearth-only'},
        'dem_thermal': {'pattern': '*dem_thermal*', 'prefix': 'mdmt-dem-thermal'},
        'dem_sar': {'pattern': '*dem_sar*', 'prefix': 'mdmt-dem-sar'},
        'dem_optical': {'pattern': '*dem_optical*', 'prefix': 'mdmt-dem-optical'},
        'dem_alphaearth': {'pattern': '*dem_alphaearth*', 'prefix': 'mdmt-dem-alphaearth'},
        'landsat6b': {'pattern': '*ls6b*', 'prefix': 'mdmt'}
    }
    
    discovered_runs = {}
    
    for exp_name, config in experiments.items():
        print(f"\n🔍 Scanning for {exp_name} experiments...")
        
        # Find all matching directories
        pattern = os.path.join(lightning_logs_dir, config['pattern'])
        exp_dirs = glob.glob(pattern)
        exp_dirs.sort()
        
        runs = []
        
        for exp_dir in exp_dirs:
            if not os.path.isdir(exp_dir):
                continue
            
            # Extract run number from directory name
            run_match = re.search(r'run(\d+)', os.path.basename(exp_dir))
            if run_match:
                run_num = run_match.group(1)
            else:
                # If no run number found, use directory name
                run_num = os.path.basename(exp_dir).split('_')[-1]
            
            # Find best checkpoint (lowest val_loss)
            checkpoints_dir = os.path.join(exp_dir, 'checkpoints')
            if not os.path.exists(checkpoints_dir):
                print(f"  ⚠️  No checkpoints directory: {exp_dir}")
                continue
            
            # Find all checkpoints
            checkpoint_pattern = os.path.join(checkpoints_dir, f"{config['prefix']}*.ckpt")
            checkpoints = glob.glob(checkpoint_pattern)
            
            if not checkpoints:
                print(f"  ⚠️  No checkpoints found: {checkpoints_dir}")
                continue
            
            # Find checkpoint with lowest val_loss
            best_checkpoint = None
            best_val_loss = float('inf')
            
            for ckpt in checkpoints:
                # Extract val_loss from filename
                val_loss_match = re.search(r'val_loss=([0-9]+\.?[0-9]*)', os.path.basename(ckpt))
                if val_loss_match:
                    try:
                        val_loss = float(val_loss_match.group(1))
                    except ValueError as e:
                        print(f"Warning: Could not parse val_loss from {ckpt}: {e}")
                        continue
                    if val_loss < best_val_loss:
                        best_val_loss = val_loss
                        best_checkpoint = ckpt
            
            if best_checkpoint:
                runs.append({
                    'run_name': f"run{run_num}",
                    'checkpoint': best_checkpoint,
                    'val_loss': best_val_loss,
                    'exp_dir': exp_dir
                })
                print(f"  ✅ Found run{run_num}: val_loss={best_val_loss:.4f}")
            else:
                print(f"  ⚠️  No valid checkpoint: {exp_dir}")
        
        discovered_runs[exp_name] = runs
        print(f"  📊 Total {exp_name} runs: {len(runs)}")
    
    return discovered_runs


def generate_run_config_files(discovered_runs: dict, output_dir: str):
    """
    Generate run configuration files for each experiment.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    for exp_name, runs in discovered_runs.items():
        if not runs:
            print(f"⚠️  No runs found for {exp_name}")
            continue
        
        config_file = os.path.join(output_dir, f"{exp_name}_runs.txt")
        
        with open(config_file, 'w') as f:
            f.write(f"# {exp_name.upper().replace('_', ' + ')} Experiment Runs\n")
            f.write(f"# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("# Format: run_name|checkpoint_path\n")
            
            # Sort runs by run number
            runs_sorted = sorted(runs, key=lambda x: int(x['run_name'].replace('run', '')))
            
            for run in runs_sorted:
                f.write(f"{run['run_name']}|{run['checkpoint']}\n")
        
        print(f"✅ Generated {config_file} ({len(runs)} runs)")


def main():
    lightning_logs_dir = "/u/nathanj/national_ml/experiments/training/lightning_logs"
    output_dir = "/u/nathanj/national_ml/experiments/evaluation/run_configs"
    
    print("🔍 MDMT Run Configuration Generator")
    print("="*50)
    print(f"Scanning: {lightning_logs_dir}")
    print(f"Output:   {output_dir}")
    
    if not os.path.exists(lightning_logs_dir):
        print(f"❌ Lightning logs directory not found: {lightning_logs_dir}")
        return
    
    # Discover all runs
    discovered_runs = find_experiment_runs(lightning_logs_dir)
    
    # Generate configuration files
    print(f"\n📝 Generating configuration files...")
    generate_run_config_files(discovered_runs, output_dir)
    
    # Summary
    print(f"\n📊 SUMMARY:")
    total_runs = sum(len(runs) for runs in discovered_runs.values())
    print(f"Total experiments: {len(discovered_runs)}")
    print(f"Total runs discovered: {total_runs}")
    
    for exp_name, runs in discovered_runs.items():
        if runs:
            print(f"  {exp_name}: {len(runs)} runs")
    
    print(f"\n🎉 Configuration files ready in: {output_dir}")
    print("You can now run batch evaluation with:")
    print("  python experiments/evaluation/batch_mdmt_evaluator.py")


if __name__ == "__main__":
    main()