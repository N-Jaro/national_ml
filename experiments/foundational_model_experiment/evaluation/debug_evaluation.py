#!/usr/bin/env python3
"""
Debug the evaluation to see where it's failing.
"""

import subprocess
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_evaluation():
    """Run evaluation with more verbose output."""
    
    config_file = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config/prithvi_runs.txt"
    checkpoint_path = None
    
    with open(config_file, 'r') as f:
        for line in f:
            if line.startswith('run1|'):
                checkpoint_path = line.split('|', 1)[1].strip()
                break
    
    # Create test HUC file
    test_huc_file = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/test_debug_huc.txt"
    with open(test_huc_file, 'w') as f:
        f.write("# Debug single HUC\n")
        f.write("03030005\n")
    
    eval_script = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_prithvi_evaluator.py"
    
    cmd = [
        "bash", "-c",
        f"source /u/nathanj/miniconda3/etc/profile.d/conda.sh && "
        f"conda activate terratorch_env && "
        f"python {eval_script} "
        f"--checkpoint '{checkpoint_path}' "
        f"--data_path '/u/nathanj/national_ml/data/processed/patch_dataset' "
        f"--output_dir '/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/test_results' "
        f"--batch_size 8 "
        f"--device cpu "
        f"--huc_file '{test_huc_file}' "
        f"--run_name 'debug_test' "
        f"--wandb_mode disabled"
    ]
    
    logger.info("Running debug evaluation...")
    
    try:
        # Run with live output
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        
        # Print output in real time
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())
        
        # Get any remaining output
        stdout, stderr = process.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print("STDERR:", stderr)
        
        return process.returncode == 0
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return False

if __name__ == "__main__":
    debug_evaluation()