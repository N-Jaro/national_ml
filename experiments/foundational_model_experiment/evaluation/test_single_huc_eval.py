#!/usr/bin/env python3
"""
Test single HUC evaluation to verify CSV output is generated correctly.
"""

import subprocess
import os
import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_single_huc_evaluation():
    """Test evaluation on a single HUC to verify CSV output."""
    
    # Get checkpoint from config
    config_file = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/run_config/prithvi_runs.txt"
    checkpoint_path = None
    
    with open(config_file, 'r') as f:
        for line in f:
            if line.startswith('run1|'):
                checkpoint_path = line.split('|', 1)[1].strip()
                break
    
    if not checkpoint_path:
        logger.error("run1 not found in config")
        return False
    
    # Create a small test HUC list with just one HUC
    test_huc_file = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/test_single_huc.txt"
    with open(test_huc_file, 'w') as f:
        f.write("# Test single HUC\n")
        f.write("03030005\n")  # HUC that we know exists
    
    # Run evaluation
    eval_script = "/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/ultra_fast_prithvi_evaluator.py"
    
    cmd = [
        "bash", "-c",
        f"source /u/nathanj/miniconda3/etc/profile.d/conda.sh && "
        f"conda activate terratorch_env && "
        f"python {eval_script} "
        f"--checkpoint '{checkpoint_path}' "
        f"--data_path '/u/nathanj/national_ml/data/processed/patch_dataset' "
        f"--output_dir '/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/test_results' "
        f"--batch_size 16 "
        f"--device cpu "
        f"--huc_file '{test_huc_file}' "
        f"--run_name 'single_huc_test' "
        f"--wandb_mode disabled"
    ]
    
    logger.info("Running single HUC evaluation test...")
    logger.info(f"Using HUC: 03030005")
    
    try:
        os.makedirs("/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/test_results", exist_ok=True)
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # 10 min timeout
        
        if result.returncode == 0:
            logger.info("✓ Single HUC evaluation completed successfully!")
            
            # Check if CSV was created
            csv_files = list(Path("/u/nathanj/national_ml/experiments/foundational_model_experiment/evaluation/test_results").glob("prithvi_single_huc_test_*.csv"))
            
            if csv_files:
                csv_file = csv_files[0]
                logger.info(f"✓ CSV file created: {csv_file.name}")
                
                # Read and display first few lines
                with open(csv_file, 'r') as f:
                    lines = f.readlines()
                    logger.info(f"CSV has {len(lines)} lines (including header)")
                    logger.info("First few lines:")
                    for i, line in enumerate(lines[:3]):
                        logger.info(f"  {i}: {line.strip()}")
                
                return True
            else:
                logger.error("✗ No CSV file was created")
                return False
        else:
            logger.error("✗ Single HUC evaluation failed!")
            logger.error(f"STDERR: {result.stderr[-1000:]}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("✗ Evaluation timed out")
        return False
    except Exception as e:
        logger.error(f"✗ Error: {e}")
        return False

def main():
    logger.info("Testing Single HUC Prithvi Evaluation")
    logger.info("=" * 50)
    
    success = test_single_huc_evaluation()
    
    logger.info("\n" + "=" * 50)
    if success:
        logger.info("🎉 Single HUC evaluation test PASSED!")
        logger.info("CSV output format verified. Ready for full evaluation!")
        return 0
    else:
        logger.error("❌ Single HUC evaluation test FAILED!")
        return 1

if __name__ == "__main__":
    exit(main())