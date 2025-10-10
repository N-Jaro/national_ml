#!/usr/bin/env python3
"""
CLI Script for Training SATLAS Pretrained Foundation Model
Run with: python run_satlas_pretrained.py --config configs/satlas_pretrained_config.yaml
"""

import os
import sys
import argparse
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))

from training.train_satlas_pretrained import main as train_main

def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Train SATLAS Pretrained Foundation Model for Water Segmentation",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Configuration
    parser.add_argument(
        "--config", 
        type=str, 
        default="configs/satlas_pretrained_config.yaml",
        help="Path to config file"
    )
    
    # Hardware
    parser.add_argument("--gpus", type=int, default=1, help="Number of GPUs")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Validate config file exists
    if not os.path.exists(args.config):
        print(f"Error: Config file {args.config} not found!")
        print("Available configs:")
        config_dir = Path(__file__).parent.parent / "configs"
        if config_dir.exists():
            for config_file in config_dir.glob("*.yaml"):
                print(f"  {config_file}")
        return 1
    
    # Set up sys.argv for the training script
    sys.argv = [
        "train_satlas_pretrained.py",
        "--config", args.config,
        "--gpus", str(args.gpus)
    ]
    
    if args.test:
        sys.argv.append("--test")
    
    print(f"Starting SATLAS pretrained training with config: {args.config}")
    print(f"Using GPUs: {args.gpus}")
    print("Note: This uses actual SATLAS pretrained weights via TerraTorch")
    print("Make sure you're using terratorch_env environment!")
    
    # Run training
    try:
        train_main()
        return 0
    except Exception as e:
        print(f"Training failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())