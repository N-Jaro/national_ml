#!/usr/bin/env python3
"""
Configuration for Transfer/Robustness Analysis
"""

import os
from pathlib import Path
from datetime import datetime

class TransferRobustnessConfig:
    """Configuration parameters for transfer/robustness analysis."""
    
    def __init__(self):
        # Base paths
        self.base_dir = Path(__file__).parent
        self.project_root = self.base_dir.parent.parent.parent.parent
        
        # Create unique results directory with datetime stamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_id = f"transfer_robustness_{timestamp}"
        
        # Model configuration - Multiple checkpoints for focused comparisons
        self.checkpoints = {
            # 4a) Transfer Analysis: AE-only vs DEM+AE (isolates DEM contribution)
            "alphaearth_only": "/u/nathanj/national_ml/experiments/training/lightning_logs/alphaearth_only_20251002_115437_run2/checkpoints/mdmt-alphaearth-only-epoch=28-val_loss=0.8024.ckpt",
            "dem_alphaearth": "/u/nathanj/national_ml/experiments/training/lightning_logs/dem_alphaearth_20251006_015003_run10/checkpoints/mdmt-dem-alphaearth-epoch=40-val_loss=0.4377.ckpt",
            
            # 4b) Robustness Analysis: All+AE clean vs masked optical
            "all_modalities": "/u/nathanj/national_ml/outputs/models/all_modalities_alphaearth/all_modalities_alphaearth_task3_seed456_epoch=09_val_loss=0.5666.ckpt"
        }
        
        # Default checkpoint for backward compatibility
        self.model_checkpoint = self.checkpoints["all_modalities"]
        
        # Data paths - Use local processed data with all modalities  
        self.test_data_path = "/u/nathanj/national_ml/data/processed/patch_dataset"
        self.huc_list_file = "/u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/test_huc_representative.txt"
        
        # Terrain classification thresholds (from terrain relief analysis)
        self.low_relief_threshold = 35.0   # meters, 33rd percentile
        self.high_relief_threshold = 113.9 # meters, 67th percentile
        
        # Cloud simulation parameters
        self.cloud_coverage = 0.3  # 30% cloud coverage
        self.cloud_fill_value = 0.0  # Value to use for masked pixels
        
        # Analysis parameters
        self.limit_patches_per_huc = None  # For quick testing, None for full analysis
        self.device = "cuda"
        
        # Output configuration - separate directory for each run
        self.results_base_dir = self.base_dir / "results"
        self.results_dir = self.results_base_dir / self.run_id
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        self.transfer_results_file = self.results_dir / "transfer_results.json"
        self.robustness_results_file = self.results_dir / "robustness_results.json"
        self.visualization_file = self.results_dir / "transfer_robustness_visualization.png"
        
        # Expected data modalities for All Modalities model
        self.expected_modalities = ['dem', 'optical', 'thermal', 'sar', 'alphaearth']
        
        # Optical bands configuration
        self.optical_bands = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7']  # Landsat bands
        self.optical_channels = 6
        
    def validate_paths(self):
        """Validate that required paths exist."""
        missing_paths = []
        
        if not os.path.exists(self.model_checkpoint):
            missing_paths.append(f"Model checkpoint: {self.model_checkpoint}")
            
        if not os.path.exists(self.test_data_path):
            missing_paths.append(f"Test data path: {self.test_data_path}")
            
        if not os.path.exists(self.huc_list_file):
            missing_paths.append(f"HUC list file: {self.huc_list_file}")
            
        if missing_paths:
            raise FileNotFoundError(f"Missing required paths: {missing_paths}")
            
        return True
    
    def set_custom_run_id(self, custom_id: str):
        """Set a custom run ID for the results directory."""
        self.run_id = custom_id
        # Update results directory paths
        self.results_dir = self.results_base_dir / self.run_id
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Update file paths
        self.transfer_results_file = self.results_dir / "transfer_results.json"
        self.robustness_results_file = self.results_dir / "robustness_results.json"
        self.visualization_file = self.results_dir / "transfer_robustness_visualization.png"
    
    def get_terrain_classification(self, relief_value):
        """Classify terrain based on relief value."""
        if relief_value < self.low_relief_threshold:
            return "low_relief"
        elif relief_value > self.high_relief_threshold:
            return "high_relief"
        else:
            return "medium_relief"
    
    def summary(self):
        """Print configuration summary."""
        print("🔧 TRANSFER/ROBUSTNESS ANALYSIS CONFIGURATION")
        print("=" * 50)
        print(f"Run ID: {self.run_id}")
        print(f"Model checkpoint: {os.path.basename(self.model_checkpoint)}")
        print(f"Test data path: {self.test_data_path}")
        print(f"HUC list: {os.path.basename(self.huc_list_file)}")
        print(f"Terrain thresholds: Low <{self.low_relief_threshold}m, High >{self.high_relief_threshold}m")
        print(f"Cloud coverage: {self.cloud_coverage*100}%")
        print(f"Results directory: {self.results_dir}")
        print(f"Device: {self.device}")
        if self.limit_patches_per_huc:
            print(f"⚡ Quick testing mode: {self.limit_patches_per_huc} patches per HUC")
        else:
            print("🔍 Full analysis mode: All patches")

# Global config instance
config = TransferRobustnessConfig()