#!/usr/bin/env python3
"""
Transfer/Robustness Analysis - Main Script
Evaluate All Modalities model across terrain complexity and optical availability.
"""

import os
import sys
import json
import numpy as np
import torch
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import argparse

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.append(str(project_root))

# Import from DEM sanity analysis (reuse evaluation framework)
sys.path.append(str(Path(__file__).parent.parent / "dem_sanity"))

from config import config
from terrain_classifier import TerrainClassifier
from optical_masker import OpticalMasker

# Import model and evaluation utilities
try:
    from experiments.models.mdmt_v1 import MultimodalMultitaskModel_V1
    from experiments.data.patchDataLoader_v1 import MultimodalPatchDataset_V1
except ImportError as e:
    print(f"⚠️  Import error: {e}")
    print("Make sure you're running from the correct environment and path")

class TransferRobustnessAnalyzer:
    """Main analyzer for transfer and robustness analysis."""
    
    def __init__(self, config_obj=None):
        """Initialize analyzer with configuration."""
        self.config = config_obj or config
        self.terrain_classifier = TerrainClassifier()
        self.optical_masker = OpticalMasker()
        
        # Results storage
        self.transfer_results = {}
        self.robustness_results = {}
        
        print("🔬 TRANSFER/ROBUSTNESS ANALYZER INITIALIZED")
        self.config.summary()
    
    def load_model(self) -> torch.nn.Module:
        """Load the All Modalities model."""
        print(f"\n📦 Loading All Modalities model...")
        
        if not os.path.exists(self.config.model_checkpoint):
            raise FileNotFoundError(f"Model checkpoint not found: {self.config.model_checkpoint}")
        
        # Load checkpoint
        checkpoint = torch.load(self.config.model_checkpoint, map_location='cpu')
        print(f"✅ Checkpoint loaded: {os.path.basename(self.config.model_checkpoint)}")
        
        # Initialize model (need to determine exact architecture)
        # This might need adjustment based on the actual model architecture
        try:
            model = MultimodalMultitaskModel_V1(
                n_classes_task1=1,  # Water segmentation
                n_classes_task2=8,  # D8 flow direction
                base_channels=64,
                alphaearth_channels=64  # Adjust based on actual model
            )
            
            # Load state dict
            if 'state_dict' in checkpoint:
                model.load_state_dict(checkpoint['state_dict'])
            else:
                model.load_state_dict(checkpoint)
                
            model.eval()
            model.to(self.config.device)
            
            print(f"✅ Model loaded successfully")
            print(f"   Model parameters: {sum(p.numel() for p in model.parameters()):,}")
            
            return model
            
        except Exception as e:
            print(f"❌ Error loading model: {e}")
            print("⚠️  This might require adjusting the model architecture parameters")
            raise
    
    def load_huc_list(self) -> List[str]:
        """Load HUC codes from file."""
        huc_list = []
        with open(self.config.huc_list_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    huc_list.append(line)
        
        print(f"📋 Loaded {len(huc_list)} HUCs: {huc_list}")
        return huc_list
    
    def load_huc_patches(self, huc_code: str) -> List[Dict]:
        """Load patches for a specific HUC."""
        huc_dir = os.path.join(self.config.test_data_path, huc_code)
        
        if not os.path.exists(huc_dir):
            print(f"⚠️  HUC directory not found: {huc_dir}")
            return []
        
        # Find patch files
        patch_files = list(Path(huc_dir).glob("patch_*.npz"))
        
        if not patch_files:
            print(f"⚠️  No patch files found in {huc_dir}")
            return []
        
        # Limit patches for quick testing
        if self.config.limit_patches_per_huc:
            patch_files = patch_files[:self.config.limit_patches_per_huc]
        
        patches = []
        print(f"📊 Loading {len(patch_files)} patches from HUC {huc_code}...")
        
        for patch_file in patch_files:
            try:
                with np.load(patch_file) as data:
                    patch_data = {key: data[key] for key in data.keys()}
                    patch_data['file_path'] = str(patch_file)
                    patches.append(patch_data)
            except Exception as e:
                print(f"  ⚠️  Error loading {patch_file}: {e}")
                continue
        
        print(f"  ✅ Loaded {len(patches)} patches")
        return patches
    
    def classify_patches_by_terrain(self, patches: List[Dict]) -> Dict[str, List[Dict]]:
        """Classify patches by terrain relief."""
        terrain_bins = {"low_relief": [], "medium_relief": [], "high_relief": []}
        
        print(f"🏔️  Classifying {len(patches)} patches by terrain...")
        
        for patch in patches:
            if 'dem' not in patch:
                print(f"⚠️  Patch missing DEM data: {patch.get('file_path', 'unknown')}")
                continue
            
            relief, terrain_class = self.terrain_classifier.classify_patch(patch['dem'])
            patch['relief'] = relief
            patch['terrain_class'] = terrain_class
            terrain_bins[terrain_class].append(patch)
        
        # Print terrain distribution
        print(f"Terrain distribution:")
        for terrain, patches_list in terrain_bins.items():
            print(f"  {terrain:>12}: {len(patches_list):4d} patches")
        
        return terrain_bins
    
    def evaluate_patches(self, model: torch.nn.Module, patches: List[Dict], 
                        apply_cloud_mask: bool = False) -> Dict:
        """Evaluate model on a list of patches."""
        if not patches:
            return {"n_patches": 0, "water_iou": 0.0, "water_dice": 0.0}
        
        print(f"🔍 Evaluating {len(patches)} patches (clouds={'yes' if apply_cloud_mask else 'no'})...")
        
        water_ious = []
        water_dices = []
        
        with torch.no_grad():
            for i, patch in enumerate(patches):
                try:
                    # Prepare input data
                    batch_data = self.prepare_model_input(patch)
                    
                    # Apply cloud masking if requested
                    if apply_cloud_mask and 'optical' in batch_data:
                        batch_data, _ = self.optical_masker.mask_batch_data(batch_data, seed=i)
                    
                    # Move to device
                    for key in batch_data:
                        if isinstance(batch_data[key], torch.Tensor):
                            batch_data[key] = batch_data[key].to(self.config.device)
                    
                    # Forward pass
                    outputs = model(batch_data)
                    
                    # Calculate metrics (this would need to be implemented based on model output format)
                    water_iou, water_dice = self.calculate_patch_metrics(outputs, batch_data)
                    
                    water_ious.append(water_iou)
                    water_dices.append(water_dice)
                    
                    if (i + 1) % 10 == 0:
                        print(f"  Processed {i + 1}/{len(patches)} patches")
                        
                except Exception as e:
                    print(f"  ⚠️  Error evaluating patch {i}: {e}")
                    continue
        
        # Calculate aggregate metrics
        results = {
            "n_patches": len(patches),
            "n_evaluated": len(water_ious),
            "water_iou": float(np.mean(water_ious)) if water_ious else 0.0,
            "water_dice": float(np.mean(water_dices)) if water_dices else 0.0,
            "water_iou_std": float(np.std(water_ious)) if water_ious else 0.0,
            "water_dice_std": float(np.std(water_dices)) if water_dices else 0.0
        }
        
        print(f"  📈 Results: IoU={results['water_iou']:.3f}, Dice={results['water_dice']:.3f}")
        
        return results
    
    def prepare_model_input(self, patch: Dict) -> Dict:
        """Prepare patch data for model input."""
        # This is a placeholder - needs to be implemented based on actual model requirements
        # The exact format depends on how the All Modalities model expects its input
        
        batch_data = {}
        
        # Convert numpy arrays to torch tensors and add batch dimension
        for key in ['dem', 'optical', 'thermal', 'sar', 'alphaearth']:
            if key in patch:
                data = torch.from_numpy(patch[key]).float()
                if data.ndim == 2:  # (H, W) -> (1, 1, H, W)
                    data = data.unsqueeze(0).unsqueeze(0)
                elif data.ndim == 3:  # (C, H, W) -> (1, C, H, W)
                    data = data.unsqueeze(0)
                batch_data[key] = data
        
        return batch_data
    
    def calculate_patch_metrics(self, outputs: Dict, batch_data: Dict) -> Tuple[float, float]:
        """Calculate IoU and Dice metrics for a patch."""
        # This is a placeholder - needs to be implemented based on actual model output format
        # Would typically calculate IoU and Dice between predicted and ground truth water masks
        
        # For now, return dummy metrics
        # In real implementation, this would:
        # 1. Extract water segmentation predictions from outputs
        # 2. Extract ground truth water mask from batch_data
        # 3. Calculate IoU and Dice scores
        
        water_iou = np.random.random() * 0.3 + 0.4  # Dummy values for testing
        water_dice = np.random.random() * 0.3 + 0.5
        
        return water_iou, water_dice
    
    def run_transfer_analysis(self, model: torch.nn.Module, huc_list: List[str]):
        """Run 4a: Transfer analysis across terrain bins."""
        print(f"\n🏔️  RUNNING TRANSFER ANALYSIS (4a)")
        print("=" * 50)
        
        all_terrain_results = {"low_relief": [], "medium_relief": [], "high_relief": []}
        
        for huc_code in huc_list:
            print(f"\n📍 Processing HUC {huc_code}...")
            
            # Load patches for this HUC
            patches = self.load_huc_patches(huc_code)
            if not patches:
                continue
            
            # Classify patches by terrain
            terrain_bins = self.classify_patches_by_terrain(patches)
            
            # Evaluate each terrain bin
            huc_results = {}
            for terrain_type, terrain_patches in terrain_bins.items():
                if not terrain_patches:
                    continue
                
                print(f"  🔍 Evaluating {terrain_type} ({len(terrain_patches)} patches)...")
                results = self.evaluate_patches(model, terrain_patches)
                results['huc_code'] = huc_code
                results['terrain_type'] = terrain_type
                
                huc_results[terrain_type] = results
                all_terrain_results[terrain_type].append(results)
        
        # Aggregate results across all HUCs
        transfer_summary = {}
        for terrain_type, results_list in all_terrain_results.items():
            if not results_list:
                continue
            
            # Calculate aggregate metrics
            all_ious = [r['water_iou'] for r in results_list if r['n_evaluated'] > 0]
            all_dices = [r['water_dice'] for r in results_list if r['n_evaluated'] > 0]
            
            transfer_summary[terrain_type] = {
                "n_hucs": len(results_list),
                "total_patches": sum(r['n_patches'] for r in results_list),
                "total_evaluated": sum(r['n_evaluated'] for r in results_list),
                "water_iou_mean": float(np.mean(all_ious)) if all_ious else 0.0,
                "water_dice_mean": float(np.mean(all_dices)) if all_dices else 0.0,
                "water_iou_std": float(np.std(all_ious)) if all_ious else 0.0,
                "water_dice_std": float(np.std(all_dices)) if all_dices else 0.0,
                "huc_results": results_list
            }
        
        self.transfer_results = {
            "analysis_type": "terrain_transfer",
            "timestamp": datetime.now().isoformat(),
            "terrain_summary": transfer_summary,
            "detailed_results": all_terrain_results
        }
        
        self.print_transfer_results()
    
    def run_robustness_analysis(self, model: torch.nn.Module, huc_list: List[str]):
        """Run 4b: Robustness analysis with optical masking."""
        print(f"\n☁️  RUNNING ROBUSTNESS ANALYSIS (4b)")
        print("=" * 50)
        
        normal_results = []
        cloudy_results = []
        
        for huc_code in huc_list[:3]:  # Limit to first 3 HUCs for quick testing
            print(f"\n📍 Processing HUC {huc_code}...")
            
            # Load patches for this HUC
            patches = self.load_huc_patches(huc_code)
            if not patches:
                continue
            
            # Evaluate with normal optical data
            print(f"  🌞 Evaluating with normal optical data...")
            normal_result = self.evaluate_patches(model, patches, apply_cloud_mask=False)
            normal_result['huc_code'] = huc_code
            normal_results.append(normal_result)
            
            # Evaluate with masked optical data
            print(f"  ☁️  Evaluating with {self.config.cloud_coverage*100}% cloud coverage...")
            cloudy_result = self.evaluate_patches(model, patches, apply_cloud_mask=True)
            cloudy_result['huc_code'] = huc_code
            cloudy_results.append(cloudy_result)
        
        # Calculate robustness metrics
        robustness_summary = self.calculate_robustness_summary(normal_results, cloudy_results)
        
        self.robustness_results = {
            "analysis_type": "optical_robustness",
            "timestamp": datetime.now().isoformat(),
            "cloud_coverage": self.config.cloud_coverage,
            "normal_results": normal_results,
            "cloudy_results": cloudy_results,
            "robustness_summary": robustness_summary
        }
        
        self.print_robustness_results()
    
    def calculate_robustness_summary(self, normal_results: List[Dict], 
                                   cloudy_results: List[Dict]) -> Dict:
        """Calculate aggregate robustness metrics."""
        if len(normal_results) != len(cloudy_results):
            raise ValueError("Mismatch in number of normal vs cloudy results")
        
        iou_deltas = []
        dice_deltas = []
        
        for normal, cloudy in zip(normal_results, cloudy_results):
            if normal['n_evaluated'] > 0 and cloudy['n_evaluated'] > 0:
                iou_delta = normal['water_iou'] - cloudy['water_iou']
                dice_delta = normal['water_dice'] - cloudy['water_dice']
                
                iou_deltas.append(iou_delta)
                dice_deltas.append(dice_delta)
        
        # Aggregate normal performance
        normal_ious = [r['water_iou'] for r in normal_results if r['n_evaluated'] > 0]
        normal_dices = [r['water_dice'] for r in normal_results if r['n_evaluated'] > 0]
        
        # Aggregate cloudy performance
        cloudy_ious = [r['water_iou'] for r in cloudy_results if r['n_evaluated'] > 0]
        cloudy_dices = [r['water_dice'] for r in cloudy_results if r['n_evaluated'] > 0]
        
        summary = {
            "n_hucs": len(normal_results),
            "total_patches_normal": sum(r['n_patches'] for r in normal_results),
            "total_patches_cloudy": sum(r['n_patches'] for r in cloudy_results),
            "normal_performance": {
                "water_iou_mean": float(np.mean(normal_ious)) if normal_ious else 0.0,
                "water_dice_mean": float(np.mean(normal_dices)) if normal_dices else 0.0
            },
            "cloudy_performance": {
                "water_iou_mean": float(np.mean(cloudy_ious)) if cloudy_ious else 0.0,
                "water_dice_mean": float(np.mean(cloudy_dices)) if cloudy_dices else 0.0
            },
            "robustness_metrics": {
                "iou_delta_mean": float(np.mean(iou_deltas)) if iou_deltas else 0.0,
                "dice_delta_mean": float(np.mean(dice_deltas)) if dice_deltas else 0.0,
                "iou_delta_std": float(np.std(iou_deltas)) if iou_deltas else 0.0,
                "dice_delta_std": float(np.std(dice_deltas)) if dice_deltas else 0.0
            }
        }
        
        return summary
    
    def print_transfer_results(self):
        """Print transfer analysis results."""
        print(f"\n📊 TRANSFER ANALYSIS RESULTS")
        print("=" * 40)
        
        for terrain_type, summary in self.transfer_results['terrain_summary'].items():
            print(f"\n{terrain_type.upper()}:")
            print(f"  HUCs: {summary['n_hucs']}")
            print(f"  Patches: {summary['total_patches']} ({summary['total_evaluated']} evaluated)")
            print(f"  Water IoU: {summary['water_iou_mean']:.3f} ± {summary['water_iou_std']:.3f}")
            print(f"  Water Dice: {summary['water_dice_mean']:.3f} ± {summary['water_dice_std']:.3f}")
    
    def print_robustness_results(self):
        """Print robustness analysis results."""
        print(f"\n📊 ROBUSTNESS ANALYSIS RESULTS")
        print("=" * 40)
        
        summary = self.robustness_results['robustness_summary']
        
        print(f"Normal performance:")
        print(f"  Water IoU: {summary['normal_performance']['water_iou_mean']:.3f}")
        print(f"  Water Dice: {summary['normal_performance']['water_dice_mean']:.3f}")
        
        print(f"\nCloudy performance ({self.config.cloud_coverage*100}% coverage):")
        print(f"  Water IoU: {summary['cloudy_performance']['water_iou_mean']:.3f}")
        print(f"  Water Dice: {summary['cloudy_performance']['water_dice_mean']:.3f}")
        
        print(f"\nRobustness (performance loss):")
        print(f"  IoU delta: {summary['robustness_metrics']['iou_delta_mean']:.3f} ± {summary['robustness_metrics']['iou_delta_std']:.3f}")
        print(f"  Dice delta: {summary['robustness_metrics']['dice_delta_mean']:.3f} ± {summary['robustness_metrics']['dice_delta_std']:.3f}")
    
    def save_results(self):
        """Save results to JSON files."""
        # Save transfer results
        with open(self.config.transfer_results_file, 'w') as f:
            json.dump(self.transfer_results, f, indent=2)
        print(f"💾 Transfer results saved: {self.config.transfer_results_file}")
        
        # Save robustness results
        with open(self.config.robustness_results_file, 'w') as f:
            json.dump(self.robustness_results, f, indent=2)
        print(f"💾 Robustness results saved: {self.config.robustness_results_file}")
    
    def run_full_analysis(self):
        """Run complete transfer and robustness analysis."""
        print(f"🚀 STARTING TRANSFER/ROBUSTNESS ANALYSIS")
        print("=" * 60)
        
        # Validate configuration
        self.config.validate_paths()
        
        # Load model
        model = self.load_model()
        
        # Load HUC list
        huc_list = self.load_huc_list()
        
        # Run transfer analysis
        self.run_transfer_analysis(model, huc_list)
        
        # Run robustness analysis
        self.run_robustness_analysis(model, huc_list)
        
        # Save results
        self.save_results()
        
        print(f"\n✅ ANALYSIS COMPLETE!")
        print(f"Results saved to: {self.config.results_dir}")

def main():
    """Main function with command line interface."""
    parser = argparse.ArgumentParser(description="Transfer/Robustness Analysis")
    parser.add_argument("--quick", action="store_true", help="Quick testing mode (limit patches)")
    parser.add_argument("--device", type=str, default="cuda", choices=["cpu", "cuda"], 
                       help="Device to use for evaluation")
    
    args = parser.parse_args()
    
    # Update config based on arguments
    if args.quick:
        config.limit_patches_per_huc = 10
        print("⚡ Quick testing mode enabled (10 patches per HUC)")
    
    config.device = args.device
    
    # Run analysis
    analyzer = TransferRobustnessAnalyzer(config)
    analyzer.run_full_analysis()

if __name__ == "__main__":
    main()