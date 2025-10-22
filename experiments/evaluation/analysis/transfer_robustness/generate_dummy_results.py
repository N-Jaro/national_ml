#!/usr/bin/env python3
"""
Generate Believable Dummy Results for Transfer/Robustness Analysis

This script generates synthetic but realistic results that align with research hypotheses:

4a) Transfer Analysis Hypothesis:
   - DEM should improve performance on high-relief terrain
   - AE-only should struggle more with complex topography
   - Effect should be stronger for flow direction than hydro segmentation

4b) Robustness Analysis Hypothesis:
   - All+AE model relies on optical but has some robustness
   - Moderate degradation (10-20%) under 30% cloud coverage
   - Flow direction more affected than segmentation
"""

import json
import numpy as np
import random
from pathlib import Path
from datetime import datetime
from collections import defaultdict

class RealisticResultsGenerator:
    """Generate realistic dummy results based on research hypotheses"""
    
    def __init__(self, seed=42):
        np.random.seed(seed)
        random.seed(seed)
        
        # Realistic baseline performance ranges (based on typical deep learning results)
        self.baseline_ranges = {
            'hydro_iou': (0.15, 0.45),      # Water segmentation IoU
            'hydro_f1': (0.25, 0.60),       # Water segmentation F1  
            'flow_accuracy': (0.04, 0.12),  # D8 flow direction accuracy (8-class is hard)
        }
        
        # Terrain relief categories
        self.terrain_types = ['low_relief', 'medium_relief', 'high_relief']
        
        # HUCs with realistic patch distributions
        self.huc_info = {
            '03030005': {'patches': 50, 'terrain_dist': [0.8, 0.15, 0.05]},  # Mostly low relief
            '04060102': {'patches': 50, 'terrain_dist': [0.6, 0.3, 0.1]},    # Mixed terrain
            '07040006': {'patches': 50, 'terrain_dist': [0.9, 0.08, 0.02]},  # Very low relief
            '08020301': {'patches': 50, 'terrain_dist': [0.85, 0.12, 0.03]}, # Mostly low relief
            '10270104': {'patches': 50, 'terrain_dist': [0.7, 0.25, 0.05]},  # Plains with some variation
        }
    
    def generate_transfer_analysis_results(self):
        """Generate 4a) Transfer Analysis results with realistic patterns"""
        
        results = {
            'alphaearth_only': {terrain: [] for terrain in self.terrain_types},
            'dem_alphaearth': {terrain: [] for terrain in self.terrain_types}
        }
        
        for huc_code, huc_info in self.huc_info.items():
            total_patches = huc_info['patches']
            terrain_dist = huc_info['terrain_dist']
            
            # Distribute patches across terrain types
            patch_counts = [int(total_patches * dist) for dist in terrain_dist]
            if sum(patch_counts) < total_patches:
                patch_counts[0] += total_patches - sum(patch_counts)
            
            for terrain_idx, terrain_type in enumerate(self.terrain_types):
                patch_count = patch_counts[terrain_idx]
                
                if patch_count == 0:
                    continue
                
                # Generate realistic performance for each model
                for _ in range(patch_count):
                    # AE-only performance (baseline with terrain effects)
                    ae_metrics = self._generate_ae_only_metrics(terrain_type)
                    results['alphaearth_only'][terrain_type].append(ae_metrics)
                    
                    # DEM+AE performance (improved, especially on high relief)
                    dem_ae_metrics = self._generate_dem_ae_metrics(terrain_type, ae_metrics)
                    results['dem_alphaearth'][terrain_type].append(dem_ae_metrics)
        
        return results
    
    def _generate_ae_only_metrics(self, terrain_type):
        """Generate AE-only performance with terrain-dependent degradation"""
        
        # Base performance (random within realistic range)
        base_hydro_iou = np.random.uniform(*self.baseline_ranges['hydro_iou'])
        base_hydro_f1 = np.random.uniform(*self.baseline_ranges['hydro_f1'])
        base_flow_acc = np.random.uniform(*self.baseline_ranges['flow_accuracy'])
        
        # Terrain-dependent degradation for AE-only
        terrain_penalties = {
            'low_relief': 1.0,      # No penalty on easy terrain
            'medium_relief': 0.85,  # 15% degradation
            'high_relief': 0.70     # 30% degradation (struggles with complex topography)
        }
        
        penalty = terrain_penalties[terrain_type]
        
        # Apply terrain penalty (with some noise)
        noise_factor = np.random.normal(1.0, 0.1)  # 10% noise
        
        hydro_iou = base_hydro_iou * penalty * noise_factor
        hydro_f1 = base_hydro_f1 * penalty * noise_factor
        
        # Flow direction more affected by terrain complexity
        flow_penalty = penalty * 0.8 if terrain_type == 'high_relief' else penalty
        flow_accuracy = base_flow_acc * flow_penalty * noise_factor
        
        # Ensure realistic bounds
        return {
            'hydro_iou': np.clip(hydro_iou, 0.05, 0.70),
            'hydro_f1': np.clip(hydro_f1, 0.10, 0.80),
            'flow_accuracy': np.clip(flow_accuracy, 0.02, 0.20)
        }
    
    def _generate_dem_ae_metrics(self, terrain_type, ae_baseline):
        """Generate DEM+AE performance with terrain-dependent improvements"""
        
        # DEM improvement factors (more benefit on complex terrain)
        dem_improvements = {
            'low_relief': 1.05,     # 5% improvement (minimal benefit)
            'medium_relief': 1.15,  # 15% improvement  
            'high_relief': 1.35     # 35% improvement (major benefit from elevation data)
        }
        
        improvement = dem_improvements[terrain_type]
        noise_factor = np.random.normal(1.0, 0.08)  # 8% noise
        
        # Apply DEM improvement
        hydro_iou = ae_baseline['hydro_iou'] * improvement * noise_factor
        hydro_f1 = ae_baseline['hydro_f1'] * improvement * noise_factor
        
        # Flow direction benefits more from DEM (topographic flow patterns)
        flow_improvement = improvement * 1.2 if terrain_type != 'low_relief' else improvement
        flow_accuracy = ae_baseline['flow_accuracy'] * flow_improvement * noise_factor
        
        # Ensure realistic bounds
        return {
            'hydro_iou': np.clip(hydro_iou, 0.05, 0.75),
            'hydro_f1': np.clip(hydro_f1, 0.10, 0.85),
            'flow_accuracy': np.clip(flow_accuracy, 0.02, 0.25)
        }
    
    def generate_robustness_analysis_results(self):
        """Generate 4b) Robustness Analysis results with realistic cloud degradation"""
        
        results = {
            'clean': {'metrics': []},
            'masked': {'metrics': []},
            'cloud_coverage': 0.3,
            'total_patches': 0
        }
        
        total_patches = sum(info['patches'] for info in self.huc_info.values())
        results['total_patches'] = total_patches
        
        # Generate patch-level results
        for _ in range(total_patches):
            # Clean performance (good baseline for All+AE model)
            clean_metrics = self._generate_clean_performance()
            results['clean']['metrics'].append(clean_metrics)
            
            # Masked performance (degraded by cloud coverage)
            masked_metrics = self._generate_masked_performance(clean_metrics)
            results['masked']['metrics'].append(masked_metrics)
        
        # Calculate summary statistics
        for condition in ['clean', 'masked']:
            metrics_list = results[condition]['metrics']
            
            # Calculate means and stds
            hydro_ious = [m['hydro_iou'] for m in metrics_list]
            hydro_f1s = [m['hydro_f1'] for m in metrics_list] 
            flow_accs = [m['flow_accuracy'] for m in metrics_list]
            
            results[condition]['summary'] = {
                'hydro_iou_mean': np.mean(hydro_ious),
                'hydro_iou_std': np.std(hydro_ious),
                'hydro_f1_mean': np.mean(hydro_f1s),
                'hydro_f1_std': np.std(hydro_f1s),
                'flow_accuracy_mean': np.mean(flow_accs),
                'flow_accuracy_std': np.std(flow_accs)
            }
        
        return results
    
    def _generate_clean_performance(self):
        """Generate clean (no cloud) performance for All+AE model"""
        
        # All+AE model should perform well (better than individual modalities)
        enhanced_ranges = {
            'hydro_iou': (0.25, 0.55),     # Better than AE-only
            'hydro_f1': (0.35, 0.70),      # Better than AE-only
            'flow_accuracy': (0.06, 0.15), # Better than AE-only
        }
        
        noise_factor = np.random.normal(1.0, 0.12)  # 12% variability
        
        return {
            'hydro_iou': np.clip(np.random.uniform(*enhanced_ranges['hydro_iou']) * noise_factor, 0.10, 0.70),
            'hydro_f1': np.clip(np.random.uniform(*enhanced_ranges['hydro_f1']) * noise_factor, 0.15, 0.80),
            'flow_accuracy': np.clip(np.random.uniform(*enhanced_ranges['flow_accuracy']) * noise_factor, 0.03, 0.20)
        }
    
    def _generate_masked_performance(self, clean_metrics):
        """Generate masked (30% cloud) performance with realistic degradation"""
        
        # Cloud degradation factors (realistic 10-20% degradation)
        base_degradation = np.random.uniform(0.85, 0.92)  # 8-15% degradation
        noise_factor = np.random.normal(1.0, 0.08)        # 8% noise
        
        # Flow direction more sensitive to optical masking
        flow_degradation = base_degradation * 0.95  # Additional 5% penalty
        
        return {
            'hydro_iou': np.clip(clean_metrics['hydro_iou'] * base_degradation * noise_factor, 0.05, 0.65),
            'hydro_f1': np.clip(clean_metrics['hydro_f1'] * base_degradation * noise_factor, 0.10, 0.75),
            'flow_accuracy': np.clip(clean_metrics['flow_accuracy'] * flow_degradation * noise_factor, 0.02, 0.18)
        }
    
    def save_results(self, results_dir):
        """Save realistic dummy results to files"""
        
        results_dir = Path(results_dir)
        results_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate transfer analysis results
        print("🏔️  Generating Transfer Analysis (4a) dummy results...")
        transfer_results = self.generate_transfer_analysis_results()
        
        # Convert to JSON-serializable format
        transfer_json = {}
        for model in transfer_results:
            transfer_json[model] = {}
            for terrain in transfer_results[model]:
                transfer_json[model][terrain] = []
                for metrics in transfer_results[model][terrain]:
                    json_metrics = {k: float(v) for k, v in metrics.items()}
                    transfer_json[model][terrain].append(json_metrics)
        
        transfer_file = results_dir / "transfer_analysis_results.json"
        with open(transfer_file, 'w') as f:
            json.dump(transfer_json, f, indent=2)
        
        print(f"✅ Transfer results saved: {transfer_file}")
        
        # Generate robustness analysis results  
        print("☁️  Generating Robustness Analysis (4b) dummy results...")
        robustness_results = self.generate_robustness_analysis_results()
        
        # Convert to JSON-serializable format
        robustness_json = {}
        for condition in robustness_results:
            if condition in ['clean', 'masked']:
                robustness_json[condition] = {}
                
                if 'metrics' in robustness_results[condition]:
                    robustness_json[condition]['metrics'] = []
                    for metrics in robustness_results[condition]['metrics']:
                        json_metrics = {k: float(v) for k, v in metrics.items()}
                        robustness_json[condition]['metrics'].append(json_metrics)
                
                # Copy other fields
                for key in robustness_results[condition]:
                    if key != 'metrics':
                        robustness_json[condition][key] = robustness_results[condition][key]
            else:
                # Top-level fields (cloud_coverage, total_patches)
                robustness_json[condition] = robustness_results[condition]
        
        robustness_file = results_dir / "robustness_analysis_results.json"
        with open(robustness_file, 'w') as f:
            json.dump(robustness_json, f, indent=2)
        
        print(f"✅ Robustness results saved: {robustness_file}")
        
        # Generate summary report
        summary_file = results_dir / "analysis_summary.md"
        self._generate_summary_report(transfer_results, robustness_results, summary_file)
        print(f"✅ Summary report saved: {summary_file}")
        
        return transfer_file, robustness_file, summary_file
    
    def _generate_summary_report(self, transfer_results, robustness_results, summary_file):
        """Generate a realistic summary report"""
        
        summary = []
        summary.append("# Transfer and Robustness Analysis Results - DUMMY DATA")
        summary.append("\n⚠️  **Note: These are synthetic results generated for demonstration purposes**\n")
        
        summary.append("## Analysis Overview")
        summary.append("This analysis addresses two key research questions:")
        summary.append("- **4a) Transfer**: Does DEM improve terrain generalization? (AE-only vs DEM+AE)")
        summary.append("- **4b) Robustness**: How robust is the model to missing optical data? (clean vs masked)\n")
        
        summary.append("## Key Findings (Synthetic)")
        summary.append("### Transfer Analysis (4a)")
        summary.append("- ✅ **DEM provides significant benefit on complex terrain**")
        summary.append("- ✅ **35% improvement on high-relief terrain vs 5% on low-relief**")
        summary.append("- ✅ **Flow direction benefits more from DEM than segmentation**")
        summary.append("- ✅ **Supports hypothesis that elevation data improves topographic generalization**")
        
        summary.append("\n### Robustness Analysis (4b)")
        summary.append("- ✅ **Model shows good robustness to optical masking**")
        summary.append("- ✅ **~10-15% performance degradation under 30% cloud coverage**")
        summary.append("- ✅ **Flow direction slightly more sensitive than segmentation**")
        summary.append("- ✅ **Confirms multi-modal architecture provides optical redundancy**")
        
        # Calculate some realistic statistics
        clean_mean_iou = np.mean([m['hydro_iou'] for m in robustness_results['clean']['metrics']])
        masked_mean_iou = np.mean([m['hydro_iou'] for m in robustness_results['masked']['metrics']])
        degradation = (1 - masked_mean_iou / clean_mean_iou) * 100
        
        summary.append(f"\n## Performance Metrics (Synthetic)")
        summary.append(f"- **Clean Hydro IoU**: {clean_mean_iou:.3f}")
        summary.append(f"- **Masked Hydro IoU**: {masked_mean_iou:.3f}")
        summary.append(f"- **Performance Degradation**: {degradation:.1f}%")
        
        summary.append(f"\n## Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append("## Status: SYNTHETIC DUMMY DATA FOR TESTING")
        
        with open(summary_file, 'w') as f:
            f.write('\n'.join(summary))

def main():
    """Generate dummy results for testing"""
    
    print("🎲 REALISTIC DUMMY RESULTS GENERATOR")
    print("=" * 50)
    print("Generating synthetic results that align with research hypotheses...")
    print()
    
    # Create generator
    generator = RealisticResultsGenerator(seed=42)
    
    # Create timestamped results directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = f"results/dummy_results_{timestamp}"
    
    # Generate and save results
    transfer_file, robustness_file, summary_file = generator.save_results(results_dir)
    
    print()
    print("🎉 DUMMY RESULTS GENERATED SUCCESSFULLY!")
    print("=" * 50)
    print(f"📁 Results directory: {results_dir}")
    print(f"📊 Transfer analysis: {transfer_file.name}")
    print(f"☁️  Robustness analysis: {robustness_file.name}")
    print(f"📄 Summary report: {summary_file.name}")
    print()
    print("💡 These results demonstrate:")
    print("   • DEM improves performance on complex terrain")
    print("   • Multi-modal model is robust to optical masking")
    print("   • Realistic performance ranges and variability")
    print("   • Statistical patterns supporting your hypotheses")

if __name__ == "__main__":
    main()