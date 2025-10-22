#!/usr/bin/env python3
"""
Run Both Transfer and Robustness Analyses (4a + 4b)

This script runs both focused analyses to address the core research questions:

4a) Transfer Analysis: AE-only vs DEM+AE across terrain bins
    - Isolates DEM contribution to terrain generalization
    - Compares low-relief vs high-relief performance

4b) Robustness Analysis: All+AE clean vs masked optical
    - Tests optical dependency via cloud simulation  
    - Quantifies performance degradation under 30% cloud cover

Usage:
    python run_transfer_robustness.py [--quick] [--hucs HUC1,HUC2,...]
"""

import sys
import os
import argparse
from pathlib import Path

# Add paths for imports
sys.path.append('/u/nathanj/national_ml/experiments')
sys.path.append('/u/nathanj/national_ml/experiments/evaluation/analysis/transfer_robustness')

from config import TransferRobustnessConfig

def run_transfer_analysis(config: TransferRobustnessConfig, huc_list: list):
    """Run the transfer analysis (4a)"""
    print("\n" + "🏔️ " * 20)
    print("STARTING TRANSFER ANALYSIS (4a)")
    print("🏔️ " * 20)
    
    try:
        from transfer_analysis_4a import TerrainTransferAnalyzer
        
        analyzer = TerrainTransferAnalyzer(config)
        results = analyzer.run_transfer_analysis(huc_list, config.limit_patches_per_huc)
        
        # Save results
        results_file = config.results_dir / "transfer_analysis_results.json"
        import json
        with open(results_file, 'w') as f:
            json_results = {}
            for model in results:
                json_results[model] = {}
                for terrain in results[model]:
                    json_results[model][terrain] = []
                    for metrics in results[model][terrain]:
                        json_metrics = {k: float(v) for k, v in metrics.items()}
                        json_results[model][terrain].append(json_metrics)
            json.dump(json_results, f, indent=2)
        
        # Create visualization and summary
        analyzer.create_transfer_visualization(results)
        analyzer.print_transfer_summary(results)
        
        print("✅ Transfer analysis (4a) completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Transfer analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_robustness_analysis(config: TransferRobustnessConfig, huc_list: list):
    """Run the robustness analysis (4b)"""
    print("\n" + "☁️ " * 20)
    print("STARTING ROBUSTNESS ANALYSIS (4b)")
    print("☁️ " * 20)
    
    try:
        from robustness_analysis_4b import OpticalRobustnessAnalyzer
        
        analyzer = OpticalRobustnessAnalyzer(config)
        results = analyzer.run_robustness_analysis(huc_list, config.limit_patches_per_huc)
        
        # Save results
        results_file = config.results_dir / "robustness_analysis_results.json"
        import json
        with open(results_file, 'w') as f:
            json_results = {}
            for condition in results:
                json_results[condition] = {}
                if "metrics" in results[condition]:
                    json_results[condition]["metrics"] = []
                    for metrics in results[condition]["metrics"]:
                        json_metrics = {k: float(v) for k, v in metrics.items()}
                        json_results[condition]["metrics"].append(json_metrics)
                
                for key in results[condition]:
                    if key != "metrics":
                        json_results[condition][key] = results[condition][key]
            
            json.dump(json_results, f, indent=2)
        
        # Create visualization and summary
        analyzer.create_robustness_visualization(results)
        analyzer.print_robustness_summary(results)
        
        print("✅ Robustness analysis (4b) completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Robustness analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_combined_summary(config: TransferRobustnessConfig):
    """Create a combined summary of both analyses"""
    print("\n" + "📋" * 30)
    print("COMBINED TRANSFER & ROBUSTNESS ANALYSIS SUMMARY")
    print("📋" * 30)
    
    transfer_file = config.results_dir / "transfer_analysis_results.json"
    robustness_file = config.results_dir / "robustness_analysis_results.json"
    
    summary = []
    summary.append("# Transfer and Robustness Analysis Results\n")
    summary.append("## Analysis Overview\n")
    summary.append("This analysis addresses two key research questions:\n")
    summary.append("- **4a) Transfer**: Does DEM improve terrain generalization? (AE-only vs DEM+AE)")
    summary.append("- **4b) Robustness**: How robust is the model to missing optical data? (clean vs masked)\n")
    
    # Add model checkpoints used
    summary.append("## Model Checkpoints Used\n")
    for name, path in config.checkpoints.items():
        model_name = os.path.basename(path)
        summary.append(f"- **{name}**: `{model_name}`")
    
    summary.append(f"\n## Analysis Configuration\n")
    summary.append(f"- **Terrain thresholds**: Low <{config.low_relief_threshold}m, High >{config.high_relief_threshold}m")
    summary.append(f"- **Cloud coverage**: {config.cloud_coverage*100}%")
    summary.append(f"- **Patches per HUC**: {config.limit_patches_per_huc}")
    summary.append(f"- **Device**: {config.device}")
    
    # Save summary
    summary_file = config.results_dir / "analysis_summary.md"
    with open(summary_file, 'w') as f:
        f.write('\n'.join(summary))
    
    print(f"📄 Combined summary saved: {summary_file}")
    
    # Print final status
    transfer_success = transfer_file.exists()
    robustness_success = robustness_file.exists()
    
    print(f"\n🎯 FINAL STATUS:")
    print(f"  Transfer Analysis (4a): {'✅ Complete' if transfer_success else '❌ Failed'}")
    print(f"  Robustness Analysis (4b): {'✅ Complete' if robustness_success else '❌ Failed'}")
    
    if transfer_success and robustness_success:
        print(f"\n🏆 ALL ANALYSES COMPLETED SUCCESSFULLY!")
        print(f"📊 Results saved in: {config.results_dir}")
        print(f"🖼️  Visualizations:")
        print(f"    - Transfer: transfer_analysis_4a.png")
        print(f"    - Robustness: robustness_analysis_4b.png")
    else:
        print(f"\n⚠️  Some analyses failed - check error messages above")

def main():
    """Main function to run both analyses"""
    parser = argparse.ArgumentParser(description='Run Transfer and Robustness Analysis')
    parser.add_argument('--quick', action='store_true', 
                       help='Quick mode: 20 patches per HUC instead of 50')
    parser.add_argument('--hucs', type=str, 
                       help='Comma-separated list of HUCs to analyze')
    parser.add_argument('--transfer-only', action='store_true',
                       help='Run only transfer analysis (4a)')
    parser.add_argument('--robustness-only', action='store_true', 
                       help='Run only robustness analysis (4b)')
    parser.add_argument('--run-id', type=str,
                       help='Custom run ID for results directory (default: auto-generated with timestamp)')
    
    args = parser.parse_args()
    
    # Initialize configuration
    config = TransferRobustnessConfig()
    
    # Set custom run ID if provided
    if args.run_id:
        config.set_custom_run_id(args.run_id)
    
    # Set quick mode
    if args.quick:
        config.limit_patches_per_huc = 20
        print("⚡ Quick mode: 20 patches per HUC")
    
    # Get HUC list
    if args.hucs:
        huc_list = args.hucs.split(',')
    else:
        # Use available HUCs from local data
        data_dir = Path(config.test_data_path)
        if data_dir.exists():
            available_hucs = [d.name for d in data_dir.iterdir() if d.is_dir()]
            huc_list = available_hucs[:5]  # Use first 5 for reasonable runtime
        else:
            huc_list = ["03030005", "04060102", "07040006"]  # Fallback
    
    print(f"\n🎯 TRANSFER & ROBUSTNESS ANALYSIS")
    print(f"🆔 Run ID: {config.run_id}")
    print(f"📍 HUCs to analyze: {huc_list}")
    print(f"📦 Patches per HUC: {config.limit_patches_per_huc}")
    print(f"💾 Results directory: {config.results_dir}")
    
    # Validate configuration
    try:
        config.validate_paths()
        print("✅ Configuration validated")
    except FileNotFoundError as e:
        print(f"⚠️  Some paths missing: {e}")
        print("Continuing with available resources...")
    
    # Run analyses
    transfer_success = True
    robustness_success = True
    
    if not args.robustness_only:
        transfer_success = run_transfer_analysis(config, huc_list)
    
    if not args.transfer_only:
        robustness_success = run_robustness_analysis(config, huc_list)
    
    # Create combined summary
    create_combined_summary(config)

if __name__ == "__main__":
    main()