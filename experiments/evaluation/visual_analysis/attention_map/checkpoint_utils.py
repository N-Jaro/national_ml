"""
Checkpoint Discovery and Management Utilities

This script provides utilities for finding, analyzing, and managing
model checkpoints across different variants.
"""

import os
import sys
import json
import argparse
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path

# Import configuration
from gradcam_config import *

class CheckpointManager:
    """Advanced checkpoint management and analysis."""
    
    def __init__(self, search_paths: List[str] = None):
        self.search_paths = search_paths or [
            "/u/nathanj/national_ml/wandb",
            "/u/nathanj/national_ml/outputs", 
            "/u/nathanj/national_ml/experiments/training",
            "/u/nathanj/national_ml/lightning_logs",
            "/projects/bcrm/nathanj/models",
            "/projects/bcrm/nathanj/outputs",
            "/projects/bcrm/nathanj/wandb"
        ]
        
        self.logger = logging.getLogger(__name__)
    
    def discover_all_checkpoints(self) -> Dict[str, List[Dict]]:
        """Discover all checkpoints for all variants."""
        all_checkpoints = {}
        
        for variant in get_all_variants():
            checkpoints = self._find_variant_checkpoints(variant)
            all_checkpoints[variant] = checkpoints
            self.logger.info(f"Found {len(checkpoints)} checkpoints for {variant}")
        
        return all_checkpoints
    
    def _find_variant_checkpoints(self, variant: str) -> List[Dict]:
        """Find all checkpoints for a specific variant."""
        checkpoints = []
        
        # Multiple search patterns to handle different naming conventions
        patterns = [
            f"**/*{variant}*.ckpt",
            f"**/*{variant}*.pth", 
            f"**/mdmt-{variant.replace('_', '-')}*.ckpt",
            f"**/mdmt_{variant}*.ckpt",
            f"**/{variant}*.ckpt",
            f"**/*{variant.upper()}*.ckpt",
            f"**/best_*{variant}*.ckpt",
            f"**/checkpoint_*{variant}*.ckpt"
        ]
        
        import glob
        
        for search_path in self.search_paths:
            if not os.path.exists(search_path):
                continue
                
            for pattern in patterns:
                full_pattern = os.path.join(search_path, pattern)
                matches = glob.glob(full_pattern, recursive=True)
                
                for match in matches:
                    if os.path.isfile(match):
                        checkpoint_info = self._analyze_checkpoint_file(match, variant)
                        if checkpoint_info:
                            checkpoints.append(checkpoint_info)
        
        # Remove duplicates and sort by quality score
        unique_checkpoints = {}
        for cp in checkpoints:
            path = cp['path']
            if path not in unique_checkpoints or cp['quality_score'] > unique_checkpoints[path]['quality_score']:
                unique_checkpoints[path] = cp
        
        final_checkpoints = list(unique_checkpoints.values())
        final_checkpoints.sort(key=lambda x: x['quality_score'], reverse=True)
        
        return final_checkpoints
    
    def _analyze_checkpoint_file(self, filepath: str, variant: str) -> Optional[Dict]:
        """Analyze a checkpoint file and extract metadata."""
        try:
            # Basic file information
            stat = os.stat(filepath)
            filename = os.path.basename(filepath)
            
            checkpoint_info = {
                'path': filepath,
                'filename': filename,
                'variant': variant,
                'size_bytes': stat.st_size,
                'size_mb': stat.st_size / (1024 * 1024),
                'modified_time': stat.st_mtime,
                'modified_datetime': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'directory': os.path.dirname(filepath)
            }
            
            # Extract metrics from filename
            metrics = self._extract_filename_metrics(filename)
            checkpoint_info.update(metrics)
            
            # Try to load and analyze checkpoint content
            try:
                content_info = self._analyze_checkpoint_content(filepath)
                checkpoint_info.update(content_info)
            except Exception as e:
                self.logger.debug(f"Could not analyze checkpoint content for {filepath}: {e}")
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(checkpoint_info)
            checkpoint_info['quality_score'] = quality_score
            
            return checkpoint_info
            
        except Exception as e:
            self.logger.warning(f"Failed to analyze checkpoint {filepath}: {e}")
            return None
    
    def _extract_filename_metrics(self, filename: str) -> Dict:
        """Extract performance metrics from checkpoint filename."""
        import re
        metrics = {}
        
        # Validation loss patterns
        val_loss_patterns = [
            r'val[-_]loss[-_=]([0-9.]+)',
            r'loss[-_=]([0-9.]+)',
            r'([0-9.]+)\.ckpt'  # Sometimes loss is just the number before .ckpt
        ]
        
        for pattern in val_loss_patterns:
            match = re.search(pattern, filename.lower())
            if match:
                try:
                    metrics['val_loss'] = float(match.group(1))
                    break
                except ValueError:
                    continue
        
        # IoU patterns
        iou_patterns = [
            r'(?:val[-_])?iou[-_=]([0-9.]+)',
            r'iou([0-9.]+)',
            r'([0-9.]+)iou'
        ]
        
        for pattern in iou_patterns:
            match = re.search(pattern, filename.lower())
            if match:
                try:
                    metrics['val_iou'] = float(match.group(1))
                    break
                except ValueError:
                    continue
        
        # F1 score patterns
        f1_patterns = [
            r'(?:val[-_])?f1[-_=]([0-9.]+)',
            r'f1([0-9.]+)'
        ]
        
        for pattern in f1_patterns:
            match = re.search(pattern, filename.lower())
            if match:
                try:
                    metrics['val_f1'] = float(match.group(1))
                    break
                except ValueError:
                    continue
        
        # Epoch number
        epoch_match = re.search(r'epoch[-_=]?([0-9]+)', filename.lower())
        if epoch_match:
            metrics['epoch'] = int(epoch_match.group(1))
        
        # Special indicators
        filename_lower = filename.lower()
        metrics['is_best'] = 'best' in filename_lower
        metrics['is_last'] = 'last' in filename_lower
        metrics['is_final'] = 'final' in filename_lower
        
        return metrics
    
    def _analyze_checkpoint_content(self, filepath: str) -> Dict:
        """Analyze the actual checkpoint content."""
        import torch
        
        content_info = {}
        
        try:
            # Load checkpoint (map to CPU to avoid GPU memory issues)
            checkpoint = torch.load(filepath, map_location='cpu')
            
            # Analyze checkpoint structure
            if isinstance(checkpoint, dict):
                content_info['checkpoint_keys'] = list(checkpoint.keys())
                
                # Look for common keys
                if 'epoch' in checkpoint:
                    content_info['checkpoint_epoch'] = checkpoint['epoch']
                
                if 'state_dict' in checkpoint:
                    state_dict = checkpoint['state_dict']
                    content_info['model_parameters'] = len(state_dict)
                    content_info['has_state_dict'] = True
                else:
                    content_info['has_state_dict'] = False
                
                # Look for optimizer state
                content_info['has_optimizer'] = 'optimizer_state_dict' in checkpoint
                
                # Look for scheduler state
                content_info['has_scheduler'] = 'scheduler_state_dict' in checkpoint or 'lr_scheduler_state_dict' in checkpoint
                
                # Look for training metrics
                metric_keys = [k for k in checkpoint.keys() if 'val' in k.lower() or 'loss' in k.lower() or 'metric' in k.lower()]
                if metric_keys:
                    content_info['available_metrics'] = metric_keys
                    
                    # Extract specific metrics if available
                    for key in metric_keys:
                        value = checkpoint[key]
                        if isinstance(value, (int, float)):
                            content_info[f'checkpoint_{key}'] = value
            
            # Try to estimate model size
            if 'state_dict' in checkpoint:
                total_params = sum(p.numel() for p in checkpoint['state_dict'].values() if hasattr(p, 'numel'))
                content_info['total_parameters'] = total_params
                content_info['estimated_model_size_mb'] = total_params * 4 / (1024 * 1024)  # Assuming float32
            
        except Exception as e:
            self.logger.debug(f"Could not load checkpoint content for {filepath}: {e}")
            content_info['content_analysis_error'] = str(e)
        
        return content_info
    
    def _calculate_quality_score(self, checkpoint_info: Dict) -> float:
        """Calculate a quality score for ranking checkpoints."""
        score = 0.0
        
        # File naming indicators
        if checkpoint_info.get('is_best', False):
            score += 100
        elif checkpoint_info.get('is_final', False):
            score += 80
        elif checkpoint_info.get('is_last', False):
            score += 60
        
        # Performance metrics from filename
        if 'val_iou' in checkpoint_info:
            iou = checkpoint_info['val_iou']
            if iou > 1:  # Probably scaled to 100
                iou = iou / 100
            score += iou * 50
        
        if 'val_f1' in checkpoint_info:
            f1 = checkpoint_info['val_f1']
            if f1 > 1:  # Probably scaled to 100
                f1 = f1 / 100
            score += f1 * 40
        
        if 'val_loss' in checkpoint_info:
            loss = checkpoint_info['val_loss']
            # Lower loss is better, convert to positive score
            score += max(0, (2.0 - loss) * 20)
        
        # Performance metrics from checkpoint content
        if 'checkpoint_val_iou' in checkpoint_info:
            score += checkpoint_info['checkpoint_val_iou'] * 30
        
        if 'checkpoint_val_loss' in checkpoint_info:
            loss = checkpoint_info['checkpoint_val_loss']
            score += max(0, (2.0 - loss) * 15)
        
        # Epoch information (higher epoch generally better, but cap it)
        if 'epoch' in checkpoint_info:
            epoch_score = min(checkpoint_info['epoch'] / 50, 5)  # Cap at 5 points
            score += epoch_score
        
        # File recency (more recent is slightly better)
        days_old = (datetime.now().timestamp() - checkpoint_info['modified_time']) / (24 * 3600)
        if days_old < 30:
            recency_score = (30 - days_old) / 30 * 5
            score += recency_score
        
        # File size reasonableness
        size_mb = checkpoint_info['size_mb']
        if 5 < size_mb < 2000:  # Reasonable size range
            score += 3
        
        # Checkpoint completeness
        if checkpoint_info.get('has_state_dict', False):
            score += 5
        if checkpoint_info.get('has_optimizer', False):
            score += 2
        if checkpoint_info.get('has_scheduler', False):
            score += 1
        
        return score
    
    def get_best_checkpoints(self, top_k: int = 1) -> Dict[str, List[Dict]]:
        """Get the top-k best checkpoints for each variant."""
        all_checkpoints = self.discover_all_checkpoints()
        best_checkpoints = {}
        
        for variant, checkpoints in all_checkpoints.items():
            best_checkpoints[variant] = checkpoints[:top_k]
        
        return best_checkpoints
    
    def create_checkpoint_report(self, output_path: str = None) -> str:
        """Create a comprehensive checkpoint discovery report."""
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = os.path.join(ANALYSIS_DIR, f"checkpoint_analysis_{timestamp}.json")
        
        # Discover all checkpoints
        all_checkpoints = self.discover_all_checkpoints()
        
        # Create comprehensive report
        report = {
            'analysis_timestamp': datetime.now().isoformat(),
            'search_paths': self.search_paths,
            'total_variants': len(all_checkpoints),
            'summary': {},
            'variants': {}
        }
        
        total_checkpoints = 0
        variants_with_checkpoints = 0
        
        for variant, checkpoints in all_checkpoints.items():
            total_checkpoints += len(checkpoints)
            if checkpoints:
                variants_with_checkpoints += 1
            
            # Get best checkpoint for this variant
            best_checkpoint = checkpoints[0] if checkpoints else None
            
            variant_info = {
                'total_checkpoints': len(checkpoints),
                'best_checkpoint_path': best_checkpoint['path'] if best_checkpoint else None,
                'best_checkpoint_score': best_checkpoint['quality_score'] if best_checkpoint else 0,
                'all_checkpoints': checkpoints
            }
            
            # Extract performance metrics from best checkpoint
            if best_checkpoint:
                for metric in ['val_iou', 'val_f1', 'val_loss', 'epoch']:
                    if metric in best_checkpoint:
                        variant_info[f'best_{metric}'] = best_checkpoint[metric]
            
            report['variants'][variant] = variant_info
        
        # Summary statistics
        report['summary'] = {
            'total_checkpoints_found': total_checkpoints,
            'variants_with_checkpoints': variants_with_checkpoints,
            'variants_without_checkpoints': len(all_checkpoints) - variants_with_checkpoints,
            'avg_checkpoints_per_variant': total_checkpoints / len(all_checkpoints) if all_checkpoints else 0
        }
        
        # Save detailed report
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Create human-readable summary
        summary_path = output_path.replace('.json', '_summary.txt')
        self._create_readable_summary(report, summary_path)
        
        self.logger.info(f"Checkpoint analysis report saved: {output_path}")
        self.logger.info(f"Human-readable summary saved: {summary_path}")
        
        return output_path
    
    def _create_readable_summary(self, report: Dict, output_path: str):
        """Create a human-readable summary of the checkpoint analysis."""
        with open(output_path, 'w') as f:
            f.write("MODEL CHECKPOINT ANALYSIS SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Analysis Date: {report['analysis_timestamp']}\n")
            f.write(f"Search Paths: {len(report['search_paths'])}\n")
            f.write(f"Total Checkpoints Found: {report['summary']['total_checkpoints_found']}\n")
            f.write(f"Variants with Checkpoints: {report['summary']['variants_with_checkpoints']}\n")
            f.write(f"Variants without Checkpoints: {report['summary']['variants_without_checkpoints']}\n\n")
            
            f.write("CHECKPOINT STATUS BY VARIANT:\n")
            f.write("-" * 40 + "\n")
            
            for variant, info in report['variants'].items():
                f.write(f"\n{variant.upper()}:\n")
                if info['total_checkpoints'] > 0:
                    f.write(f"  Status: ✅ {info['total_checkpoints']} checkpoint(s) found\n")
                    f.write(f"  Best Checkpoint: {os.path.basename(info['best_checkpoint_path'])}\n")
                    f.write(f"  Quality Score: {info['best_checkpoint_score']:.2f}\n")
                    
                    # Add performance metrics if available
                    for metric in ['best_val_iou', 'best_val_f1', 'best_val_loss', 'best_epoch']:
                        if metric in info:
                            metric_name = metric.replace('best_', '').replace('_', ' ').title()
                            f.write(f"  {metric_name}: {info[metric]}\n")
                else:
                    f.write(f"  Status: ❌ No checkpoints found\n")
            
            # List search paths
            f.write(f"\nSEARCH PATHS:\n")
            f.write("-" * 20 + "\n")
            for i, path in enumerate(report['search_paths'], 1):
                exists = "✅" if os.path.exists(path) else "❌"
                f.write(f"{i:2d}. {exists} {path}\n")

def main():
    parser = argparse.ArgumentParser(description="Checkpoint discovery and analysis utilities")
    parser.add_argument("--action", choices=['discover', 'analyze', 'list-best'], default='analyze',
                       help="Action to perform")
    parser.add_argument("--variant", help="Specific variant to analyze (optional)")
    parser.add_argument("--search-paths", nargs='+', help="Additional search paths for checkpoints")
    parser.add_argument("--output-dir", default=ANALYSIS_DIR, help="Output directory for reports")
    parser.add_argument("--top-k", type=int, default=3, help="Number of top checkpoints to show per variant")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Initialize checkpoint manager
    search_paths = args.search_paths if args.search_paths else None
    manager = CheckpointManager(search_paths)
    
    if args.action == 'discover':
        print("Discovering checkpoints...")
        all_checkpoints = manager.discover_all_checkpoints()
        
        print(f"\n{'='*60}")
        print("CHECKPOINT DISCOVERY RESULTS")
        print(f"{'='*60}")
        
        for variant, checkpoints in all_checkpoints.items():
            print(f"\n{variant.upper()}: {len(checkpoints)} checkpoint(s)")
            if checkpoints:
                for i, cp in enumerate(checkpoints[:3], 1):  # Show top 3
                    print(f"  {i}. {cp['filename']} (score: {cp['quality_score']:.2f})")
                if len(checkpoints) > 3:
                    print(f"  ... and {len(checkpoints) - 3} more")
            else:
                print("  ❌ No checkpoints found")
    
    elif args.action == 'analyze':
        print("Running comprehensive checkpoint analysis...")
        report_path = manager.create_checkpoint_report()
        
        print(f"\n{'='*60}")
        print("CHECKPOINT ANALYSIS COMPLETED")
        print(f"{'='*60}")
        print(f"Detailed report: {report_path}")
        print(f"Summary: {report_path.replace('.json', '_summary.txt')}")
    
    elif args.action == 'list-best':
        print(f"Finding top {args.top_k} checkpoints per variant...")
        best_checkpoints = manager.get_best_checkpoints(args.top_k)
        
        print(f"\n{'='*60}")
        print(f"TOP {args.top_k} CHECKPOINTS PER VARIANT")
        print(f"{'='*60}")
        
        for variant, checkpoints in best_checkpoints.items():
            print(f"\n{variant.upper()}:")
            if checkpoints:
                for i, cp in enumerate(checkpoints, 1):
                    print(f"  {i}. {cp['filename']}")
                    print(f"     Score: {cp['quality_score']:.2f}")
                    print(f"     Path: {cp['path']}")
                    if 'val_iou' in cp:
                        print(f"     Val IoU: {cp['val_iou']}")
                    if 'val_loss' in cp:
                        print(f"     Val Loss: {cp['val_loss']}")
                    print()
            else:
                print("  ❌ No checkpoints found")

if __name__ == "__main__":
    main()