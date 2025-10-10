#!/usr/bin/env python3
"""
Intelligent Patch Selection for Grad-CAM Visualization

This module implements a systematic approach to select the most informative
patches within the optimized HUC list for attention map visualization.
The goal is to select patches that will provide meaningful and comparable
attention patterns across all model variants.
"""

import os
import sys
import numpy as np
import glob
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json
from dataclasses import dataclass
from collections import defaultdict
import logging

# Add src directory to path for imports
sys.path.append('/u/nathanj/national_ml/src')


@dataclass
class PatchInfo:
    """Information about a patch for selection scoring."""
    huc_id: str
    patch_id: str
    file_path: str
    water_pixel_count: int
    water_percentage: float
    total_pixels: int
    valid_pixels: int
    data_completeness: float
    geographic_diversity_score: float
    selection_score: float


class IntelligentPatchSelector:
    """Select patches systematically for Grad-CAM visualization."""
    
    def __init__(self, test_data_path: str = "/projects/bcrm/nathanj/data/processed/test/patch_dataset"):
        self.test_data_path = Path(test_data_path)
        self.setup_logging()
        
        # Load optimized HUC list
        self.selected_hucs = self.load_optimized_huc_list()
        
        # Selection criteria
        self.criteria = {
            'min_water_pixels': 50,      # Minimum water pixels for meaningful attention
            'min_water_percentage': 5.0,  # Minimum 5% water coverage
            'max_water_percentage': 85.0, # Maximum 85% water (avoid all-water patches)
            'min_data_completeness': 90.0, # Minimum 90% valid pixels
            'patches_per_huc': 3,         # Target patches per HUC
            'water_distribution_targets': { # Diverse water content representation
                'low_water': (5.0, 20.0),    # 5-20% water
                'medium_water': (20.0, 50.0), # 20-50% water  
                'high_water': (50.0, 85.0)    # 50-85% water
            }
        }
        
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def load_optimized_huc_list(self) -> List[str]:
        """Load the optimized HUC list from test_huc_list.txt."""
        huc_file = Path('/u/nathanj/national_ml/experiments/evaluation/test_huc_list.txt')
        
        if not huc_file.exists():
            # Fallback to test_huc_optimized.txt if it exists
            huc_file = Path('/u/nathanj/national_ml/experiments/evaluation/test_huc_optimized.txt')
        
        if not huc_file.exists():
            self.logger.error(f"HUC list file not found: {huc_file}")
            return []
            
        hucs = []
        with open(huc_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    hucs.append(line)
                    
        self.logger.info(f"Loaded {len(hucs)} HUCs from {huc_file}")
        return hucs
        
    def analyze_patch(self, patch_path: Path) -> Optional[PatchInfo]:
        """Analyze a single patch file to extract selection metrics."""
        try:
            # Load patch data
            data = np.load(patch_path)
            
            # Extract components
            dem = data.get('dem', None)
            hydro_mask = data.get('hydro_mask', None)
            
            if dem is None or hydro_mask is None:
                return None
                
            # Calculate basic statistics
            total_pixels = dem.size
            valid_dem_pixels = np.sum(~np.isnan(dem))
            valid_hydro_pixels = np.sum(~np.isnan(hydro_mask))
            valid_pixels = min(valid_dem_pixels, valid_hydro_pixels)
            
            # Water pixel analysis (assuming water = 1 in hydro_mask)
            water_mask = (hydro_mask == 1) & (~np.isnan(hydro_mask))
            water_pixel_count = np.sum(water_mask)
            water_percentage = (water_pixel_count / valid_pixels) * 100 if valid_pixels > 0 else 0
            
            # Data completeness
            data_completeness = (valid_pixels / total_pixels) * 100
            
            # Extract HUC and patch ID from filename
            huc_id = patch_path.parent.name
            patch_id = patch_path.stem
            
            # Calculate geographic diversity score (simplified)
            geographic_diversity_score = self.calculate_geographic_diversity_score(huc_id)
            
            # Calculate overall selection score
            selection_score = self.calculate_selection_score(
                water_percentage, data_completeness, geographic_diversity_score
            )
            
            return PatchInfo(
                huc_id=huc_id,
                patch_id=patch_id,
                file_path=str(patch_path),
                water_pixel_count=water_pixel_count,
                water_percentage=water_percentage,
                total_pixels=total_pixels,
                valid_pixels=valid_pixels,
                data_completeness=data_completeness,
                geographic_diversity_score=geographic_diversity_score,
                selection_score=selection_score
            )
            
        except Exception as e:
            self.logger.warning(f"Error analyzing patch {patch_path}: {e}")
            return None
            
    def calculate_geographic_diversity_score(self, huc_id: str) -> float:
        """Calculate geographic diversity score based on HUC characteristics."""
        # Simplified scoring based on water resource region
        region_scores = {
            '01': 85,  # New England - high diversity
            '02': 90,  # Mid-Atlantic - very high diversity
            '03': 80,  # South Atlantic - high diversity
            '04': 75,  # Great Lakes - good diversity
            '10': 95,  # Missouri - excellent diversity
            '14': 90,  # Upper Colorado - very high diversity
            '17': 85,  # Pacific Northwest - high diversity
            '18': 88,  # California - very high diversity
            '19': 70,  # Alaska - unique but challenging
        }
        
        region = huc_id[:2]
        return region_scores.get(region, 60.0)  # Default moderate score
        
    def calculate_selection_score(self, water_percentage: float, 
                                 data_completeness: float,
                                 geographic_diversity_score: float) -> float:
        """Calculate overall selection score for a patch."""
        
        # Water content score (prefer patches with meaningful water content)
        if 15.0 <= water_percentage <= 40.0:  # Sweet spot
            water_score = 100.0
        elif 5.0 <= water_percentage < 15.0 or 40.0 < water_percentage <= 60.0:
            water_score = 80.0
        elif 60.0 < water_percentage <= 85.0:
            water_score = 60.0
        else:
            water_score = 20.0  # Too little or too much water
            
        # Data quality score
        quality_score = data_completeness  # Already 0-100
        
        # Combined score with weights
        weights = {
            'water_content': 0.4,
            'data_quality': 0.4,
            'geographic_diversity': 0.2
        }
        
        score = (
            weights['water_content'] * water_score +
            weights['data_quality'] * quality_score +
            weights['geographic_diversity'] * geographic_diversity_score
        )
        
        return score
        
    def select_patches_for_huc(self, huc_id: str) -> List[PatchInfo]:
        """Select the best patches for a specific HUC."""
        huc_path = self.test_data_path / huc_id
        
        if not huc_path.exists():
            self.logger.warning(f"HUC directory not found: {huc_path}")
            return []
            
        # Get all patch files
        patch_files = list(huc_path.glob("*.npz"))
        
        if len(patch_files) == 0:
            self.logger.warning(f"No patch files found in {huc_path}")
            return []
            
        self.logger.info(f"Analyzing {len(patch_files)} patches in HUC {huc_id}")
        
        # Analyze all patches
        valid_patches = []
        for patch_file in patch_files:
            patch_info = self.analyze_patch(patch_file)
            if patch_info and self.meets_criteria(patch_info):
                valid_patches.append(patch_info)
                
        if not valid_patches:
            self.logger.warning(f"No valid patches found in HUC {huc_id}")
            return []
            
        # Sort by selection score
        valid_patches.sort(key=lambda x: x.selection_score, reverse=True)
        
        # Select diverse patches based on water content
        selected = self.select_diverse_patches(valid_patches)
        
        self.logger.info(f"Selected {len(selected)} patches from HUC {huc_id}")
        return selected
        
    def meets_criteria(self, patch_info: PatchInfo) -> bool:
        """Check if patch meets basic selection criteria."""
        return (
            patch_info.water_pixel_count >= self.criteria['min_water_pixels'] and
            patch_info.water_percentage >= self.criteria['min_water_percentage'] and
            patch_info.water_percentage <= self.criteria['max_water_percentage'] and
            patch_info.data_completeness >= self.criteria['min_data_completeness']
        )
        
    def select_diverse_patches(self, valid_patches: List[PatchInfo]) -> List[PatchInfo]:
        """Select patches with diverse water content representation."""
        targets = self.criteria['water_distribution_targets']
        target_count = self.criteria['patches_per_huc']
        
        # Categorize patches by water content
        categorized = {
            'low_water': [],
            'medium_water': [],
            'high_water': []
        }
        
        for patch in valid_patches:
            wp = patch.water_percentage
            if targets['low_water'][0] <= wp < targets['low_water'][1]:
                categorized['low_water'].append(patch)
            elif targets['medium_water'][0] <= wp < targets['medium_water'][1]:
                categorized['medium_water'].append(patch)
            elif targets['high_water'][0] <= wp <= targets['high_water'][1]:
                categorized['high_water'].append(patch)
                
        # Select diverse patches
        selected = []
        
        # Try to get one from each category first
        for category in ['medium_water', 'low_water', 'high_water']:  # Prefer medium water first
            if categorized[category] and len(selected) < target_count:
                # Sort by selection score and take the best
                categorized[category].sort(key=lambda x: x.selection_score, reverse=True)
                selected.append(categorized[category][0])
                
        # Fill remaining slots with highest scoring patches
        remaining_patches = [p for p in valid_patches if p not in selected]
        remaining_patches.sort(key=lambda x: x.selection_score, reverse=True)
        
        while len(selected) < target_count and remaining_patches:
            selected.append(remaining_patches.pop(0))
            
        return selected
        
    def select_all_patches(self) -> Dict[str, List[PatchInfo]]:
        """Select patches for all HUCs in the optimized list."""
        self.logger.info(f"Starting patch selection for {len(self.selected_hucs)} HUCs")
        
        all_selected = {}
        total_patches = 0
        
        for huc_id in self.selected_hucs:
            selected_patches = self.select_patches_for_huc(huc_id)
            if selected_patches:
                all_selected[huc_id] = selected_patches
                total_patches += len(selected_patches)
            else:
                self.logger.warning(f"No patches selected for HUC {huc_id}")
                
        self.logger.info(f"Selected {total_patches} patches across {len(all_selected)} HUCs")
        return all_selected
        
    def create_patch_selection_summary(self, selected_patches: Dict[str, List[PatchInfo]]) -> Dict:
        """Create a summary of patch selection results."""
        summary = {
            'selection_timestamp': '2025-10-07',
            'total_hucs': len(selected_patches),
            'total_patches': sum(len(patches) for patches in selected_patches.values()),
            'selection_criteria': self.criteria,
            'water_content_distribution': {
                'low_water': 0,
                'medium_water': 0, 
                'high_water': 0
            },
            'average_scores': {
                'selection_score': 0.0,
                'water_percentage': 0.0,
                'data_completeness': 0.0
            },
            'huc_details': {}
        }
        
        all_patches = []
        for huc_id, patches in selected_patches.items():
            huc_summary = {
                'patch_count': len(patches),
                'patches': []
            }
            
            for patch in patches:
                all_patches.append(patch)
                
                # Categorize water content
                wp = patch.water_percentage
                if 5.0 <= wp < 20.0:
                    summary['water_content_distribution']['low_water'] += 1
                elif 20.0 <= wp < 50.0:
                    summary['water_content_distribution']['medium_water'] += 1
                else:
                    summary['water_content_distribution']['high_water'] += 1
                    
                huc_summary['patches'].append({
                    'patch_id': patch.patch_id,
                    'file_path': patch.file_path,
                    'water_percentage': round(patch.water_percentage, 2),
                    'data_completeness': round(patch.data_completeness, 2),
                    'selection_score': round(patch.selection_score, 2)
                })
                
            summary['huc_details'][huc_id] = huc_summary
            
        # Calculate averages
        if all_patches:
            summary['average_scores']['selection_score'] = np.mean([p.selection_score for p in all_patches])
            summary['average_scores']['water_percentage'] = np.mean([p.water_percentage for p in all_patches])
            summary['average_scores']['data_completeness'] = np.mean([p.data_completeness for p in all_patches])
            
        return summary
        
    def save_selection_results(self, selected_patches: Dict[str, List[PatchInfo]], 
                              output_dir: str = "/u/nathanj/national_ml/experiments/evaluation/visual_analysis/attention_map"):
        """Save patch selection results to files."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create summary
        summary = self.create_patch_selection_summary(selected_patches)
        
        # Save detailed JSON
        json_file = output_path / "selected_patches_detailed.json"
        with open(json_file, 'w') as f:
            json.dump(summary, f, indent=2)
            
        # Save simple patch list for visualization scripts
        patch_list_file = output_path / "selected_patches_for_gradcam.txt"
        with open(patch_list_file, 'w') as f:
            f.write("# Selected Patches for Grad-CAM Visualization\n")
            f.write("# Format: HUC_ID,PATCH_ID,FILE_PATH,WATER_PERCENTAGE,SELECTION_SCORE\n")
            f.write("# Generated on 2025-10-07\n\n")
            
            for huc_id, patches in selected_patches.items():
                for patch in patches:
                    f.write(f"{huc_id},{patch.patch_id},{patch.file_path},"
                           f"{patch.water_percentage:.2f},{patch.selection_score:.2f}\n")
                           
        # Save Python-friendly format
        python_file = output_path / "selected_patches.py"
        with open(python_file, 'w') as f:
            f.write('"""Selected patches for Grad-CAM visualization."""\n\n')
            f.write('SELECTED_PATCHES = {\n')
            for huc_id, patches in selected_patches.items():
                f.write(f'    "{huc_id}": [\n')
                for patch in patches:
                    f.write(f'        {{\n')
                    f.write(f'            "patch_id": "{patch.patch_id}",\n')
                    f.write(f'            "file_path": "{patch.file_path}",\n')
                    f.write(f'            "water_percentage": {patch.water_percentage:.2f},\n')
                    f.write(f'            "selection_score": {patch.selection_score:.2f}\n')
                    f.write(f'        }},\n')
                f.write('    ],\n')
            f.write('}\n')
            
        self.logger.info(f"Selection results saved to:")
        self.logger.info(f"  - Detailed JSON: {json_file}")
        self.logger.info(f"  - Patch list: {patch_list_file}")
        self.logger.info(f"  - Python format: {python_file}")
        
        return summary


def main():
    """Main function to run patch selection."""
    selector = IntelligentPatchSelector()
    
    print("🎯 Starting Intelligent Patch Selection for Grad-CAM Visualization")
    print("=" * 70)
    
    # Select patches
    selected_patches = selector.select_all_patches()
    
    if not selected_patches:
        print("❌ No patches were selected!")
        return
        
    # Save results
    summary = selector.save_selection_results(selected_patches)
    
    # Print summary
    print(f"\n✅ PATCH SELECTION COMPLETE")
    print("=" * 50)
    print(f"📊 Total HUCs: {summary['total_hucs']}")
    print(f"📋 Total patches: {summary['total_patches']}")
    print(f"💧 Water content distribution:")
    print(f"   Low water (5-20%): {summary['water_content_distribution']['low_water']} patches")
    print(f"   Medium water (20-50%): {summary['water_content_distribution']['medium_water']} patches")
    print(f"   High water (50-85%): {summary['water_content_distribution']['high_water']} patches")
    print(f"📈 Average selection score: {summary['average_scores']['selection_score']:.1f}")
    print(f"💧 Average water percentage: {summary['average_scores']['water_percentage']:.1f}%")
    print(f"✅ Average data completeness: {summary['average_scores']['data_completeness']:.1f}%")
    print(f"\n🎊 Ready for Grad-CAM visualization!")


if __name__ == "__main__":
    main()