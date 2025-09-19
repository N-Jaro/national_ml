#!/usr/bin/env python3
"""
Speed Test - Quick validation of the optimized pipeline.
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ee
from config import Settings
from local_config import LocalProcessingSettings
from multi_source_orchestrator import MultiSourcePipelineOrchestrator

def main():
    """Quick speed test of the pipeline."""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        print("🚀 Pipeline Speed Test")
        print("=" * 50)
        
        # Initialize
        gee_settings = Settings()
        ee.Initialize(project=gee_settings.GEE_PROJECT_ID)
        print("✅ GEE initialized")
        
        config = LocalProcessingSettings()
        orchestrator = MultiSourcePipelineOrchestrator(config)
        print("✅ Orchestrator ready")
        
        # Test configuration
        huc_id = "10020007"
        data_sources = ["dem", "landsat"]  # Start with faster sources
        
        print(f"\n📍 Testing HUC: {huc_id}")
        print(f"🛰️  Sources: {data_sources}")
        
        # Check existing files
        print(f"\n📁 Checking existing files...")
        
        existing_files = {}
        for source in data_sources:
            if source == "dem":
                path = config.local_raster_base / "dem" / f"huc_{huc_id}_dem_10m_research_grade.tif"
            elif source == "landsat":
                path = config.local_raster_base / "landsat" / f"huc_{huc_id}_landsat_30m_2023-06-01_2023-09-30_median.tif"
            
            if path.exists():
                size_mb = path.stat().st_size / (1024 * 1024)
                existing_files[source] = {'path': path, 'size_mb': size_mb}
                print(f"   ✅ {source}: {path.name} ({size_mb:.1f} MB)")
            else:
                print(f"   ❌ {source}: Not found")
        
        # Speed test: Create and execute plan
        print(f"\n⚡ Speed Test: Fast Mode Processing")
        start_time = time.time()
        
        plan = orchestrator.create_processing_plan(
            huc_ids=[huc_id],
            data_sources=data_sources,
            start_date="2023-06-01",
            end_date="2023-09-30"
        )
        
        executed_plan = orchestrator.execute_plan(plan, max_workers=4, fast_mode=True)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Results
        summary = orchestrator.get_execution_summary(executed_plan)
        
        print(f"\n📊 Speed Test Results:")
        print(f"   Duration: {duration:.1f} seconds")
        print(f"   Success rate: {summary['overall']['success_rate']:.1f}%")
        print(f"   Tasks completed: {summary['overall']['completed_tasks']}")
        print(f"   Tasks failed: {summary['overall']['failed_tasks']}")
        
        print(f"\n📈 Per-source results:")
        for source, stats in summary['by_data_source'].items():
            print(f"   {source}: {stats['completed']}/{stats['total']} completed")
        
        if summary['failed_tasks']:
            print(f"\n❌ Failed tasks:")
            for task in summary['failed_tasks']:
                print(f"   {task['data_source']}: {task['error']}")
        
        print(f"\n✅ Speed test completed in {duration:.1f} seconds!")
        
        # Show speedup benefits
        if existing_files:
            skipped = sum(1 for task in executed_plan['tasks'] if task.get('skipped', False))
            print(f"\n⚡ Speed optimizations:")
            print(f"   Files skipped (already exist): {skipped}")
            print(f"   Parallel processing: 4 workers")
            print(f"   Fast mode enabled: True")
        
    except Exception as e:
        print(f"❌ Speed test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
