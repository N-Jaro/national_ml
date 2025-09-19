#!/usr/bin/env python3
"""
Multi-Source Pipeline Orchestrator - Coordinates download and processing of all data sources.
Handles DEM, Landsat, SAR, and other satellite data for comprehensive HUC processing.
"""

import ee
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from local_config import LocalProcessingSettings
from data_manager import LocalDataManager
from gee_bulk_downloader import GEEBulkDownloader
from landsat_bulk_downloader import LandsatBulkDownloader
from sar_bulk_downloader import SARBulkDownloader
from alphaearth_bulk_downloader import AlphaEarthBulkDownloader
from reference_bulk_processor import ReferenceBulkProcessor

class MultiSourcePipelineOrchestrator:
    """Orchestrate multi-source satellite data processing pipeline."""
    
    def __init__(self, config: LocalProcessingSettings):
        """Initialize the pipeline orchestrator."""
        self.config = config
        self.data_manager = LocalDataManager(config)
        self.logger = logging.getLogger(__name__)
        
        # Initialize downloaders
        self.dem_downloader = GEEBulkDownloader(config, self.data_manager)
        self.landsat_downloader = LandsatBulkDownloader(config, self.data_manager)
        self.sar_downloader = SARBulkDownloader(config, self.data_manager)
        self.alphaearth_downloader = AlphaEarthBulkDownloader(config, self.data_manager)
        self.reference_processor = ReferenceBulkProcessor(config, self.data_manager)
        
        # Pipeline configuration
        self.default_date_range_days = 120  # 4 months for temporal composites
        self.max_concurrent_downloads = 4   # Increased from 2 for better parallelism
        self.enable_fast_mode = True        # Enable speed optimizations
        
    def create_processing_plan(self, huc_ids: List[str], 
                              data_sources: List[str],
                              start_date: Optional[str] = None,
                              end_date: Optional[str] = None) -> Dict:
        """Create a comprehensive processing plan for multiple HUCs and data sources."""
        
        # Set default date range if not provided
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=self.default_date_range_days)).strftime("%Y-%m-%d")
        
        # Available data sources with updated configurations
        available_sources = {
            'dem': {'resolution': 10.0, 'temporal': False, 'bands': 1},
            'landsat': {'resolution': 30.0, 'temporal': True, 'bands': 7},  # Updated: 7 bands (SR_B2-B7 + ST_B10)
            'sar': {'resolution': 10.0, 'temporal': True, 'bands': 6},
            'alphaearth': {'resolution': 10.0, 'temporal': False, 'bands': 64},
            'references': {'resolution': 10.0, 'temporal': False, 'bands': 2}  # hydro_mask, flow_direction only
        }
        
        # Validate data sources
        invalid_sources = set(data_sources) - set(available_sources.keys())
        if invalid_sources:
            raise ValueError(f"Invalid data sources: {invalid_sources}")
        
        # Create processing plan
        plan = {
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'huc_count': len(huc_ids),
                'data_sources': data_sources,
                'date_range': {'start': start_date, 'end': end_date},
                'total_tasks': len(huc_ids) * len(data_sources)
            },
            'tasks': []
        }
        
        # Generate tasks for each HUC and data source
        task_id = 0
        for huc_id in huc_ids:
            for source in data_sources:
                source_config = available_sources[source]
                
                task = {
                    'task_id': task_id,
                    'huc_id': huc_id,
                    'data_source': source,
                    'resolution': source_config['resolution'],
                    'temporal': source_config['temporal'],
                    'start_date': start_date if source_config['temporal'] else None,
                    'end_date': end_date if source_config['temporal'] else None,
                    'status': 'pending',
                    'output_path': None,
                    'error': None,
                    'duration_seconds': None
                }
                
                plan['tasks'].append(task)
                task_id += 1
        
        self.logger.info(f"Created processing plan with {len(plan['tasks'])} tasks")
        return plan
    
    def execute_task(self, task: Dict, skip_existing: bool = True) -> Dict:
        """Execute a single processing task with optimizations."""
        
        # Check if output already exists (speed optimization)
        if skip_existing:
            expected_output = self._get_expected_output_path(task)
            if expected_output and expected_output.exists():
                file_size_mb = expected_output.stat().st_size / (1024 * 1024)
                if file_size_mb > 1:  # File exists and is not empty
                    self.logger.info(f"⚡ Skipping task {task['task_id']}: Output already exists ({file_size_mb:.1f}MB)")
                    task['status'] = 'completed'
                    task['output_path'] = str(expected_output)
                    task['duration_seconds'] = 0
                    task['skipped'] = True
                    return task
        
        task_start = time.time()
        self.logger.info(f"Starting task {task['task_id']}: {task['data_source']} for HUC {task['huc_id']}")
        
        try:
            if task['data_source'] == 'dem':
                output_path = self.dem_downloader.download_dem_for_huc(
                    task['huc_id'], 
                    resolution=task['resolution']
                )
            
            elif task['data_source'] == 'landsat':
                output_path = self.landsat_downloader.download_landsat_for_huc(
                    task['huc_id'],
                    task['start_date'],
                    task['end_date'],
                    resolution=task['resolution']
                )
            
            elif task['data_source'] == 'sar':
                output_path = self.sar_downloader.download_sar_for_huc(
                    task['huc_id'],
                    task['start_date'],
                    task['end_date'],
                    resolution=task['resolution']
                )
            
            elif task['data_source'] == 'alphaearth':
                # Speed optimization: Use smaller band subset for faster processing
                band_count = 8 if self.enable_fast_mode else 16
                output_path = self.alphaearth_downloader.download_alphaearth_subset(
                    task['huc_id'],
                    band_count=band_count,
                    year=2024  # Use latest available year
                )
            
            elif task['data_source'] == 'references':
                reference_types = ['hydro_mask', 'flow_direction']  # Only 2 reference types needed
                results = self.reference_processor.process_all_references_for_huc(
                    task['huc_id'], 
                    reference_types
                )
                # Return the directory containing all reference files
                output_path = str(self.config.local_raster_base / "reference")
            
            else:
                raise ValueError(f"Unknown data source: {task['data_source']}")
            
            # Update task with success
            task['status'] = 'completed'
            task['output_path'] = output_path
            task['duration_seconds'] = time.time() - task_start
            
            self.logger.info(f"Task {task['task_id']} completed in {task['duration_seconds']:.1f}s")
            
        except Exception as e:
            task['status'] = 'failed'
            task['error'] = str(e)
            task['duration_seconds'] = time.time() - task_start
            
            self.logger.error(f"Task {task['task_id']} failed: {e}")
        
        return task
    
    def execute_plan(self, plan: Dict, max_workers: Optional[int] = None, 
                     fast_mode: bool = True) -> Dict:
        """Execute the complete processing plan with optimizations."""
        
        if max_workers is None:
            max_workers = self.max_concurrent_downloads
        
        # Increase workers in fast mode
        if fast_mode:
            max_workers = min(max_workers * 2, 8)  # Double workers, cap at 8
        
        self.logger.info(f"Executing plan with {max_workers} concurrent workers (fast_mode={fast_mode})")
        
        # Track execution
        execution_start = time.time()
        completed_tasks = 0
        failed_tasks = 0
        
        # Execute tasks with thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self.execute_task, task, fast_mode): task 
                for task in plan['tasks']
            }
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                
                try:
                    updated_task = future.result()
                    
                    # Update plan with results
                    task_index = next(i for i, t in enumerate(plan['tasks']) 
                                    if t['task_id'] == updated_task['task_id'])
                    plan['tasks'][task_index] = updated_task
                    
                    if updated_task['status'] == 'completed':
                        completed_tasks += 1
                    else:
                        failed_tasks += 1
                    
                    # Progress update
                    total_tasks = len(plan['tasks'])
                    progress = ((completed_tasks + failed_tasks) / total_tasks) * 100
                    
                    self.logger.info(f"Progress: {progress:.1f}% ({completed_tasks} completed, {failed_tasks} failed)")
                
                except Exception as e:
                    self.logger.error(f"Task execution error: {e}")
                    failed_tasks += 1
        
        # Update plan metadata with results
        execution_time = time.time() - execution_start
        plan['metadata']['execution_summary'] = {
            'total_duration_seconds': execution_time,
            'completed_tasks': completed_tasks,
            'failed_tasks': failed_tasks,
            'success_rate': (completed_tasks / len(plan['tasks'])) * 100,
            'completed_at': datetime.now().isoformat()
        }
        
        self.logger.info(f"Plan execution completed in {execution_time:.1f}s")
        self.logger.info(f"Success rate: {plan['metadata']['execution_summary']['success_rate']:.1f}%")
        
        return plan
    
    def save_plan(self, plan: Dict, output_path: Path):
        """Save processing plan to JSON file."""
        with open(output_path, 'w') as f:
            json.dump(plan, f, indent=2)
        self.logger.info(f"Plan saved to: {output_path}")
    
    def load_plan(self, plan_path: Path) -> Dict:
        """Load processing plan from JSON file."""
        with open(plan_path, 'r') as f:
            plan = json.load(f)
        self.logger.info(f"Plan loaded from: {plan_path}")
        return plan
    
    def get_execution_summary(self, plan: Dict) -> Dict:
        """Get a summary of plan execution results."""
        
        if 'execution_summary' not in plan['metadata']:
            return {'status': 'not_executed'}
        
        # Group results by data source and HUC
        by_source = {}
        by_huc = {}
        
        for task in plan['tasks']:
            source = task['data_source']
            huc = task['huc_id']
            status = task['status']
            
            # By source
            if source not in by_source:
                by_source[source] = {'completed': 0, 'failed': 0, 'total': 0}
            by_source[source]['total'] += 1
            by_source[source][status] += 1
            
            # By HUC
            if huc not in by_huc:
                by_huc[huc] = {'completed': 0, 'failed': 0, 'total': 0}
            by_huc[huc]['total'] += 1
            by_huc[huc][status] += 1
        
        return {
            'status': 'executed',
            'overall': plan['metadata']['execution_summary'],
            'by_data_source': by_source,
            'by_huc': by_huc,
            'failed_tasks': [task for task in plan['tasks'] if task['status'] == 'failed']
        }
    
    def _get_expected_output_path(self, task: Dict) -> Optional[Path]:
        """Predict the expected output path for a task."""
        try:
            huc_id = task['huc_id']
            source = task['data_source']
            resolution = int(task['resolution'])
            
            if source == 'dem':
                return self.config.local_raster_base / "dem" / f"huc_{huc_id}_dem_{resolution}m_research_grade.tif"
            elif source == 'landsat':
                start_date = task['start_date']
                end_date = task['end_date']
                return self.config.local_raster_base / "landsat" / f"huc_{huc_id}_landsat_{resolution}m_{start_date}_{end_date}.tif"
            elif source == 'sar':
                start_date = task['start_date']
                end_date = task['end_date']
                return self.config.local_raster_base / "sentinel1" / f"huc_{huc_id}_sar_{resolution}m_{start_date}_{end_date}_median.tif"
            elif source == 'alphaearth':
                return self.config.local_raster_base / "alphaearth" / f"huc_{huc_id}_alphaearth_{resolution}m_2024_16bands.tif"
            elif source == 'references':
                return self.config.local_raster_base / "reference" / f"huc_{huc_id}_flow_direction_{resolution}m.tif"
        except Exception:
            return None
        return None

def main():
    """Demo the multi-source pipeline orchestrator."""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    try:
        # Initialize Earth Engine with project
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from config import Settings
        
        gee_settings = Settings()
        ee.Initialize(project=gee_settings.GEE_PROJECT_ID)
        print(f"✅ Initialized GEE with project: {gee_settings.GEE_PROJECT_ID}")
        
        # Initialize orchestrator
        config = LocalProcessingSettings()
        orchestrator = MultiSourcePipelineOrchestrator(config)
        
        # Demo with a comprehensive test case
        huc_ids = ["10020007"]  # Start with one HUC
        data_sources = ["dem", "landsat", "sar", "alphaearth"]  # All available sources
        
        # Use focused date range for proven Landsat processing (our tested range)
        test_start_date = "2023-07-01"  # Proven date range from our testing
        test_end_date = "2023-07-31"    # Single month for reliable results
        
        print("\n🚀 Multi-Source Pipeline Orchestrator Demo")
        print("=" * 60)
        print(f"📍 HUCs: {huc_ids}")
        print(f"🛰️  Data sources: {data_sources}")
        print(f"📅 Date range: {test_start_date} to {test_end_date} (proven test range)")
        print(f"📊 Expected bands: DEM(1), Landsat(7), SAR(6), AlphaEarth(64)")
        
        # Create processing plan
        plan = orchestrator.create_processing_plan(
            huc_ids=huc_ids,
            data_sources=data_sources,
            start_date=test_start_date,
            end_date=test_end_date
        )
        
        print(f"\n📋 Created plan with {plan['metadata']['total_tasks']} tasks")
        
        # Save plan
        plan_path = Path("multi_source_processing_plan.json")
        orchestrator.save_plan(plan, plan_path)
        
        # Execute plan
        print("\n⚡ Executing processing plan...")
        executed_plan = orchestrator.execute_plan(plan, max_workers=1)  # Single worker for demo
        
        # Show results
        summary = orchestrator.get_execution_summary(executed_plan)
        print(f"\n📊 Execution Summary:")
        print(f"   Success rate: {summary['overall']['success_rate']:.1f}%")
        print(f"   Total time: {summary['overall']['total_duration_seconds']:.1f}s")
        
        print(f"\n📈 Results by data source:")
        for source, stats in summary['by_data_source'].items():
            print(f"   {source}: {stats['completed']}/{stats['total']} completed")
        
        if summary['failed_tasks']:
            print(f"\n❌ Failed tasks:")
            for task in summary['failed_tasks']:
                print(f"   Task {task['task_id']}: {task['data_source']} for HUC {task['huc_id']} - {task['error']}")
        else:
            print(f"\n✅ All tasks completed successfully!")
        
        # Save final results
        final_plan_path = Path("multi_source_processing_results.json")
        orchestrator.save_plan(executed_plan, final_plan_path)
        
        print(f"\n🎉 Demo completed! Results saved to {final_plan_path}")
        
        # Show file summary
        print(f"\n📁 Generated files:")
        for task in executed_plan['tasks']:
            if task['status'] == 'completed' and task['output_path']:
                file_path = Path(task['output_path'])
                if file_path.exists():
                    size_mb = file_path.stat().st_size / (1024 * 1024)
                    print(f"   {task['data_source']}: {file_path.name} ({size_mb:.1f} MB)")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        
    except Exception as e:
        print(f"✗ Error: {e}")
        raise

if __name__ == "__main__":
    main()
