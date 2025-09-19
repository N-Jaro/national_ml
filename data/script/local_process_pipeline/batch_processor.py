#!/usr/bin/env python3
"""
Batch Processing Script - Process multiple HUCs with all data sources.
Production-ready batch processing for the complete satellite data pipeline.
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import argparse

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ee
from config import Settings
from local_config import LocalProcessingSettings
from multi_source_orchestrator import MultiSourcePipelineOrchestrator


class BatchProcessor:
    """Batch processor for multi-HUC, multi-source satellite data processing."""
    
    def __init__(self):
        """Initialize the batch processor."""
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'batch_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize GEE
        gee_settings = Settings()
        ee.Initialize(project=gee_settings.GEE_PROJECT_ID)
        self.logger.info(f"✅ Initialized GEE with project: {gee_settings.GEE_PROJECT_ID}")
        
        # Initialize orchestrator
        config = LocalProcessingSettings()
        self.orchestrator = MultiSourcePipelineOrchestrator(config)
        
    def get_available_hucs(self) -> List[str]:
        """Get list of available HUC IDs for processing."""
        # For now, return a curated list of HUCs
        # In production, this could read from a database or file
        return [
            "10020007",  # Our test HUC
            "10020008",  
            "10020009",
            "10020010",
            # Add more HUCs as needed
        ]
    
    def process_single_huc(self, huc_id: str, data_sources: List[str], 
                          start_date: str, end_date: str, 
                          output_dir: Path, fast_mode: bool = True) -> Dict:
        """Process a single HUC with all specified data sources."""
        
        self.logger.info(f"🎯 Starting processing for HUC {huc_id} (fast_mode={fast_mode})")
        
        # Create processing plan for single HUC
        plan = self.orchestrator.create_processing_plan(
            huc_ids=[huc_id],
            data_sources=data_sources,
            start_date=start_date,
            end_date=end_date
        )
        
        # Save plan
        plan_path = output_dir / f"plan_huc_{huc_id}.json"
        self.orchestrator.save_plan(plan, plan_path)
        
        # Execute plan with fast mode
        max_workers = 4 if fast_mode else 1
        executed_plan = self.orchestrator.execute_plan(plan, max_workers=max_workers, fast_mode=fast_mode)
        
        # Save results
        results_path = output_dir / f"results_huc_{huc_id}.json"
        self.orchestrator.save_plan(executed_plan, results_path)
        
        # Get summary
        summary = self.orchestrator.get_execution_summary(executed_plan)
        
        self.logger.info(f"✅ HUC {huc_id} completed: {summary['overall']['success_rate']:.1f}% success")
        
        return {
            'huc_id': huc_id,
            'summary': summary,
            'plan_path': str(plan_path),
            'results_path': str(results_path)
        }
    
    def process_multiple_hucs(self, huc_ids: List[str], data_sources: List[str],
                             start_date: str, end_date: str,
                             output_dir: Path, fast_mode: bool = True,
                             max_concurrent_hucs: int = 1) -> Dict:
        """Process multiple HUCs with all specified data sources."""
        
        self.logger.info(f"🚀 Starting batch processing for {len(huc_ids)} HUCs")
        self.logger.info(f"📍 HUCs: {huc_ids}")
        self.logger.info(f"🛰️  Data sources: {data_sources}")
        self.logger.info(f"📅 Date range: {start_date} to {end_date}")
        
        # Create output directory
        output_dir.mkdir(exist_ok=True)
        
        batch_start = datetime.now()
        results = []
        
        # Process HUCs sequentially for now (can be parallelized later)
        for i, huc_id in enumerate(huc_ids, 1):
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"Processing HUC {i}/{len(huc_ids)}: {huc_id}")
            self.logger.info(f"{'='*60}")
            
            try:
                huc_result = self.process_single_huc(
                    huc_id, data_sources, start_date, end_date, output_dir, fast_mode
                )
                results.append(huc_result)
                
            except Exception as e:
                self.logger.error(f"❌ Failed to process HUC {huc_id}: {e}")
                results.append({
                    'huc_id': huc_id,
                    'error': str(e),
                    'summary': None
                })
        
        # Create batch summary
        batch_end = datetime.now()
        batch_duration = (batch_end - batch_start).total_seconds()
        
        successful_hucs = [r for r in results if 'error' not in r]
        failed_hucs = [r for r in results if 'error' in r]
        
        batch_summary = {
            'batch_info': {
                'start_time': batch_start.isoformat(),
                'end_time': batch_end.isoformat(),
                'duration_seconds': batch_duration,
                'total_hucs': len(huc_ids),
                'successful_hucs': len(successful_hucs),
                'failed_hucs': len(failed_hucs),
                'success_rate': (len(successful_hucs) / len(huc_ids)) * 100
            },
            'data_sources': data_sources,
            'date_range': {'start': start_date, 'end': end_date},
            'huc_results': results
        }
        
        # Save batch summary
        summary_path = output_dir / f"batch_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        import json
        with open(summary_path, 'w') as f:
            json.dump(batch_summary, f, indent=2)
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"🎉 BATCH PROCESSING COMPLETED")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"📊 Batch Summary:")
        self.logger.info(f"   Total time: {batch_duration:.1f} seconds")
        self.logger.info(f"   Success rate: {batch_summary['batch_info']['success_rate']:.1f}%")
        self.logger.info(f"   Successful HUCs: {len(successful_hucs)}/{len(huc_ids)}")
        self.logger.info(f"   Summary saved: {summary_path}")
        
        if failed_hucs:
            self.logger.warning(f"❌ Failed HUCs: {[r['huc_id'] for r in failed_hucs]}")
        
        return batch_summary

def main():
    """Main batch processing function."""
    
    parser = argparse.ArgumentParser(description='Batch process satellite data for multiple HUCs')
    parser.add_argument('--hucs', nargs='+', default=["10020007"], 
                       help='HUC IDs to process (default: 10020007)')
    parser.add_argument('--sources', nargs='+', 
                       default=["dem", "landsat", "sar", "alphaearth", "references"],
                       choices=["dem", "landsat", "sar", "alphaearth", "references"],
                       help='Data sources to process')
    parser.add_argument('--start-date', default="2023-06-01",
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default="2023-09-30", 
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-dir', default="batch_processing_results",
                       help='Output directory for results')
    parser.add_argument('--demo', action='store_true',
                       help='Run demo with single HUC and limited sources')
    parser.add_argument('--fast', action='store_true', default=True,
                       help='Enable fast mode (parallel processing, skip existing)')
    parser.add_argument('--skip-existing', action='store_true', default=True,
                       help='Skip processing if output files already exist')
    parser.add_argument('--workers', type=int, default=4,
                       help='Maximum number of concurrent workers')
    
    args = parser.parse_args()
    
    try:
        processor = BatchProcessor()
        
        if args.demo:
            print("🔬 Running DEMO mode...")
            huc_ids = ["10020007"]
            data_sources = ["dem", "landsat"]  # Limited for demo
        else:
            huc_ids = args.hucs
            data_sources = args.sources
        
        output_dir = Path(args.output_dir)
        
        # Run batch processing
        batch_summary = processor.process_multiple_hucs(
            huc_ids=huc_ids,
            data_sources=data_sources,
            start_date=args.start_date,
            end_date=args.end_date,
            output_dir=output_dir,
            fast_mode=args.fast
        )
        
        print(f"\n✅ Batch processing completed successfully!")
        print(f"📁 Results saved to: {output_dir}")
        
    except Exception as e:
        print(f"❌ Batch processing failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
