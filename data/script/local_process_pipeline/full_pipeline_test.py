#!/usr/bin/env python3
"""
Full Multi-Source Pipeline Test - Comprehensive test of all data sources for a HUC region.
This validates the complete pipeline with DEM, Landsat, SAR, and orchestration.
"""

import ee
import logging
import time
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import rasterio
import numpy as np

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from local_config import LocalProcessingSettings
from data_manager import LocalDataManager
from gee_bulk_downloader import GEEBulkDownloader
from landsat_bulk_downloader import LandsatBulkDownloader
from sar_bulk_downloader import SARBulkDownloader

class FullPipelineTest:
    """Comprehensive test of the complete multi-source pipeline."""
    
    def __init__(self):
        """Initialize the test suite."""
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.config = LocalProcessingSettings()
        self.data_manager = LocalDataManager(self.config)
        
        # Initialize downloaders
        self.dem_downloader = GEEBulkDownloader(self.config, self.data_manager)
        self.landsat_downloader = LandsatBulkDownloader(self.config, self.data_manager)
        self.sar_downloader = SARBulkDownloader(self.config, self.data_manager)
        
        # Test parameters
        self.test_huc = "10020007"  # Our proven HUC
        self.test_start_date = "2023-06-01"
        self.test_end_date = "2023-09-30"
        
        # Results tracking
        self.test_results = {
            'test_id': f"full_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'huc_id': self.test_huc,
            'start_time': None,
            'end_time': None,
            'total_duration': None,
            'data_sources': {},
            'validation': {},
            'overall_status': 'pending'
        }
    
    def test_individual_data_source(self, source_name: str, downloader, **kwargs) -> dict:
        """Test an individual data source."""
        
        self.logger.info(f"Testing {source_name} data source...")
        
        source_start = time.time()
        source_result = {
            'status': 'pending',
            'start_time': source_start,
            'duration': None,
            'output_path': None,
            'file_size_mb': None,
            'error': None,
            'validation': {}
        }
        
        try:
            # Call the appropriate downloader method
            if source_name == 'dem':
                output_path = downloader.download_dem_for_huc_research_grade(
                    self.test_huc, 
                    buffer_meters=kwargs.get('buffer_meters', 5000)
                )
            elif source_name == 'landsat':
                output_path = downloader.download_landsat_for_huc(
                    self.test_huc,
                    self.test_start_date,
                    self.test_end_date,
                    resolution=kwargs.get('resolution', 30.0)
                )
            elif source_name == 'sar':
                output_path = downloader.download_sar_for_huc(
                    self.test_huc,
                    self.test_start_date,
                    self.test_end_date,
                    resolution=kwargs.get('resolution', 10.0)
                )
            else:
                raise ValueError(f"Unknown data source: {source_name}")
            
            # Calculate duration and file size
            source_result['duration'] = time.time() - source_start
            source_result['output_path'] = output_path
            
            if Path(output_path).exists():
                source_result['file_size_mb'] = Path(output_path).stat().st_size / (1024 * 1024)
                source_result['status'] = 'completed'
                
                # Validate the output file
                source_result['validation'] = self.validate_output_file(output_path, source_name)
                
            else:
                source_result['status'] = 'failed'
                source_result['error'] = f"Output file not created: {output_path}"
            
        except Exception as e:
            source_result['status'] = 'failed'
            source_result['error'] = str(e)
            source_result['duration'] = time.time() - source_start
            self.logger.error(f"{source_name} test failed: {e}")
        
        return source_result
    
    def validate_output_file(self, file_path: str, source_name: str) -> dict:
        """Validate an output raster file."""
        
        validation = {
            'file_readable': False,
            'dimensions': None,
            'band_count': None,
            'data_type': None,
            'crs': None,
            'resolution': None,
            'has_valid_data': False,
            'data_range': None,
            'nodata_percentage': None
        }
        
        try:
            with rasterio.open(file_path) as src:
                validation['file_readable'] = True
                validation['dimensions'] = [src.width, src.height]
                validation['band_count'] = src.count
                validation['data_type'] = str(src.dtypes[0])
                validation['crs'] = str(src.crs)
                validation['resolution'] = [abs(src.transform[0]), abs(src.transform[4])]
                
                # Read a sample for data validation
                sample_size = min(1000, src.width, src.height)
                sample = src.read(1, window=((0, sample_size), (0, sample_size)))
                
                # Check for valid data
                if src.nodata is not None:
                    valid_data = sample[sample != src.nodata]
                    nodata_count = np.sum(sample == src.nodata)
                    validation['nodata_percentage'] = (nodata_count / sample.size) * 100
                else:
                    valid_data = sample[~np.isnan(sample)]
                    validation['nodata_percentage'] = (np.sum(np.isnan(sample)) / sample.size) * 100
                
                if len(valid_data) > 0:
                    validation['has_valid_data'] = True
                    validation['data_range'] = [float(np.min(valid_data)), float(np.max(valid_data))]
                
                self.logger.info(f"{source_name} validation: {validation['band_count']} bands, "
                               f"{validation['dimensions'][0]}x{validation['dimensions'][1]} pixels, "
                               f"{validation['resolution'][0]:.1f}m resolution")
                
        except Exception as e:
            validation['error'] = str(e)
            self.logger.error(f"Failed to validate {source_name} file: {e}")
        
        return validation
    
    def run_full_test(self) -> dict:
        """Run the complete multi-source pipeline test."""
        
        print("🚀 FULL MULTI-SOURCE PIPELINE TEST")
        print("=" * 60)
        print(f"HUC Region: {self.test_huc}")
        print(f"Date Range: {self.test_start_date} to {self.test_end_date}")
        print(f"Test ID: {self.test_results['test_id']}")
        print()
        
        self.test_results['start_time'] = time.time()
        
        # Test each data source
        data_sources = [
            ('dem', self.dem_downloader, {'resolution': 10.0}),
            ('landsat', self.landsat_downloader, {'resolution': 30.0}),
            ('sar', self.sar_downloader, {'resolution': 10.0})
        ]
        
        for source_name, downloader, kwargs in data_sources:
            print(f"🔄 Testing {source_name.upper()} data source...")
            
            source_result = self.test_individual_data_source(source_name, downloader, **kwargs)
            self.test_results['data_sources'][source_name] = source_result
            
            # Print immediate results
            if source_result['status'] == 'completed':
                print(f"   ✅ SUCCESS: {source_result['file_size_mb']:.1f}MB in {source_result['duration']:.1f}s")
                if source_result['validation']['has_valid_data']:
                    data_range = source_result['validation']['data_range']
                    print(f"   📊 Data range: {data_range[0]:.2f} to {data_range[1]:.2f}")
                    print(f"   📐 Resolution: {source_result['validation']['resolution'][0]:.1f}m")
            else:
                print(f"   ❌ FAILED: {source_result['error']}")
            print()
        
        # Calculate overall results
        self.test_results['end_time'] = time.time()
        self.test_results['total_duration'] = self.test_results['end_time'] - self.test_results['start_time']
        
        # Determine overall status
        completed_sources = sum(1 for result in self.test_results['data_sources'].values() 
                              if result['status'] == 'completed')
        total_sources = len(self.test_results['data_sources'])
        
        if completed_sources == total_sources:
            self.test_results['overall_status'] = 'all_successful'
        elif completed_sources > 0:
            self.test_results['overall_status'] = 'partially_successful'
        else:
            self.test_results['overall_status'] = 'all_failed'
        
        return self.test_results
    
    def generate_test_report(self, results: dict):
        """Generate a comprehensive test report."""
        
        print("📊 COMPREHENSIVE TEST REPORT")
        print("=" * 60)
        
        # Overall summary
        print(f"Test Duration: {results['total_duration']:.1f} seconds")
        print(f"Overall Status: {results['overall_status'].upper()}")
        print()
        
        # Data source summary
        print("Data Source Results:")
        print("-" * 30)
        
        total_size_mb = 0
        total_files = 0
        
        for source_name, source_result in results['data_sources'].items():
            status_emoji = "✅" if source_result['status'] == 'completed' else "❌"
            
            print(f"{status_emoji} {source_name.upper()}")
            print(f"   Duration: {source_result['duration']:.1f}s")
            
            if source_result['status'] == 'completed':
                print(f"   File size: {source_result['file_size_mb']:.1f}MB")
                print(f"   Dimensions: {source_result['validation']['dimensions'][0]}x{source_result['validation']['dimensions'][1]}")
                print(f"   Bands: {source_result['validation']['band_count']}")
                print(f"   Resolution: {source_result['validation']['resolution'][0]:.1f}m")
                print(f"   Valid data: {'Yes' if source_result['validation']['has_valid_data'] else 'No'}")
                
                total_size_mb += source_result['file_size_mb']
                total_files += 1
            else:
                print(f"   Error: {source_result['error']}")
            print()
        
        # Overall statistics
        print("Overall Statistics:")
        print("-" * 20)
        print(f"Total files created: {total_files}")
        print(f"Total data volume: {total_size_mb:.1f}MB")
        print(f"Average file size: {total_size_mb/max(total_files, 1):.1f}MB")
        print()
        
        # Performance assessment
        print("Performance Assessment:")
        print("-" * 25)
        
        if results['overall_status'] == 'all_successful':
            print("🎉 EXCELLENT: All data sources processed successfully!")
            print("   ✅ Pipeline is fully operational for multi-source processing")
            print("   ✅ Ready for production deployment at scale")
        elif results['overall_status'] == 'partially_successful':
            print("⚡ GOOD: Some data sources successful, others need attention")
            print("   ✅ Core pipeline architecture is working")
            print("   🔧 Some data sources need debugging")
        else:
            print("🔧 NEEDS WORK: Pipeline issues need to be resolved")
            print("   ❌ Core components may need debugging")
        
        print()
        
        # Resource utilization
        throughput_mb_per_sec = total_size_mb / results['total_duration']
        print(f"Data Throughput: {throughput_mb_per_sec:.2f} MB/second")
        
        # Save detailed results
        output_path = Path(f"full_pipeline_test_{results['test_id']}.json")
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"📁 Detailed results saved to: {output_path}")

def main():
    """Run the full pipeline test."""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize Earth Engine with project
        from config import Settings
        gee_settings = Settings()
        ee.Initialize(project=gee_settings.GEE_PROJECT_ID)
        print(f"✅ Initialized GEE with project: {gee_settings.GEE_PROJECT_ID}")
        
        # Run the test
        tester = FullPipelineTest()
        results = tester.run_full_test()
        
        # Generate report
        tester.generate_test_report(results)
        
        print("\n" + "="*60)
        print("🏁 FULL PIPELINE TEST COMPLETED!")
        
        if results['overall_status'] == 'all_successful':
            print("🎉 ALL SYSTEMS GO! Pipeline ready for production!")
        else:
            print("🔧 Some components need attention - see report above")
        
        print("="*60)
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        raise

if __name__ == "__main__":
    main()
