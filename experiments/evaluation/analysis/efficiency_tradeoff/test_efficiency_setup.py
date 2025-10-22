#!/usr/bin/env python3
"""
Test Efficiency Analysis

Quick test script to validate the efficiency analysis setup and identify any issues
before running the full analysis.

Author: GitHub Copilot  
Date: October 2025
"""

import sys
import torch
from pathlib import Path

# Add necessary paths
sys.path.append('/u/nathanj/national_ml/experiments/models')
sys.path.append('/u/nathanj/national_ml/experiments/data')

def test_model_imports():
    """Test that we can import all required model classes"""
    print("Testing model imports...")
    
    try:
        from mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
        print("✓ AlphaEarth-only model imported successfully")
    except Exception as e:
        print(f"✗ AlphaEarth-only model import failed: {e}")
        return False
    
    try:
        from mdmt_dem_alphaearth import MultimodalMultitaskModel_DEM_AlphaEarth
        print("✓ DEM+AlphaEarth model imported successfully")
    except Exception as e:
        print(f"✗ DEM+AlphaEarth model import failed: {e}")
        return False
    
    try:
        from mdmt_all_modalities_alphaearth import MultimodalMultitaskModel_All_Modalities_AlphaEarth
        print("✓ All+AlphaEarth model imported successfully")
    except Exception as e:
        print(f"✗ All+AlphaEarth model import failed: {e}")
        return False
    
    return True

def test_model_instantiation():
    """Test that we can instantiate all models"""
    print("\nTesting model instantiation...")
    
    device = torch.device('cpu')  # Use CPU for testing
    
    # Test AlphaEarth-only
    try:
        from mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
        model = MultitaskModel_AlphaEarth_Only(alphaearth_channels=64)
        print("✓ AlphaEarth-only model instantiated successfully")
    except Exception as e:
        print(f"✗ AlphaEarth-only model instantiation failed: {e}")
        return False
    
    # Test DEM+AlphaEarth
    try:
        from mdmt_dem_alphaearth import MultimodalMultitaskModel_DEM_AlphaEarth
        model = MultimodalMultitaskModel_DEM_AlphaEarth(alphaearth_channels=64)
        print("✓ DEM+AlphaEarth model instantiated successfully")
    except Exception as e:
        print(f"✗ DEM+AlphaEarth model instantiation failed: {e}")
        return False
    
    # Test All+AlphaEarth
    try:
        from mdmt_all_modalities_alphaearth import MultimodalMultitaskModel_All_Modalities_AlphaEarth
        model = MultimodalMultitaskModel_All_Modalities_AlphaEarth()
        print("✓ All+AlphaEarth model instantiated successfully")
    except Exception as e:
        print(f"✗ All+AlphaEarth model instantiation failed: {e}")
        return False
    
    return True

def test_dummy_forward_pass():
    """Test dummy forward passes for all models"""
    print("\nTesting dummy forward passes...")
    
    device = torch.device('cpu')
    batch_size = 2
    input_size = (224, 224)
    
    # Test AlphaEarth-only
    try:
        from mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
        model = MultitaskModel_AlphaEarth_Only(alphaearth_channels=64)
        
        alphaearth_input = torch.randn(batch_size, 64, input_size[0], input_size[1])
        
        with torch.no_grad():
            outputs = model(alphaearth_input)  # Pass tensor directly, not dict
        
        print("✓ AlphaEarth-only forward pass successful")
        print(f"  Output shapes: {[out.shape for out in outputs]}")
        
    except Exception as e:
        print(f"✗ AlphaEarth-only forward pass failed: {e}")
        return False
    
    # Test DEM+AlphaEarth
    try:
        from mdmt_dem_alphaearth import MultimodalMultitaskModel_DEM_AlphaEarth
        model = MultimodalMultitaskModel_DEM_AlphaEarth(alphaearth_channels=64)
        
        dem_input = torch.randn(batch_size, 1, input_size[0], input_size[1])
        alphaearth_input = torch.randn(batch_size, 64, input_size[0], input_size[1])
        
        with torch.no_grad():
            outputs = model(dem_input, alphaearth_input)  # Pass as separate args
        
        print("✓ DEM+AlphaEarth forward pass successful") 
        print(f"  Output shapes: {[out.shape for out in outputs]}")
        
    except Exception as e:
        print(f"✗ DEM+AlphaEarth forward pass failed: {e}")
        return False
    
    # Test All+AlphaEarth  
    try:
        from mdmt_all_modalities_alphaearth import MultimodalMultitaskModel_All_Modalities_AlphaEarth
        model = MultimodalMultitaskModel_All_Modalities_AlphaEarth()  # No channel params needed
        
        dem_input = torch.randn(batch_size, 1, input_size[0], input_size[1])
        optical_input = torch.randn(batch_size, 6, input_size[0], input_size[1])
        thermal_input = torch.randn(batch_size, 1, input_size[0], input_size[1])
        sar_input = torch.randn(batch_size, 1, input_size[0], input_size[1])
        alphaearth_input = torch.randn(batch_size, 64, input_size[0], input_size[1])
        
        with torch.no_grad():
            outputs = model(dem_input, optical_input, thermal_input, sar_input, alphaearth_input)
        
        print("✓ All+AlphaEarth forward pass successful")
        print(f"  Output shapes: {[out.shape for out in outputs]}")
        
    except Exception as e:
        print(f"✗ All+AlphaEarth forward pass failed: {e}")
        return False
    
    return True

def test_parameter_counting():
    """Test parameter counting functionality"""
    print("\nTesting parameter counting...")
    
    try:
        from mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
        model = MultitaskModel_AlphaEarth_Only(alphaearth_channels=64)
        
        total_params = sum(p.numel() for p in model.parameters())
        trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
        
        print(f"✓ AlphaEarth-only parameters: {total_params:,} total, {trainable_params:,} trainable")
        
    except Exception as e:
        print(f"✗ Parameter counting failed: {e}")
        return False
    
    return True

def test_thop_import():
    """Test thop library import for FLOP counting"""
    print("\nTesting thop library...")
    
    try:
        from thop import profile, clever_format
        print("✓ thop library imported successfully")
        
        # Test basic usage
        from mdmt_alphaearth_only import MultitaskModel_AlphaEarth_Only
        model = MultitaskModel_AlphaEarth_Only(alphaearth_channels=64)
        
        inputs = torch.randn(1, 64, 224, 224)
        flops, params = profile(model, inputs=(inputs,), verbose=False)
        flops_str, params_str = clever_format([flops, params], "%.3f")
        
        print(f"✓ thop FLOP estimation successful: {flops_str}, {params_str}")
        
    except ImportError:
        print("✗ thop library not available - will use alternative FLOP estimation")
        return False
    except Exception as e:
        print(f"✗ thop testing failed: {e}")
        return False
    
    return True

def test_performance_data_collection():
    """Test performance data collection"""
    print("\nTesting performance data collection...")
    
    try:
        from collect_performance_data import PerformanceCollector
        collector = PerformanceCollector()
        
        # Test DEM sanity data collection
        dem_data = collector.collect_from_dem_sanity()
        print(f"✓ DEM sanity data collection: {len(dem_data)} models found")
        
        # Test estimation
        estimated_data = collector.estimate_missing_performance(dem_data)
        print(f"✓ Performance estimation: {len(estimated_data)} models total")
        
    except Exception as e:
        print(f"✗ Performance data collection failed: {e}")
        return False
    
    return True

def main():
    """Run all tests"""
    print("=== Efficiency Analysis Test Suite ===")
    
    tests = [
        test_model_imports,
        test_model_instantiation, 
        test_dummy_forward_pass,
        test_parameter_counting,
        test_thop_import,
        test_performance_data_collection
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test_func.__name__} crashed: {e}")
    
    print(f"\n=== Test Results ===")
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("✓ All tests passed! Ready to run efficiency analysis.")
        return True
    else:
        print("✗ Some tests failed. Please fix issues before running full analysis.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)