# Model Efficiency Tradeoff Analysis

This directory contains scripts and results for analyzing the computational efficiency tradeoffs between different model variants in the National ML project.

## Overview

The efficiency analysis evaluates three key model variants:
- **AlphaEarth-only**: Single modality using Google satellite embeddings
- **DEM+AlphaEarth**: Two modalities combining elevation and satellite embeddings  
- **All+AlphaEarth**: Full multimodal model with DEM, optical, thermal, SAR, and AlphaEarth

The analysis determines the optimal "sweet spot" that balances accuracy with computational cost.

## Key Questions Addressed

1. **Does adding DEM to AlphaEarth justify the extra computational cost?**
2. **What is the accuracy-per-compute sweet spot among the three variants?**
3. **How do parameter count and FLOPs scale with performance gains?**
4. **Which model provides the best efficiency for production deployment?**

## Scripts

### Core Analysis Scripts

- **`efficiency_analysis.py`**: Main analysis script that calculates parameters, FLOPs, and creates visualizations
- **`collect_performance_data.py`**: Gathers actual performance metrics from training results and evaluations
- **`test_efficiency_setup.py`**: Test suite to validate setup before running full analysis

### Usage

1. **Test the setup** (recommended first):
   ```bash
   cd /u/nathanj/national_ml/experiments/evaluation/analysis/efficiency_tradeoff
   python test_efficiency_setup.py
   ```

2. **Collect performance data**:
   ```bash
   python collect_performance_data.py
   ```

3. **Run full efficiency analysis**:
   ```bash
   python efficiency_analysis.py
   ```

## Analysis Components

### 1. Parameter Counting
- Total trainable parameters for each model variant
- Memory requirements estimation
- Model size comparison

### 2. FLOP Estimation  
- Forward pass computational cost using thop library
- Inference speed implications
- Computational resource requirements

### 3. Performance Metrics
- Water segmentation IoU from actual model evaluations
- Data sources: DEM sanity analysis, training logs, multitask benefit results
- Fallback estimates for missing data

### 4. Efficiency Metrics
- **IoU per Million Parameters**: Performance efficiency relative to model size
- **IoU per GFLOP**: Performance efficiency relative to computational cost
- **Composite Efficiency Score**: Normalized combination of performance, parameter efficiency, and FLOP efficiency

## Visualizations

The analysis produces several key visualizations:

### 1. Pareto Efficiency Plots (2x2 grid)
- **Performance vs Parameters**: Shows accuracy-model size tradeoff
- **Performance vs FLOPs**: Shows accuracy-computation tradeoff  
- **Parameter Efficiency**: IoU per million parameters bar chart
- **FLOP Efficiency**: IoU per GFLOP bar chart

### 2. Sweet Spot Analysis
- Bubble plot where bubble size represents efficiency score
- Identifies the optimal accuracy-per-compute balance
- Highlights the recommended model with golden star

## Expected Results

Based on preliminary analysis and DEM sanity results:

### Performance Ranking (IoU)
1. **All+AlphaEarth**: ~0.450 (estimated)
2. **DEM+AlphaEarth**: 0.432 (from DEM sanity)
3. **AlphaEarth-only**: 0.420 (from DEM sanity)

### Efficiency Expectations
- **AlphaEarth-only**: Highest parameter efficiency, moderate FLOP efficiency
- **DEM+AlphaEarth**: Balanced efficiency, likely sweet spot candidate
- **All+AlphaEarth**: Lowest efficiency but highest absolute performance

## Output Files

The analysis generates timestamped results in the `results/` subdirectory:

- **`efficiency_analysis_YYYYMMDD_HHMMSS.csv`**: Raw numerical results
- **`efficiency_tradeoff_analysis_YYYYMMDD_HHMMSS.png`**: Pareto efficiency plots
- **`sweet_spot_analysis_YYYYMMDD_HHMMSS.png`**: Sweet spot visualization
- **`efficiency_analysis_report_YYYYMMDD_HHMMSS.md`**: Comprehensive analysis report
- **`performance_data.json`**: Collected performance metrics from all sources

## Technical Details

### Model Architectures
All models use U-Net style architectures with:
- Hierarchical attention fusion for multi-modal variants
- Task-specific decoders for water segmentation and flow direction
- Dynamic loss weighting with uncertainty estimation

### Input Specifications
- **AlphaEarth-only**: 64 channels (Google satellite embeddings)
- **DEM+AlphaEarth**: 1 + 64 = 65 channels total
- **All+AlphaEarth**: 1 + 6 + 1 + 1 + 64 = 73 channels total

### Computational Environment
- Input size: 224×224 pixels
- Batch size: 1 (for FLOP estimation)
- Device: CUDA if available, CPU fallback
- Framework: PyTorch with Lightning

## Integration with Paper

This analysis directly supports the paper's efficiency justification:

### Key Messages for Publication
1. **"DEM+AlphaEarth represents the optimal accuracy-per-compute sweet spot"**
2. **"Adding DEM to AlphaEarth provides X% performance improvement for only Y% computational overhead"**
3. **"All-modality models show diminishing returns relative to computational cost"**

### Figure for Paper
The sweet spot analysis visualization can be directly included in the paper to show:
- Clear Pareto frontier between the three model variants
- DEM+AlphaEarth as the optimal balance point
- Quantitative justification for model selection

## Troubleshooting

### Common Issues

1. **thop library not available**: 
   - Install with `pip install thop`
   - Analysis will use fallback FLOP estimation if unavailable

2. **Model import errors**:
   - Ensure you're in the correct conda environment (`pytorch_gpu_cu118`)
   - Check that model files exist in `/experiments/models/`

3. **No performance data found**:
   - Analysis will use fallback estimates based on DEM sanity results
   - Run actual model training to get real performance numbers

4. **CUDA out of memory**:
   - Analysis automatically falls back to CPU if CUDA unavailable
   - Reduce batch size if needed (currently set to 1)

### Dependencies

Required packages:
- torch
- pandas  
- matplotlib
- seaborn
- numpy
- thop (optional, for FLOP counting)

## Future Extensions

Potential enhancements:
1. **Memory usage analysis**: Peak GPU memory consumption during training/inference
2. **Temporal efficiency**: Training time comparisons across variants
3. **Multi-scale analysis**: Efficiency at different input resolutions
4. **Hardware-specific optimization**: Efficiency on different GPU architectures

## Contact

For questions about the efficiency analysis:
- Check the comprehensive report generated by the analysis
- Review test output for diagnostic information
- Examine the visualization plots for insights