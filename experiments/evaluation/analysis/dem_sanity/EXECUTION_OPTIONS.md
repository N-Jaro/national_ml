# DEM Sanity Analysis - Execution Options

## 📊 **No, you don't need to run all 67 HUCs!**

For the DEM sanity analysis, you have **multiple execution options** depending on your needs. Since this analysis is proving a **methodological point** (that DEM provides topographic signal), a smaller representative subset is often sufficient.

## 🎯 **Available Options**

### 1. **Development/Testing** (⚡ Fastest - 10-15 min)
```bash
# Quick validation and debugging
sbatch run_dem_sanity_dev.sh
# OR
python quick_test_dem_sanity.py
```
- **HUCs**: 3 (minimal set)
- **Purpose**: Initial testing, debugging, proof-of-concept
- **Runtime**: ~10-15 minutes
- **Resource**: 0.5 hours, 50GB RAM, 8 cores

### 2. **Representative Analysis** (⚖️ Balanced - 30-45 min)
```bash
# Scientifically robust with faster runtime
sbatch run_dem_sanity_quick.sh
```
- **HUCs**: 10 (strategic geographic sample)
- **Purpose**: Publication-ready results with statistical significance
- **Runtime**: ~30-45 minutes  
- **Resource**: 1 hour, 100GB RAM, 16 cores

### 3. **Full Evaluation** (🔬 Complete - 2-4 hours)
```bash
# Maximum statistical power
sbatch run_dem_sanity_analysis.sh
```
- **HUCs**: 67 (complete test set)
- **Purpose**: Maximum statistical confidence
- **Runtime**: ~2-4 hours
- **Resource**: 4 hours, 100GB RAM, 16 cores

## 🗺️ **HUC Coverage Comparison**

| Option | HUCs | Coverage | Scientific Value |
|--------|------|----------|------------------|
| Development | 3 | Basic testing | Proof-of-concept |
| Representative | 10 | Geographic diversity | **Publication-ready** ⭐ |
| Full | 67 | Complete national | Maximum confidence |

### Representative HUCs (Recommended)
The 10-HUC set covers all major US climate zones and physiographic regions:
- **Eastern US**: Humid continental (01030003)
- **Southeast**: Humid subtropical (02080201)  
- **Great Plains**: Semi-arid (08050002)
- **Western Mountains**: Alpine (14080106)
- **Pacific Northwest**: Oceanic (17020006)
- **California**: Mediterranean (18090208)
- **Southwest**: Arid desert (15030104)
- **Great Lakes**: Continental (04050001)
- **Texas**: Subtropical (12060201)
- **Alaska**: Subarctic (19020103)

## 📈 **Expected Results (All Options)**

All options should show the same **performance pattern**:
```
DEM+AlphaEarth > SmoothedDEM+AE > ConstantDEM+AE > AlphaEarth-only
```

**Key differences:**
- **Development**: May show more variance due to small sample
- **Representative**: Statistically robust, good for publication
- **Full**: Maximum confidence intervals, but diminishing returns

## 🎯 **Recommendation**

For **publication purposes**, use the **Representative Analysis** (`run_dem_sanity_quick.sh`):

✅ **Why it's ideal:**
- Covers all major US physiographic regions
- Statistically significant (n=10 HUCs)
- Fast runtime (~30-45 minutes)
- Publication-ready results
- Good balance of coverage vs efficiency

## 🚀 **Quick Start**

```bash
# Activate environment
conda activate pytorch_gpu_cu118

# Go to analysis directory
cd /u/nathanj/national_ml/experiments/evaluation/analysis/dem_sanity/

# Submit representative analysis (RECOMMENDED)
sbatch run_dem_sanity_quick.sh

# OR quick test first
python quick_test_dem_sanity.py
```

## 📊 **Scientific Justification**

The DEM sanity analysis is testing a **methodological hypothesis**: whether DEM provides meaningful topographic signal. This is:

- **Not dependent on sample size** for the core finding
- **More about showing the pattern exists** than precise quantification
- **Generalizable across regions** if geographic diversity is maintained

Therefore, the **representative 10-HUC analysis provides sufficient scientific evidence** while being much more efficient than the full 67-HUC evaluation.

## 🔍 **When to Use Full Analysis**

Use the full 67-HUC analysis only if:
- You need maximum statistical confidence for peer review
- You're doing a comprehensive methods paper
- You have extra computational resources and time
- Reviewers specifically request full evaluation

For most publication purposes, the representative analysis is sufficient and preferred.