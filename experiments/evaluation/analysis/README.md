# Analysis Scripts

This directory contains specialized analysis scripts for the National ML project's multimodal deep learning models.

## Available Analyses

### 🔬 DEM Sanity Analysis (`dem_sanity/`)

**Purpose**: Prove that DEM provides meaningful physical topographic signal rather than just acting as "extra channels".

**What it does**: 
- Systematically degrades topographic information in DEM inputs
- Compares performance across: Original DEM → Smoothed DEM → Constant DEM → No DEM
- Generates publication-ready metrics and visualizations

**Key files**:
- `dem_sanity_analysis.py` - Main analysis script
- `quick_test_dem_sanity.py` - Interactive testing version
- `run_dem_sanity_analysis.sh` - SLURM job submission
- `README_DEM_Sanity.md` - Complete documentation

**Usage**:
```bash
cd dem_sanity/
python quick_test_dem_sanity.py  # Quick test
# OR
sbatch run_dem_sanity_analysis.sh  # Full analysis
```

## Adding New Analyses

When adding new analysis types, create a dedicated subdirectory with:

1. **Main analysis script** - Core analysis implementation
2. **Quick test script** - Interactive/development version
3. **SLURM submission script** - For production runs
4. **README** - Documentation and usage instructions
5. **slurm_logs/** - Directory for job outputs

### Naming Convention
- Folder: `analysis_type/` (e.g., `modality_ablation/`, `attention_analysis/`)
- Main script: `analysis_type_analysis.py`
- Quick test: `quick_test_analysis_type.py`
- SLURM job: `run_analysis_type_analysis.sh`
- README: `README_AnalysisType.md`

### Example Structure
```
analysis/
├── README.md                    # This file
├── analysis_type/
│   ├── analysis_type_analysis.py
│   ├── quick_test_analysis_type.py
│   ├── run_analysis_type_analysis.sh
│   ├── README_AnalysisType.md
│   └── slurm_logs/
└── dem_sanity/                  # Existing DEM analysis
    ├── dem_sanity_analysis.py
    ├── quick_test_dem_sanity.py
    ├── run_dem_sanity_analysis.sh
    ├── README_DEM_Sanity.md
    └── slurm_logs/
```

## Common Patterns

### Project Path Setup
```python
# For scripts in analysis/subdir/
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
```

### Model Loading
```python
from models.mdmt_variant import ModelClass
from data.patchDataLoader_variant import DatasetClass
```

### Result Organization
- Save results with timestamps: `analysis_YYYYMMDD_HHMMSS.{csv,json}`
- Create publication-ready plots
- Include statistical significance testing
- Generate summary reports

## Dependencies

All analyses should work with the main project environment:
```bash
conda activate pytorch_gpu_cu118
```

Standard imports for most analyses:
- `torch` - Deep learning framework
- `pandas` - Data manipulation
- `numpy` - Numerical computing
- `matplotlib/seaborn` - Visualization
- `sklearn` - Metrics and evaluation
- `tqdm` - Progress bars