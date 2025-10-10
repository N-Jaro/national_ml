#!/bin/bash
# submit_all_1huc_tests.sh
# Batch submission script for all model variant 1-HUC tests with W&B logging

echo "=== Submitting All Model Variant 1-HUC Tests ==="
echo "Purpose: Quick validation of all MDMT variants with optimal HUC split"
echo "Configuration: 1 train HUC (03160113) + 1 val HUC (16040204)"
echo "Batch size: 32, Epochs: 5, W&B: Online"
echo ""

cd /u/nathanj/national_ml/experiments/training

# Submit all test jobs
echo "Submitting test jobs..."

JOB1=$(sbatch --parsable test_all_modalities_alphaearth_1huc.sh)
echo "✅ All Modalities + AlphaEarth (73 ch): Job $JOB1"

JOB2=$(sbatch --parsable test_dem_optical_1huc.sh)
echo "✅ DEM + Optical (7 ch): Job $JOB2"

JOB3=$(sbatch --parsable test_dem_alphaearth_1huc.sh)
echo "✅ DEM + AlphaEarth (65 ch): Job $JOB3"

JOB4=$(sbatch --parsable test_alphaearth_only_1huc.sh)
echo "✅ AlphaEarth Only (64 ch): Job $JOB4"

JOB5=$(sbatch --parsable test_dem_only_1huc.sh)
echo "✅ DEM Only (1 ch): Job $JOB5"

JOB6=$(sbatch --parsable test_dem_sar_1huc.sh)
echo "✅ DEM + SAR (2 ch): Job $JOB6"

JOB7=$(sbatch --parsable test_dem_thermal_1huc.sh)
echo "✅ DEM + Thermal (2 ch): Job $JOB7"

echo ""
echo "=== All 7 Model Variants Submitted ==="
echo "Job IDs: $JOB1, $JOB2, $JOB3, $JOB4, $JOB5, $JOB6, $JOB7"
echo ""
echo "Monitor with: squeue -u nathanj"
echo "Check logs: tail -f slurm_logs*/test_*_1huc_*.out"
echo "W&B Dashboard: All runs will appear with '_1huc_test_' naming"
echo ""
echo "Expected runtime: ~10-15 minutes per job"
echo "Total estimated time: ~2 hours (parallel execution)"