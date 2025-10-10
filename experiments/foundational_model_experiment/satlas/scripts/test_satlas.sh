#!/bin/bash

echo "======================================================"
echo "Testing SATLAS Foundation Model"
echo "======================================================"

# Activate environment
source /u/nathanj/miniconda3/etc/profile.d/conda.sh
conda activate terratorch_env

# Navigate to SATLAS directory
cd /u/nathanj/national_ml/experiments/foundational_model_experiment/satlas

echo "Running comprehensive SATLAS evaluation..."
python scripts/evaluate_satlas_comprehensive.py

if [ $? -eq 0 ]; then
    echo "✅ SATLAS comprehensive test PASSED"
else
    echo "❌ SATLAS comprehensive test FAILED"
    exit 1
fi

echo "Running quick synthetic training test..."
python scripts/train_satlas_foundation.py \
    --config configs/satlas_test_config.yaml \
    --gpus 0 \
    --synthetic \
    --test \
    --offline

if [ $? -eq 0 ]; then
    echo "✅ SATLAS synthetic training test PASSED"
else
    echo "❌ SATLAS synthetic training test FAILED"
    exit 1
fi

echo "======================================================"
echo "🎉 All SATLAS tests PASSED!"
echo "======================================================"