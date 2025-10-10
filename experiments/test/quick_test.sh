#!/bin/bash
# Quick MDMT Model Test Script
# 
# This script provides quick commands for testing MDMT model variants
# Usage: ./quick_test.sh [variant] [mode]

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" 
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [[ ! -f "run_all_tests.py" ]]; then
    print_error "Must be run from the test directory"
    print_status "Navigate to: /u/nathanj/national_ml/experiments/test"
    exit 1
fi

# Default values
VARIANT=""
MODE="demo"

# Parse arguments
case "$1" in
    "all"|"")
        VARIANT=""
        ;;
    "all_modalities_alphaearth"|"comprehensive"|"full")
        VARIANT="all_modalities_alphaearth"
        ;;
    "dem_optical"|"basic")
        VARIANT="dem_optical"
        ;;
    "dem_alphaearth"|"foundation")
        VARIANT="dem_alphaearth"
        ;;
    "alphaearth_only"|"alphaearth"|"ae")
        VARIANT="alphaearth_only"
        ;;
    "dem_thermal"|"thermal")
        VARIANT="dem_thermal"
        ;;
    "dem_sar"|"sar")
        VARIANT="dem_sar"
        ;;
    *)
        print_error "Unknown variant: $1"
        echo "Available variants:"
        echo "  all                    - Run all variants"
        echo "  all_modalities_alphaearth - Comprehensive model (73 channels)"
        echo "  dem_optical           - Basic model (7 channels)"
        echo "  dem_alphaearth        - Foundation model (65 channels)"
        echo "  alphaearth_only       - AlphaEarth only (64 channels)"
        echo "  dem_thermal           - DEM + Thermal (2 channels)"
        echo "  dem_sar               - DEM + SAR (2 channels)"
        exit 1
        ;;
esac

case "$2" in
    "demo"|"arch"|"architecture"|"")
        MODE="demo"
        ;;
    "full"|"complete"|"data")
        MODE="full"
        ;;
    *)
        print_error "Unknown mode: $2"
        echo "Available modes:"
        echo "  demo - Architecture tests only (fast, no data required)"
        echo "  full - Complete tests (architecture + data loading + end-to-end)"
        exit 1
        ;;
esac

# Print test configuration
print_status "MDMT Model Test Configuration"
echo "  Variant: ${VARIANT:-'All variants'}"
echo "  Mode: $MODE"
echo "  Directory: $(pwd)"
echo ""

# Check environment
print_status "Checking environment..."
if ! python -c "import torch" 2>/dev/null; then
    print_error "PyTorch not available. Activate the conda environment:"
    echo "  conda activate pytorch_gpu_cu118"
    exit 1
fi

CUDA_AVAILABLE=$(python -c "import torch; print(torch.cuda.is_available())" 2>/dev/null || echo "false")
if [[ "$CUDA_AVAILABLE" == "True" ]]; then
    print_success "CUDA available"
else
    print_warning "CUDA not available, using CPU"
fi

# Build command
CMD="python run_all_tests.py"

if [[ "$MODE" == "demo" ]]; then
    CMD="$CMD --demo_only"
fi

if [[ -n "$VARIANT" ]]; then
    CMD="$CMD --variant $VARIANT"
fi

CMD="$CMD --verbose"

print_status "Running command: $CMD"
echo ""

# Run the test
if $CMD; then
    print_success "Tests completed successfully!"
    echo ""
    echo "Next steps:"
    if [[ "$MODE" == "demo" ]]; then
        echo "  - Run full tests: ./quick_test.sh $1 full"
    fi
    echo "  - Start training: cd ../training && sbatch submit_train_[variant]_single.sh"
else
    print_error "Tests failed!"
    echo ""
    echo "Troubleshooting:"
    echo "  - Check conda environment: conda activate pytorch_gpu_cu118"
    echo "  - Run with demo mode: ./quick_test.sh $1 demo"
    echo "  - Check individual test: python test_[variant].py --demo_only"
    exit 1
fi