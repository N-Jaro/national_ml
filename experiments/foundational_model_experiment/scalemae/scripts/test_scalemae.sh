#!/bin/bash
# Quick test for Scalemae Foundation Model

cd "$(dirname "$0")/.."

echo "🧪 Testing Scalemae Foundation Model"
echo "============================================="
echo "Running minimal 2-epoch test..."
echo ""

./scripts/run_scalemae.sh --test

echo ""
echo "Test completed for scalemae!"
