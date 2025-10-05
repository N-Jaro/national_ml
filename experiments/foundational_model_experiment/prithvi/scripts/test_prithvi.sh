#!/bin/bash
# Quick test for Prithvi Foundation Model

cd "$(dirname "$0")/.."

echo "🧪 Testing Prithvi Foundation Model"
echo "============================================="
echo "Running minimal 2-epoch test..."
echo ""

./scripts/run_prithvi.sh --test

echo ""
echo "Test completed for prithvi!"
