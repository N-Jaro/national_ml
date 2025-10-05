#!/bin/bash
# Quick test for Dofa Foundation Model

cd "$(dirname "$0")/.."

echo "🧪 Testing Dofa Foundation Model"
echo "============================================="
echo "Running minimal 2-epoch test..."
echo ""

./scripts/run_dofa.sh --test

echo ""
echo "Test completed for dofa!"
