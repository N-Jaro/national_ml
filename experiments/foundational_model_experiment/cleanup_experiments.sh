#!/bin/bash
# cleanup_experiments.sh - Maintenance script for foundational model experiments

echo "🧹 Cleaning up foundational model experiments..."

# Remove temporary files
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find . -name ".pytest_cache" -type d -exec rm -rf {} + 2>/dev/null || true
find . -name "*.log" -delete 2>/dev/null || true

# Clean up wandb logs (keep recent ones)
if [ -d "wandb" ]; then
    echo "📊 Cleaning old wandb logs..."
    find wandb -name "run-*" -type d -mtime +7 -exec rm -rf {} + 2>/dev/null || true
fi

# Clean up lightning logs (keep recent ones)
if [ -d "lightning_logs" ]; then
    echo "⚡ Cleaning old lightning logs..."
    find lightning_logs -name "version_*" -type d -mtime +7 -exec rm -rf {} + 2>/dev/null || true
fi

# Clean up each experiment's wandb and output directories
for experiment in prithvi clay dofa scalemae; do
    if [ -d "$experiment" ]; then
        echo "🧪 Cleaning $experiment..."
        
        # Clean wandb runs older than 7 days
        if [ -d "$experiment/wandb" ]; then
            find "$experiment/wandb" -name "run-*" -type d -mtime +7 -exec rm -rf {} + 2>/dev/null || true
        fi
        
        # Clean old outputs
        if [ -d "$experiment/outputs" ]; then
            find "$experiment/outputs" -name "*.tmp" -delete 2>/dev/null || true
        fi
        
        # Clean old logs
        if [ -d "$experiment/logs" ]; then
            find "$experiment/logs" -name "*.log" -mtime +7 -delete 2>/dev/null || true
        fi
    fi
done

echo "✅ Cleanup completed!"
echo ""
echo "📁 Current directory structure:"
ls -la | grep "^d"
echo ""
echo "📈 Disk usage:"
du -sh . 2>/dev/null || echo "Unable to calculate disk usage"