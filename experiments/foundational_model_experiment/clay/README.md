# Clay Foundation Model Fine-tuning Experiment

## 📋 Overview

This experiment **fine-tunes the Clay foundation model** for water segmentation using 4-modal satellite data (DEM + optical + thermal + SAR). Following the same pattern as the Prithvi fine-tuning experiment.

**Paper:** https://arxiv.org/abs/2404.15366  
**Model Type:** `timm_clay_v1_base`  
**Approach:** Fine-tuning pre-trained weights with intelligent channel initialization

## 🚀 Quick Start

### Run Full Training
```bash
./scripts/run_clay.sh
```

### Run Quick Test (2 epochs, 2 HUCs)
```bash
./scripts/test_clay.sh
```

### Custom Configuration
```bash
./scripts/run_clay.sh --config configs/custom_config.yaml --gpus 2
```

## 📁 Directory Structure

```
clay_experiment/
├── configs/
│   └── clay_config.yaml     # Model configuration
├── training/
│   └── train_clay.py        # Training script
├── data/
│   └── (data adapters if needed)
├── outputs/
│   ├── models/                      # Saved models
│   ├── figures/                     # Plots and visualizations  
│   └── logs/                        # Training logs
├── scripts/
│   ├── run_clay.sh          # Main training script
│   └── test_clay.sh         # Quick test script
└── README.md                        # This file
```

## ⚙️ Configuration

Key configuration sections in `configs/clay_config.yaml`:

### Data
- **50 HUCs** for comprehensive evaluation
- **4 modalities:** DEM, optical (6-band), thermal, SAR
- **90/10 train/val split** with patch-level splitting

### Model  
- **Foundation Model:** timm_clay_v1_base
- **Pre-trained:** Yes, with intelligent 9-channel weight initialization
- **Decoder:** FCN decoder for segmentation  
- **Input Channels:** 9 (1 DEM + 6 optical + 1 thermal + 1 SAR)
- **Output:** Binary water/non-water segmentation

### Fine-tuning Strategy
- **Weight Initialization:** Intelligent mapping from pre-trained weights to 9 channels
  - DEM: Average of RGB channels
  - Optical 1-6: Original pre-trained optical/RGB weights  
  - Thermal: NIR/RED channel weights
  - SAR: SWIR/GREEN channel weights
- **Learning Rate:** 1e-5 (conservative for fine-tuning)
- **Warmup:** 20 epochs linear warmup
- **Scheduler:** Cosine annealing after warmup

### Training
- **Optimizer:** AdamW with cosine annealing
- **Epochs:** 500 with early stopping (patience=30)
- **Batch Size:** 8 (matching Prithvi configuration)
- **Loss:** CombinedFocalDiceLoss (focal_weight=0.7, dice_weight=0.3)

## 📊 Monitoring

### WandB Integration
- **Project:** `clay_water_segmentation`
- **Metrics:** IoU, accuracy, F1-score, loss
- **Tracking:** Gradients, model artifacts, hyperparameters

### Metrics
- **Primary:** IoU (Intersection over Union)
- **Secondary:** Accuracy, F1-score  
- **Validation:** Real-time during training

## 🎯 Expected Results

### Performance Targets
- **IoU > 0.85** on validation set (comparable to Prithvi fine-tuning)
- **Training Time:** ~24-48 hours on H100 GPU
- **Model Size:** ~90M-1B parameters (Clay v1 base)

### Fine-tuning Benefits
- **Faster Convergence:** Pre-trained weights accelerate training
- **Better Feature Representations:** Foundation model features for geospatial data
- **Improved Generalization:** Leverages Clay's pre-training on diverse Earth observation data

### Outputs
- **Fine-tuned Model:** `outputs/models/{run_id}/checkpoints/best_model.ckpt`
- **Training Logs:** `logs/{run_id}/training.log`
- **Config & Run Info:** `logs/{run_id}/config.json`, `run_info.json`
- **Validation Visualizations:** WandB dashboard every 10 epochs

## 🔧 Troubleshooting

### Common Issues
1. **CUDA OOM:** Reduce `batch_size` in config
2. **Data Loading:** Ensure data path is correct
3. **Dependencies:** Check TerraTorch installation

### Debug Mode
```bash
# Run with minimal data for debugging
./scripts/test_clay.sh
```

## � Comparison with Prithvi

This Clay fine-tuning experiment follows the same pattern as the Prithvi fine-tuning:

| Aspect | Clay | Prithvi |
|--------|------|---------|
| **Model Type** | timm_clay_v1_base | prithvi_eo_v1_100 |
| **Pre-training** | General geospatial data | Landsat/Sentinel data |
| **Architecture** | Vision Transformer | Vision Transformer |
| **Channel Init** | RGB → 9-channel mapping | 6-channel → 9-channel mapping |
| **Learning Rate** | 1e-5 | 2e-5 |
| **Batch Size** | 8 | 8 |
| **Warmup Epochs** | 20 | 20 |

## �📈 Next Steps

After fine-tuning completes:

1. **Evaluate Results:** Check WandB dashboard for metrics
2. **Compare with Prithvi:** Direct performance comparison
3. **Analyze Visualizations:** Compare prediction quality
4. **Fine-tune Hyperparameters:** Adjust if needed based on results

## 🔗 References

- **Clay Paper:** https://arxiv.org/abs/2404.15366
- **TerraTorch:** Foundation model framework
- **WandB:** Experiment tracking and monitoring
- **Pattern Based On:** Prithvi fine-tuning implementation
