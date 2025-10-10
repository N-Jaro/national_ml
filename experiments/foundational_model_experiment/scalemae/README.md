# ScaleMAE Foundation Model Experiment

## 📋 Overview

This experiment trains the **Scalemae foundation model** for water segmentation using 4-modal satellite data (DEM + optical + thermal + SAR).

**Paper:** https://arxiv.org/abs/2212.14532  
**Model Type:** `scalemae_base`

## 🚀 Quick Start

### Run Full Training
```bash
./scripts/run_scalemae.sh
```

### Run Quick Test (2 epochs, 2 HUCs)
```bash
./scripts/test_scalemae.sh
```

### Custom Configuration
```bash
./scripts/run_scalemae.sh --config configs/custom_config.yaml --gpus 2
```

## 📁 Directory Structure

```
scalemae_experiment/
├── configs/
│   └── scalemae_config.yaml     # Model configuration
├── training/
│   └── train_scalemae.py        # Training script
├── data/
│   └── (data adapters if needed)
├── outputs/
│   ├── models/                      # Saved models
│   ├── figures/                     # Plots and visualizations  
│   └── logs/                        # Training logs
├── scripts/
│   ├── run_scalemae.sh          # Main training script
│   └── test_scalemae.sh         # Quick test script
└── README.md                        # This file
```

## ⚙️ Configuration

Key configuration sections in `configs/scalemae_config.yaml`:

### Data
- **50 HUCs** for comprehensive evaluation
- **4 modalities:** DEM, optical (6-band), thermal, SAR
- **90/10 train/val split** with patch-level splitting

### Model  
- **Foundation Model:** scalemae_base
- **Decoder:** FCN decoder for segmentation
- **Input Channels:** 9 (1 DEM + 6 optical + 1 thermal + 1 SAR)
- **Output:** Binary water/non-water segmentation

### Training
- **Optimizer:** AdamW with cosine annealing
- **Learning Rate:** 1e-5 (conservative for foundation models)
- **Epochs:** 500 with early stopping (patience=20)
- **Batch Size:** 16 (memory optimized)
- **Loss:** CombinedFocalDiceLoss (matching MDMT baseline)

## 📊 Monitoring

### WandB Integration
- **Project:** `scalemae_water_segmentation`
- **Metrics:** IoU, accuracy, F1-score, loss
- **Tracking:** Gradients, model artifacts, hyperparameters

### Metrics
- **Primary:** IoU (Intersection over Union)
- **Secondary:** Accuracy, F1-score  
- **Validation:** Real-time during training

## 🎯 Expected Results

### Performance Targets
- **IoU > 0.85** on validation set
- **Training Time:** ~24-48 hours on H100 GPU
- **Model Size:** ~90M-1B parameters (varies by model)

### Outputs
- **Trained Model:** `outputs/models/best_model.ckpt`
- **Metrics:** `outputs/logs/metrics.csv`
- **Visualizations:** `outputs/figures/`

## 🔧 Troubleshooting

### Common Issues
1. **CUDA OOM:** Reduce `batch_size` in config
2. **Data Loading:** Ensure data path is correct
3. **Dependencies:** Check TerraTorch installation

### Debug Mode
```bash
# Run with minimal data for debugging
./scripts/test_scalemae.sh
```

## 📈 Next Steps

After training completes:

1. **Evaluate Results:** Check WandB dashboard for metrics
2. **Compare Models:** Use multi-model comparison scripts  
3. **Fine-tune:** Adjust hyperparameters if needed
4. **Deploy:** Export best model for inference

## 🔗 References

- **Paper:** https://arxiv.org/abs/2212.14532
- **TerraTorch:** Foundation model framework
- **WandB:** Experiment tracking and monitoring
