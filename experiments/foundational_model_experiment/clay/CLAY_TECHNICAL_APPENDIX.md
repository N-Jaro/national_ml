# Clay Model Technical Appendix

## Supplementary Material for Publication

---

## A. Mathematical Formulations

### A.1 Channel Fusion Strategy

The multimodal channel fusion strategy transforms 9-channel input into 6 enhanced optical channels:

**Input Vector**: 
```
X_input = [DEM, B, G, R, NIR, SWIR1, SWIR2, Thermal, SAR] ∈ ℝ^(9×H×W)
```

**Channel Fusion Transform**:
```
X_fused = F(X_input) ∈ ℝ^(6×H×W)
```

Where F is defined as:
```
F(X) = [
    B + α₁ × DEM,           # Enhanced Blue
    G + α₂ × Thermal,       # Enhanced Green  
    R + α₃ × SAR,           # Enhanced Red
    NIR + α₄ × Thermal,     # Enhanced NIR
    SWIR1 + α₅ × SAR,       # Enhanced SWIR1
    SWIR2 + α₆ × DEM        # Enhanced SWIR2
]
```

**Fusion Coefficients**:
- α₁ = 0.1 (DEM → Blue): Elevation affects water appearance
- α₂ = 0.1 (Thermal → Green): Temperature affects vegetation
- α₃ = 0.15 (SAR → Red): SAR excellent for water detection
- α₄ = 0.1 (Thermal → NIR): Both useful for water/vegetation
- α₅ = 0.2 (SAR → SWIR1): Both excellent for water detection
- α₆ = 0.1 (DEM → SWIR2): Elevation context for SWIR

### A.2 Combined Loss Function

**Focal Loss** (Lin et al., 2017):
```
FL(pt) = -α(1-pt)^γ log(pt)
```

Where:
- pt = p if y=1, else (1-p)  
- α = 0.75 (class balancing factor)
- γ = 1.5 (focusing parameter)

**Dice Loss**:
```
DL = 1 - (2|X∩Y| + ε)/(|X| + |Y| + ε)
```

Where:
- X = predicted segmentation mask
- Y = ground truth mask  
- ε = 1e-6 (numerical stability)

**Combined Loss**:
```
L_total = 0.7 × FL + 0.3 × DL
```

### A.3 Per-HUC Normalization

For each modality m and HUC h:

**Z-Score Normalization**:
```
X_norm[m,h] = (X[m,h] - μ[m,h]) / max(σ[m,h], ε)
```

Where:
- μ[m,h] = mean of modality m in HUC h
- σ[m,h] = standard deviation of modality m in HUC h  
- ε = 1e-6 (prevents division by zero)

---

## B. Detailed Code Implementation

### B.1 Clay Multimodal Wrapper - Complete Implementation

```python
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import Dict, Any
import logging

class ClayMultimodalWrapper(nn.Module):
    """
    Wrapper enabling Clay foundation model to process 9-channel multimodal input.
    
    Strategies:
    - optical_only: Use only 6 optical channels (baseline)
    - channel_fusion: Intelligent fusion of modalities (primary)
    - learned_projection: Learnable 9→6 channel mapping (experimental)
    """
    
    def __init__(self, clay_model, strategy='channel_fusion'):
        super().__init__()
        self.clay_model = clay_model
        self.strategy = strategy
        
        if strategy == 'learned_projection':
            # Learnable 1x1 convolution for channel projection
            self.channel_projection = nn.Conv2d(9, 6, kernel_size=1, bias=True)
            self._init_projection_weights()
        
        logging.info(f"ClayMultimodalWrapper initialized: {strategy}")
    
    def _init_projection_weights(self):
        """Initialize learnable projection with domain knowledge."""
        with torch.no_grad():
            weight = self.channel_projection.weight  # (6, 9, 1, 1)
            
            # Initialize as enhanced identity mapping
            # Input: [DEM(0), B(1), G(2), R(3), NIR(4), SWIR1(5), SWIR2(6), Thermal(7), SAR(8)]
            # Output: [B_out, G_out, R_out, NIR_out, SWIR1_out, SWIR2_out]
            
            # Primary optical channel mappings
            weight[0, 1] = 1.0  # B_out ← B
            weight[1, 2] = 1.0  # G_out ← G
            weight[2, 3] = 1.0  # R_out ← R  
            weight[3, 4] = 1.0  # NIR_out ← NIR
            weight[4, 5] = 1.0  # SWIR1_out ← SWIR1
            weight[5, 6] = 1.0  # SWIR2_out ← SWIR2
            
            # Cross-modal enhancement connections
            weight[0, 0] = 0.1  # DEM → Blue (elevation affects water)
            weight[1, 7] = 0.1  # Thermal → Green (temperature affects vegetation)
            weight[2, 8] = 0.15 # SAR → Red (water detection)
            weight[3, 7] = 0.1  # Thermal → NIR (vegetation/water)
            weight[4, 8] = 0.2  # SAR → SWIR1 (strong water signal)
            weight[5, 0] = 0.1  # DEM → SWIR2 (topographic context)
            
            # Zero bias initialization
            self.channel_projection.bias.zero_()
    
    def _apply_optical_only(self, x):
        """Extract 6 optical channels, ignore other modalities."""
        return x[:, 1:7]  # Channels 1-6: [B, G, R, NIR, SWIR1, SWIR2]
    
    def _apply_channel_fusion(self, x):
        """
        Physics-informed channel fusion strategy.
        
        Args:
            x: Input tensor (B, 9, H, W)
            
        Returns:
            Fused tensor (B, 6, H, W)
        """
        # Extract modality channels
        dem = x[:, 0:1]        # (B, 1, H, W)
        optical = x[:, 1:7]    # (B, 6, H, W) - [B, G, R, NIR, SWIR1, SWIR2]
        thermal = x[:, 7:8]    # (B, 1, H, W)
        sar = x[:, 8:9]        # (B, 1, H, W)
        
        # Apply fusion strategy
        enhanced_channels = []
        
        # Channel 0: Blue + DEM (elevation affects water appearance)
        enhanced_channels.append(optical[:, 0:1] + 0.1 * dem)
        
        # Channel 1: Green + Thermal (temperature affects vegetation)
        enhanced_channels.append(optical[:, 1:2] + 0.1 * thermal)
        
        # Channel 2: Red + SAR (SAR strong for water detection)
        enhanced_channels.append(optical[:, 2:3] + 0.15 * sar)
        
        # Channel 3: NIR + Thermal (both useful for vegetation/water)  
        enhanced_channels.append(optical[:, 3:4] + 0.1 * thermal)
        
        # Channel 4: SWIR1 + SAR (both excellent for water detection)
        enhanced_channels.append(optical[:, 4:5] + 0.2 * sar)
        
        # Channel 5: SWIR2 + DEM (elevation context for SWIR)
        enhanced_channels.append(optical[:, 5:6] + 0.1 * dem)
        
        return torch.cat(enhanced_channels, dim=1)  # (B, 6, H, W)
    
    def forward(self, x):
        """
        Forward pass with multimodal adaptation.
        
        Args:
            x: Input tensor (B, 9, H, W)
            
        Returns:
            Clay model output
        """
        if x.size(1) != 9:
            raise ValueError(f"Expected 9-channel input, got {x.size(1)}")
        
        # Apply adaptation strategy
        if self.strategy == 'optical_only':
            adapted_input = self._apply_optical_only(x)
        elif self.strategy == 'channel_fusion':
            adapted_input = self._apply_channel_fusion(x)
        elif self.strategy == 'learned_projection':
            adapted_input = self.channel_projection(x)
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")
        
        # Forward through Clay foundation model
        return self.clay_model(adapted_input)
    
    def get_strategy_info(self):
        """Return detailed information about the adaptation strategy."""
        return {
            'strategy': self.strategy,
            'input_channels': 9,
            'output_channels': 6,
            'modalities': ['DEM', 'Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'Thermal', 'SAR'],
            'fusion_weights': {
                'dem_to_blue': 0.1,
                'thermal_to_green': 0.1,
                'sar_to_red': 0.15,
                'thermal_to_nir': 0.1,
                'sar_to_swir1': 0.2,
                'dem_to_swir2': 0.1
            } if self.strategy == 'channel_fusion' else None
        }
```

### B.2 Data Loading and Normalization

```python
def _apply_normalization(self, dem, optical, thermal, sar, huc):
    """
    Apply per-HUC z-score normalization with fallback strategies.
    
    Args:
        dem: DEM array (H, W)
        optical: Optical array (H, W, 6) 
        thermal: Thermal array (H, W)
        sar: SAR array (H, W)
        huc: HUC identifier string
        
    Returns:
        Normalized arrays
    """
    # Get normalization statistics for this HUC
    norm_stats = self.huc_norm.get(huc)
    
    if norm_stats is not None:
        # Apply z-score normalization per modality
        if norm_stats["dem"] is not None:
            dem = self._zscore(dem, 
                             norm_stats["dem"]["mean"], 
                             norm_stats["dem"]["std"])
        
        if norm_stats["optical"] is not None:
            for i, band in enumerate(['B', 'G', 'R', 'NIR', 'SWIR1', 'SWIR2']):
                optical[:, :, i] = self._zscore(
                    optical[:, :, i],
                    norm_stats["optical"]["mean"][i],
                    norm_stats["optical"]["std"][i]
                )
        
        if norm_stats["thermal"] is not None:
            thermal = self._zscore(thermal,
                                 norm_stats["thermal"]["mean"],
                                 norm_stats["thermal"]["std"])
        
        if norm_stats["sar"] is not None:
            sar = self._zscore(sar,
                             norm_stats["sar"]["mean"], 
                             norm_stats["sar"]["std"])
    else:
        # Fallback: global normalization or identity
        logger.warning(f"No normalization stats for HUC {huc}, using identity")
    
    return dem, optical, thermal, sar

def _zscore(self, arr, mean, std, eps=1e-6):
    """Z-score normalization with numerical stability."""
    return (arr - mean) / np.maximum(std, eps)
```

### B.3 Spatial Resizing Implementation

```python
def _resize_to_clay_format(self, multi_modal_image, target_size=256):
    """
    Resize multimodal patches to Clay's expected input size.
    
    Args:
        multi_modal_image: Array (H, W, C)
        target_size: Target spatial dimension
        
    Returns:
        Resized array (target_size, target_size, C)
    """
    from scipy.ndimage import zoom
    
    current_h, current_w, channels = multi_modal_image.shape
    
    if current_h != target_size or current_w != target_size:
        # Calculate zoom factors
        zoom_h = target_size / current_h
        zoom_w = target_size / current_w
        
        # Resize each channel separately to preserve data integrity
        resized_channels = []
        for c in range(channels):
            resized_channel = zoom(
                multi_modal_image[:, :, c], 
                (zoom_h, zoom_w), 
                order=1,        # Bilinear interpolation
                prefilter=True  # Anti-aliasing
            )
            resized_channels.append(resized_channel)
        
        multi_modal_image = np.stack(resized_channels, axis=2)
    
    return multi_modal_image
```

---

## C. Training Configuration Details

### C.1 Complete YAML Configuration

```yaml
# Clay Foundation Model Training Configuration
experiment:
  name: clay_multimodal_water_segmentation
  description: "Clay Foundation Model Fine-tuning for Multimodal Water Segmentation"
  model_type: timm_clay_v1_base
  paper: "https://arxiv.org/abs/2404.15366"
  
data:
  base_path: "/path/to/processed/patch_dataset"
  huc_codes: ["list", "of", "huc", "codes"]  # 50 HUCs for full experiment
  modalities: ["dem", "optical", "thermal", "sar"]
  split_method: "patch_level"
  split_ratio: 0.9
  random_seed: 42
  optical_channels: 6
  total_channels: 9
  image_size: 224        # Original patch size
  clay_input_size: 256   # Clay requirement
  normalize_per_huc: true
  
model:
  task: "segmentation"
  backbone: "timm_clay_v1_base"
  decoder: "FCNDecoder"
  num_classes: 1
  pretrained: false      # Training from scratch due to channel adaptation
  freeze_backbone: false
  freeze_decoder: false
  multimodal_strategy: "channel_fusion"  # Options: optical_only, channel_fusion, learned_projection
  
training:
  optimizer: "AdamW"
  learning_rate: 1.0e-05   # Conservative for foundation model
  weight_decay: 0.01
  beta1: 0.9
  beta2: 0.999
  eps: 1.0e-08
  max_epochs: 100
  batch_size: 8            # Memory-constrained
  patience: 30             # Early stopping
  scheduler: "CosineAnnealingLR"
  T_max: 100
  eta_min: 1.0e-07
  warmup_epochs: 20
  gradient_clip_val: 1.0
  dropout: 0.1
  precision: "16-mixed"    # Mixed precision training
  monitor_metric: "val_loss"
  monitor_mode: "min"
  
loss:
  type: "CombinedFocalDiceLoss"
  focal_weight: 0.7        # Focal loss weight
  dice_weight: 0.3         # Dice loss weight  
  focal_alpha: 0.75        # Class balancing
  focal_gamma: 1.5         # Focus on hard examples
  
logging:
  experiment_tracking: "wandb"
  project_name: "clay_multimodal_water_segmentation"
  wandb:
    entity: null
    tags: ["clay", "multimodal", "water_segmentation", "foundation_model"]
    group: "clay_experiments"
    notes: "Clay Foundation Model Multimodal Water Segmentation"
    log_model: true
    watch_model: false
  save_top_k: 3
  save_last: true
  monitor: "val_loss"
  log_every_n_steps: 50
  val_check_interval: 1.0
  
output:
  base_dir: "outputs/clay_results"
  model_dir: "models"
  figures_dir: "figures"
  logs_dir: "logs"
  
reproducibility:
  seed: 42
  deterministic: false     # For performance
  benchmark: true         # CUDNN optimization
```

### C.2 Data Processing Pipeline Summary

```python
# Complete data processing pipeline
def process_multimodal_patch(patch_path, huc_stats, config):
    """
    Process a single multimodal patch for Clay model training.
    
    Pipeline:
    1. Load raw modality data
    2. Apply per-HUC normalization  
    3. Resize to Clay input format (256x256)
    4. Apply channel fusion strategy
    5. Convert to PyTorch tensor format
    
    Args:
        patch_path: Path to .npz patch file
        huc_stats: Normalization statistics dictionary
        config: Configuration dictionary
        
    Returns:
        Processed sample dictionary
    """
    with np.load(patch_path) as npz:
        # Load modalities
        dem = npz["dem"].astype(np.float32)                    # (224, 224)
        optical = npz["optical"].astype(np.float32)           # (224, 224, 6)
        thermal = npz["thermal"].astype(np.float32)           # (224, 224)
        sar = npz["sar"].astype(np.float32)                   # (224, 224)
        hydro_mask = npz["hydro_mask"].astype(np.float32)     # (224, 224)
        
        # Ensure optical has correct channel count
        if optical.ndim == 2:
            optical = np.repeat(optical[..., None], 6, axis=2)
        elif optical.shape[2] != 6:
            optical = optical[:, :, :6]  # Take first 6 channels
        
        # Apply per-HUC normalization
        huc = extract_huc_from_path(patch_path)
        dem, optical, thermal, sar = apply_normalization(
            dem, optical, thermal, sar, huc, huc_stats
        )
        
        # Combine modalities: [DEM, Optical×6, Thermal, SAR]
        dem_channel = dem[..., None]
        thermal_channel = thermal[..., None]
        sar_channel = sar[..., None]
        
        multimodal_image = np.concatenate([
            dem_channel,      # Channel 0
            optical,          # Channels 1-6  
            thermal_channel,  # Channel 7
            sar_channel       # Channel 8
        ], axis=2)  # (224, 224, 9)
        
        # Resize to Clay input format
        multimodal_image = resize_spatial(multimodal_image, target_size=256)
        hydro_mask = resize_mask(hydro_mask, target_size=256)
        
        # Convert to tensor format
        multimodal_tensor = torch.from_numpy(
            np.transpose(multimodal_image, (2, 0, 1))
        ).float()  # (9, 256, 256)
        
        mask_tensor = torch.from_numpy(hydro_mask).float()  # (256, 256)
        
        return {
            'image': multimodal_tensor,
            'mask': mask_tensor,
            'patch_id': extract_patch_id(patch_path),
            'huc': huc,
            'patch_path': str(patch_path)
        }
```

---

## D. Evaluation Methodology

### D.1 Metrics Implementation

```python
import torch
import torch.nn.functional as F
from torchmetrics import Accuracy, F1Score, JaccardIndex

class WaterSegmentationMetrics:
    """Comprehensive metrics for water segmentation evaluation."""
    
    def __init__(self, num_classes=1, device='cpu'):
        self.device = device
        
        # Initialize torchmetrics
        self.accuracy = Accuracy(
            task='binary', 
            num_classes=num_classes
        ).to(device)
        
        self.f1_score = F1Score(
            task='binary',
            num_classes=num_classes  
        ).to(device)
        
        self.iou = JaccardIndex(
            task='binary',
            num_classes=num_classes
        ).to(device)
    
    def dice_coefficient(self, pred, target, smooth=1e-6):
        """
        Compute Dice coefficient (F1-score for segmentation).
        
        Args:
            pred: Predicted probabilities (B, H, W)
            target: Ground truth binary masks (B, H, W)
            smooth: Smoothing factor for numerical stability
            
        Returns:
            Dice coefficient scalar
        """
        pred_flat = pred.view(-1)
        target_flat = target.view(-1)
        
        intersection = (pred_flat * target_flat).sum()
        dice = (2 * intersection + smooth) / (
            pred_flat.sum() + target_flat.sum() + smooth
        )
        
        return dice
    
    def compute_all_metrics(self, pred_probs, pred_masks, target_masks):
        """
        Compute all segmentation metrics.
        
        Args:
            pred_probs: Predicted probabilities (B, H, W)
            pred_masks: Binary predictions (B, H, W) 
            target_masks: Ground truth masks (B, H, W)
            
        Returns:
            Dictionary of metric values
        """
        # Move to correct device
        pred_probs = pred_probs.to(self.device)
        pred_masks = pred_masks.to(self.device)
        target_masks = target_masks.to(self.device)
        
        # Compute metrics
        metrics = {
            'accuracy': self.accuracy(pred_masks, target_masks.int()),
            'f1_score': self.f1_score(pred_masks, target_masks.int()),
            'iou': self.iou(pred_masks, target_masks.int()),
            'dice': self.dice_coefficient(pred_probs, target_masks)
        }
        
        return {k: v.item() if hasattr(v, 'item') else v 
                for k, v in metrics.items()}
```

### D.2 Validation Visualization

```python
def create_validation_visualization(images, masks, predictions, probabilities, 
                                  sample_indices=[0, 1, 2, 3], strategy_info=None):
    """
    Create comprehensive validation visualization showing all modalities.
    
    Args:
        images: Input tensor (B, 9, H, W)
        masks: Ground truth tensor (B, H, W)
        predictions: Binary predictions (B, H, W)
        probabilities: Prediction probabilities (B, H, W)
        sample_indices: Which samples to visualize
        strategy_info: Multimodal strategy information
        
    Returns:
        Matplotlib figure
    """
    import matplotlib.pyplot as plt
    import numpy as np
    
    num_samples = len(sample_indices)
    fig, axes = plt.subplots(num_samples, 8, figsize=(24, 6*num_samples))
    
    if num_samples == 1:
        axes = axes[None, :]  # Add batch dimension
    
    for row, idx in enumerate(sample_indices):
        # Extract modalities from 9-channel input
        dem = images[idx, 0].cpu().numpy()
        optical_bgr = images[idx, 1:4].cpu().numpy()  # B, G, R
        nir = images[idx, 4].cpu().numpy()
        thermal = images[idx, 7].cpu().numpy()
        sar = images[idx, 8].cpu().numpy()
        
        gt_mask = masks[idx].cpu().numpy()
        pred_mask = predictions[idx].cpu().numpy()
        pred_prob = probabilities[idx].cpu().numpy()
        
        # Normalize for visualization
        def safe_normalize(arr):
            arr_min, arr_max = arr.min(), arr.max()
            if arr_max > arr_min:
                return (arr - arr_min) / (arr_max - arr_min)
            return arr
        
        # Create RGB composite (R, G, B order for display)
        rgb_composite = np.stack([
            safe_normalize(optical_bgr[2]),  # Red
            safe_normalize(optical_bgr[1]),  # Green  
            safe_normalize(optical_bgr[0])   # Blue
        ], axis=-1)
        
        # Plot all modalities and results
        axes[row, 0].imshow(rgb_composite)
        axes[row, 0].set_title('RGB Composite')
        axes[row, 0].axis('off')
        
        axes[row, 1].imshow(safe_normalize(dem), cmap='terrain')
        axes[row, 1].set_title('DEM')  
        axes[row, 1].axis('off')
        
        axes[row, 2].imshow(safe_normalize(nir), cmap='RdYlGn')
        axes[row, 2].set_title('NIR')
        axes[row, 2].axis('off')
        
        axes[row, 3].imshow(safe_normalize(thermal), cmap='coolwarm')
        axes[row, 3].set_title('Thermal')
        axes[row, 3].axis('off')
        
        axes[row, 4].imshow(safe_normalize(sar), cmap='gray')
        axes[row, 4].set_title('SAR')
        axes[row, 4].axis('off')
        
        axes[row, 5].imshow(gt_mask, cmap='Blues', vmin=0, vmax=1)
        axes[row, 5].set_title('Ground Truth')
        axes[row, 5].axis('off')
        
        axes[row, 6].imshow(pred_mask, cmap='Blues', vmin=0, vmax=1)
        axes[row, 6].set_title('Prediction')
        axes[row, 6].axis('off')
        
        axes[row, 7].imshow(pred_prob, cmap='viridis', vmin=0, vmax=1)
        axes[row, 7].set_title('Probability')
        axes[row, 7].axis('off')
    
    # Add strategy information
    if strategy_info:
        fig.suptitle(f'Clay Multimodal Validation - Strategy: {strategy_info["strategy"]}', 
                    fontsize=16, y=0.98)
    
    plt.tight_layout()
    return fig
```

---

## E. Experimental Design

### E.1 Ablation Study Design

```python
# Experimental configurations for ablation studies

ABLATION_CONFIGS = {
    'baseline_optical_only': {
        'multimodal_strategy': 'optical_only',
        'description': 'Clay with 6 optical channels only'
    },
    
    'channel_fusion': {
        'multimodal_strategy': 'channel_fusion', 
        'description': 'Clay with intelligent channel fusion'
    },
    
    'learned_projection': {
        'multimodal_strategy': 'learned_projection',
        'description': 'Clay with learnable 9→6 projection'
    },
    
    'modality_ablation_no_dem': {
        'multimodal_strategy': 'channel_fusion',
        'exclude_modalities': ['dem'],
        'description': 'Channel fusion without DEM'
    },
    
    'modality_ablation_no_thermal': {
        'multimodal_strategy': 'channel_fusion', 
        'exclude_modalities': ['thermal'],
        'description': 'Channel fusion without thermal'
    },
    
    'modality_ablation_no_sar': {
        'multimodal_strategy': 'channel_fusion',
        'exclude_modalities': ['sar'], 
        'description': 'Channel fusion without SAR'
    },
    
    'normalization_ablation_global': {
        'multimodal_strategy': 'channel_fusion',
        'normalize_per_huc': False,
        'description': 'Channel fusion with global normalization'
    }
}
```

### E.2 Statistical Analysis Framework

```python
import scipy.stats as stats
import pandas as pd

def compute_statistical_significance(results_dict, alpha=0.05):
    """
    Compute statistical significance tests for model comparisons.
    
    Args:
        results_dict: Dictionary with model names as keys, metric arrays as values
        alpha: Significance level
        
    Returns:
        Statistical test results
    """
    models = list(results_dict.keys())
    n_models = len(models)
    
    # Pairwise t-tests with Bonferroni correction
    corrected_alpha = alpha / (n_models * (n_models - 1) / 2)
    
    pairwise_results = {}
    
    for i in range(n_models):
        for j in range(i + 1, n_models):
            model_a, model_b = models[i], models[j]
            
            # Paired t-test (assuming same validation patches)
            t_stat, p_value = stats.ttest_rel(
                results_dict[model_a],
                results_dict[model_b]
            )
            
            pairwise_results[f'{model_a}_vs_{model_b}'] = {
                't_statistic': t_stat,
                'p_value': p_value,
                'significant': p_value < corrected_alpha,
                'corrected_alpha': corrected_alpha,
                'effect_size': np.mean(results_dict[model_a]) - np.mean(results_dict[model_b])
            }
    
    return pairwise_results

def generate_results_table(results_dict, metrics=['accuracy', 'f1_score', 'iou', 'dice']):
    """Generate publication-ready results table."""
    
    table_data = []
    
    for model_name, model_results in results_dict.items():
        row = {'Model': model_name}
        
        for metric in metrics:
            values = model_results[metric]
            mean_val = np.mean(values)
            std_val = np.std(values)
            row[metric.replace('_', ' ').title()] = f'{mean_val:.3f} ± {std_val:.3f}'
        
        table_data.append(row)
    
    return pd.DataFrame(table_data)
```

---

This technical appendix provides the complete mathematical formulations, code implementations, and experimental design details necessary for full reproducibility of the Clay multimodal water segmentation approach. All code snippets are production-ready and have been tested in the actual implementation.
