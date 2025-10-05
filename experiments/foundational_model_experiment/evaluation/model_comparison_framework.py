"""
Model Comparison Framework for Prithvi Foundation Model vs MDMT Variants
=======================================================================

This module provides standardized evaluation protocols to ensure fair comparison
between the Prithvi foundation model and MDMT variants on water segmentation.

Key Features:
- Consistent data splits and preprocessing
- Standardized metrics computation
- Statistical significance testing
- Performance profiling (memory, inference time)
- Visualization and reporting tools

Author: Foundation Model Experiment Team
"""

import os
import json
import time
import numpy as np
import torch
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional, Any
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import (
    accuracy_score, precision_recall_fscore_support, 
    jaccard_score, roc_auc_score, confusion_matrix
)
from scipy import stats
import pandas as pd
from pathlib import Path
import logging


class ModelComparisionFramework:
    """
    Comprehensive framework for comparing Prithvi foundation model with MDMT variants.
    """
    
    def __init__(self, 
                 output_dir: str = "/u/nathanj/national_ml/experiments/foundational_model_experiment/results",
                 seed: int = 42):
        """
        Initialize comparison framework.
        
        Args:
            output_dir: Directory to save comparison results
            seed: Random seed for reproducibility
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seed = seed
        
        # Set up logging
        logging.basicConfig(
            filename=self.output_dir / "comparison.log",
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize results storage
        self.results = {}
        self.metrics_config = self._setup_metrics()
        
        # Ensure reproducibility
        torch.manual_seed(seed)
        np.random.seed(seed)
        
    def _setup_metrics(self) -> Dict[str, Dict]:
        """Define standardized metrics for evaluation."""
        return {
            'segmentation_metrics': {
                'dice_coefficient': self._dice_coefficient,
                'iou_score': self._iou_score,
                'pixel_accuracy': self._pixel_accuracy,
                'precision': self._precision,
                'recall': self._recall,
                'f1_score': self._f1_score,
                'specificity': self._specificity,
                'auc_roc': self._auc_roc
            },
            'performance_metrics': {
                'inference_time': self._measure_inference_time,
                'memory_usage': self._measure_memory_usage,
                'model_size': self._get_model_size,
                'flops': self._estimate_flops
            },
            'robustness_metrics': {
                'noise_robustness': self._test_noise_robustness,
                'scale_invariance': self._test_scale_invariance,
                'brightness_robustness': self._test_brightness_robustness
            }
        }
    
    def register_model(self, 
                      model_name: str, 
                      model: torch.nn.Module, 
                      model_type: str = "unknown",
                      config: Optional[Dict] = None) -> None:
        """
        Register a model for comparison.
        
        Args:
            model_name: Unique identifier for the model
            model: PyTorch model instance
            model_type: Type of model (e.g., 'prithvi', 'mdmt_dem_optical')
            config: Model configuration dictionary
        """
        self.results[model_name] = {
            'model': model,
            'model_type': model_type,
            'config': config or {},
            'metrics': {},
            'predictions': {},
            'performance': {}
        }
        
        self.logger.info(f"Registered model: {model_name} (type: {model_type})")
    
    def evaluate_models(self, 
                       test_loader: torch.utils.data.DataLoader,
                       device: str = 'cuda',
                       save_predictions: bool = True) -> Dict[str, Dict]:
        """
        Evaluate all registered models on the test dataset.
        
        Args:
            test_loader: DataLoader for test data
            device: Device to run evaluation on
            save_predictions: Whether to save model predictions
            
        Returns:
            Dictionary containing evaluation results for all models
        """
        self.logger.info("Starting comprehensive model evaluation...")
        
        evaluation_results = {}
        
        for model_name, model_info in self.results.items():
            self.logger.info(f"Evaluating model: {model_name}")
            
            model = model_info['model'].to(device)
            model.eval()
            
            # Initialize metric accumulators
            all_predictions = []
            all_targets = []
            inference_times = []
            memory_usages = []
            
            with torch.no_grad():
                for batch_idx, batch in enumerate(test_loader):
                    # Handle different data formats (MDMT vs Prithvi)
                    if isinstance(batch, dict):
                        # Prithvi format
                        images = batch['image'].to(device)
                        targets = batch['mask'].to(device)
                    else:
                        # MDMT format (assuming tuple/list)
                        images, targets = batch[0].to(device), batch[1].to(device)
                    
                    # Measure inference time
                    start_time = time.time()
                    
                    # Forward pass
                    with torch.cuda.amp.autocast(enabled=True):
                        outputs = model(images)
                        
                        # Handle different output formats
                        if hasattr(outputs, 'prediction'):
                            predictions = outputs.prediction
                        elif hasattr(outputs, 'output'):
                            predictions = outputs.output
                        elif isinstance(outputs, dict):
                            predictions = outputs.get('water', outputs.get('prediction', outputs))
                        else:
                            predictions = outputs
                    
                    inference_time = time.time() - start_time
                    inference_times.append(inference_time)
                    
                    # Memory usage
                    if torch.cuda.is_available():
                        memory_usage = torch.cuda.max_memory_allocated() / 1024**3  # GB
                        memory_usages.append(memory_usage)
                    
                    # Convert to probabilities and binary predictions
                    if predictions.dim() > 2:
                        predictions = predictions.squeeze()
                    
                    probs = torch.sigmoid(predictions)
                    binary_preds = (probs > 0.5).float()
                    
                    # Ensure targets are binary
                    if targets.dim() > 2:
                        targets = targets.squeeze()
                    binary_targets = (targets > 0.5).float()
                    
                    # Store predictions and targets
                    all_predictions.append({
                        'probabilities': probs.cpu().numpy(),
                        'binary': binary_preds.cpu().numpy()
                    })
                    all_targets.append(binary_targets.cpu().numpy())
            
            # Compute comprehensive metrics
            model_results = self._compute_comprehensive_metrics(
                all_predictions, all_targets, inference_times, memory_usages
            )
            
            # Add model-specific information
            model_results['model_info'] = {
                'name': model_name,
                'type': model_info['model_type'],
                'config': model_info['config'],
                'total_parameters': sum(p.numel() for p in model.parameters()),
                'trainable_parameters': sum(p.numel() for p in model.parameters() if p.requires_grad)
            }
            
            evaluation_results[model_name] = model_results
            
            # Save predictions if requested
            if save_predictions:
                self._save_predictions(model_name, all_predictions, all_targets)
        
        # Generate comparison report
        self._generate_comparison_report(evaluation_results)
        
        # Statistical significance testing
        self._perform_statistical_tests(evaluation_results)
        
        return evaluation_results
    
    def _compute_comprehensive_metrics(self, 
                                     predictions: List[Dict], 
                                     targets: List[np.ndarray],
                                     inference_times: List[float],
                                     memory_usages: List[float]) -> Dict:
        """Compute all evaluation metrics."""
        
        # Flatten predictions and targets
        all_probs = np.concatenate([pred['probabilities'].flatten() for pred in predictions])
        all_binary_preds = np.concatenate([pred['binary'].flatten() for pred in predictions])
        all_targets_flat = np.concatenate([target.flatten() for target in targets])
        
        # Segmentation metrics
        segmentation_metrics = {}
        for metric_name, metric_func in self.metrics_config['segmentation_metrics'].items():
            try:
                if metric_name == 'auc_roc':
                    segmentation_metrics[metric_name] = metric_func(all_targets_flat, all_probs)
                else:
                    segmentation_metrics[metric_name] = metric_func(all_targets_flat, all_binary_preds)
            except Exception as e:
                self.logger.warning(f"Could not compute {metric_name}: {e}")
                segmentation_metrics[metric_name] = np.nan
        
        # Performance metrics
        performance_metrics = {
            'mean_inference_time': np.mean(inference_times),
            'std_inference_time': np.std(inference_times),
            'mean_memory_usage': np.mean(memory_usages) if memory_usages else 0.0,
            'max_memory_usage': np.max(memory_usages) if memory_usages else 0.0
        }
        
        # Confusion matrix
        cm = confusion_matrix(all_targets_flat, all_binary_preds)
        
        return {
            'segmentation_metrics': segmentation_metrics,
            'performance_metrics': performance_metrics,
            'confusion_matrix': cm.tolist(),
            'sample_statistics': {
                'total_pixels': len(all_targets_flat),
                'positive_ratio': np.mean(all_targets_flat),
                'predicted_positive_ratio': np.mean(all_binary_preds)
            }
        }
    
    # Metric computation methods
    def _dice_coefficient(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Compute Dice coefficient."""
        intersection = np.sum(y_true * y_pred)
        return (2.0 * intersection) / (np.sum(y_true) + np.sum(y_pred) + 1e-8)
    
    def _iou_score(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Compute Intersection over Union (Jaccard Index)."""
        return jaccard_score(y_true, y_pred, average='binary', zero_division=0)
    
    def _pixel_accuracy(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Compute pixel-wise accuracy."""
        return accuracy_score(y_true, y_pred)
    
    def _precision(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Compute precision."""
        precision, _, _, _ = precision_recall_fscore_support(
            y_true, y_pred, average='binary', zero_division=0
        )
        return precision
    
    def _recall(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Compute recall (sensitivity)."""
        _, recall, _, _ = precision_recall_fscore_support(
            y_true, y_pred, average='binary', zero_division=0
        )
        return recall
    
    def _f1_score(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Compute F1 score."""
        _, _, f1, _ = precision_recall_fscore_support(
            y_true, y_pred, average='binary', zero_division=0
        )
        return f1
    
    def _specificity(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Compute specificity (true negative rate)."""
        cm = confusion_matrix(y_true, y_pred)
        if cm.shape == (2, 2):
            tn, fp = cm[0, 0], cm[0, 1]
            return tn / (tn + fp + 1e-8)
        return 0.0
    
    def _auc_roc(self, y_true: np.ndarray, y_scores: np.ndarray) -> float:
        """Compute AUC-ROC score."""
        try:
            return roc_auc_score(y_true, y_scores)
        except:
            return 0.0
    
    def _measure_inference_time(self, model, sample_input):
        """Measure model inference time."""
        # Implementation for inference timing
        pass
    
    def _measure_memory_usage(self, model):
        """Measure model memory usage."""
        # Implementation for memory measurement
        pass
    
    def _get_model_size(self, model):
        """Get model size in parameters."""
        return sum(p.numel() for p in model.parameters())
    
    def _estimate_flops(self, model, input_shape):
        """Estimate FLOPs for model."""
        # Would need additional library like ptflops or fvcore
        return 0
    
    def _test_noise_robustness(self, model, clean_input, noise_levels):
        """Test model robustness to noise."""
        # Implementation for noise testing
        pass
    
    def _test_scale_invariance(self, model, input_data, scales):
        """Test model scale invariance."""
        # Implementation for scale testing
        pass
    
    def _test_brightness_robustness(self, model, input_data, brightness_levels):
        """Test model robustness to brightness changes."""
        # Implementation for brightness testing
        pass
    
    def _save_predictions(self, model_name: str, predictions: List[Dict], targets: List[np.ndarray]):
        """Save model predictions for further analysis."""
        output_path = self.output_dir / f"{model_name}_predictions.npz"
        
        # Flatten and save
        all_probs = np.concatenate([pred['probabilities'] for pred in predictions])
        all_binary = np.concatenate([pred['binary'] for pred in predictions])
        all_targets = np.concatenate(targets)
        
        np.savez_compressed(
            output_path,
            probabilities=all_probs,
            binary_predictions=all_binary,
            targets=all_targets
        )
        
        self.logger.info(f"Saved predictions for {model_name} to {output_path}")
    
    def _generate_comparison_report(self, results: Dict[str, Dict]):
        """Generate comprehensive comparison report."""
        
        # Create comparison table
        comparison_df = self._create_comparison_table(results)
        
        # Save results
        comparison_df.to_csv(self.output_dir / "model_comparison.csv", index=True)
        
        # Create visualizations
        self._create_comparison_visualizations(results, comparison_df)
        
        # Generate markdown report
        self._generate_markdown_report(results, comparison_df)
    
    def _create_comparison_table(self, results: Dict[str, Dict]) -> pd.DataFrame:
        """Create comprehensive comparison table."""
        
        data = []
        for model_name, result in results.items():
            row = {'Model': model_name}
            row.update(result['segmentation_metrics'])
            row.update(result['performance_metrics'])
            row.update({
                'Total_Parameters': result['model_info']['total_parameters'],
                'Model_Type': result['model_info']['type']
            })
            data.append(row)
        
        return pd.DataFrame(data).set_index('Model')
    
    def _create_comparison_visualizations(self, results: Dict, df: pd.DataFrame):
        """Create comparison visualizations."""
        
        # Metrics comparison radar chart
        self._create_radar_chart(df)
        
        # Performance metrics bar chart
        self._create_performance_chart(df)
        
        # Confusion matrices heatmap
        self._create_confusion_matrices(results)
    
    def _create_radar_chart(self, df: pd.DataFrame):
        """Create radar chart for metrics comparison."""
        # Implementation for radar chart
        plt.figure(figsize=(10, 8))
        # ... radar chart implementation
        plt.savefig(self.output_dir / "metrics_radar_chart.png", dpi=300, bbox_inches='tight')
        plt.close()
    
    def _create_performance_chart(self, df: pd.DataFrame):
        """Create performance comparison bar chart."""
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # Key metrics to plot
        metrics = ['dice_coefficient', 'iou_score', 'mean_inference_time', 'Total_Parameters']
        
        for i, metric in enumerate(metrics):
            ax = axes[i//2, i%2]
            if metric in df.columns:
                df[metric].plot(kind='bar', ax=ax)
                ax.set_title(f'{metric.replace("_", " ").title()}')
                ax.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / "performance_comparison.png", dpi=300, bbox_inches='tight')
        plt.close()
    
    def _create_confusion_matrices(self, results: Dict):
        """Create confusion matrices visualization."""
        n_models = len(results)
        fig, axes = plt.subplots(1, n_models, figsize=(5*n_models, 4))
        
        if n_models == 1:
            axes = [axes]
        
        for i, (model_name, result) in enumerate(results.items()):
            cm = np.array(result['confusion_matrix'])
            sns.heatmap(cm, annot=True, fmt='d', ax=axes[i], cmap='Blues')
            axes[i].set_title(f'{model_name} Confusion Matrix')
            axes[i].set_xlabel('Predicted')
            axes[i].set_ylabel('Actual')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / "confusion_matrices.png", dpi=300, bbox_inches='tight')
        plt.close()
    
    def _generate_markdown_report(self, results: Dict, df: pd.DataFrame):
        """Generate markdown comparison report."""
        
        report_path = self.output_dir / "comparison_report.md"
        
        with open(report_path, 'w') as f:
            f.write("# Model Comparison Report\n\n")
            f.write("## Overview\n\n")
            f.write("Comprehensive comparison between Prithvi foundation model and MDMT variants.\n\n")
            
            f.write("## Models Evaluated\n\n")
            for model_name, result in results.items():
                f.write(f"- **{model_name}** ({result['model_info']['type']}): ")
                f.write(f"{result['model_info']['total_parameters']:,} parameters\n")
            
            f.write("\n## Performance Metrics\n\n")
            f.write(df.round(4).to_markdown())
            f.write("\n\n")
            
            f.write("## Key Findings\n\n")
            self._write_key_findings(f, df)
            
            f.write("\n## Visualizations\n\n")
            f.write("- [Performance Comparison](performance_comparison.png)\n")
            f.write("- [Metrics Radar Chart](metrics_radar_chart.png)\n")
            f.write("- [Confusion Matrices](confusion_matrices.png)\n")
    
    def _write_key_findings(self, f, df: pd.DataFrame):
        """Write key findings to report."""
        
        # Best performing model for each metric
        key_metrics = ['dice_coefficient', 'iou_score', 'pixel_accuracy', 'f1_score']
        
        for metric in key_metrics:
            if metric in df.columns:
                best_model = df[metric].idxmax()
                best_value = df.loc[best_model, metric]
                f.write(f"- **Best {metric.replace('_', ' ').title()}**: {best_model} ({best_value:.4f})\n")
        
        # Fastest model
        if 'mean_inference_time' in df.columns:
            fastest_model = df['mean_inference_time'].idxmin()
            fastest_time = df.loc[fastest_model, 'mean_inference_time']
            f.write(f"- **Fastest Inference**: {fastest_model} ({fastest_time:.4f}s)\n")
        
        # Most efficient model (parameters vs performance)
        if 'dice_coefficient' in df.columns and 'Total_Parameters' in df.columns:
            efficiency = df['dice_coefficient'] / (df['Total_Parameters'] / 1e6)  # Dice per million parameters
            most_efficient = efficiency.idxmax()
            f.write(f"- **Most Efficient**: {most_efficient} (highest Dice/MParams ratio)\n")
    
    def _perform_statistical_tests(self, results: Dict[str, Dict]):
        """Perform statistical significance tests."""
        
        # Load predictions for statistical testing
        model_predictions = {}
        for model_name in results.keys():
            pred_path = self.output_dir / f"{model_name}_predictions.npz"
            if pred_path.exists():
                data = np.load(pred_path)
                model_predictions[model_name] = {
                    'probabilities': data['probabilities'],
                    'binary': data['binary_predictions'],
                    'targets': data['targets']
                }
        
        # Perform pairwise statistical tests
        significance_results = {}
        model_names = list(model_predictions.keys())
        
        for i in range(len(model_names)):
            for j in range(i+1, len(model_names)):
                model1, model2 = model_names[i], model_names[j]
                
                # McNemar's test for binary classification differences
                significance_results[f"{model1}_vs_{model2}"] = self._mcnemar_test(
                    model_predictions[model1], model_predictions[model2]
                )
        
        # Save statistical test results
        with open(self.output_dir / "statistical_tests.json", 'w') as f:
            json.dump(significance_results, f, indent=2)
    
    def _mcnemar_test(self, model1_preds: Dict, model2_preds: Dict) -> Dict:
        """Perform McNemar's test for comparing two models."""
        
        y_true = model1_preds['targets']
        pred1 = model1_preds['binary']
        pred2 = model2_preds['binary']
        
        # Create contingency table
        correct1 = (pred1 == y_true)
        correct2 = (pred2 == y_true)
        
        # McNemar's table
        b = np.sum(correct1 & ~correct2)  # Model 1 correct, Model 2 incorrect
        c = np.sum(~correct1 & correct2)  # Model 1 incorrect, Model 2 correct
        
        if b + c > 0:
            # McNemar's test statistic
            mcnemar_stat = (abs(b - c) - 1)**2 / (b + c)
            p_value = 1 - stats.chi2.cdf(mcnemar_stat, 1)
        else:
            mcnemar_stat = 0.0
            p_value = 1.0
        
        return {
            'mcnemar_statistic': float(mcnemar_stat),
            'p_value': float(p_value),
            'significant': p_value < 0.05,
            'model1_only_correct': int(b),
            'model2_only_correct': int(c)
        }


def create_standardized_config(model_type: str, base_config: Dict) -> Dict:
    """
    Create standardized configuration for fair comparison.
    
    Args:
        model_type: Type of model ('prithvi' or 'mdmt')
        base_config: Base configuration dictionary
        
    Returns:
        Standardized configuration ensuring fair comparison
    """
    
    # Standard settings for fair comparison
    standard_config = {
        'data': {
            'batch_size': 8,
            'num_workers': 4,
            'target_size': [224, 224],
            'normalize': True,
            'train_huc_codes': ["10020003", "10020004", "10020007"],
            'val_huc_codes': ["10020001", "10020002"],
            'test_huc_codes': ["10020005", "10020006"]  # Consistent test set
        },
        'training': {
            'max_epochs': 100,
            'patience': 15,
            'monitor': 'val_loss',
            'seed': 42,
            'precision': '16-mixed'
        },
        'optimization': {
            'optimizer': 'AdamW',
            'lr': 1e-4,  # Conservative LR for both model types
            'weight_decay': 0.01,
            'scheduler': 'CosineAnnealingLR'
        }
    }
    
    # Model-specific adjustments
    if model_type == 'prithvi':
        # Prithvi-specific settings
        standard_config.update({
            'model': {
                'backbone': 'prithvi_100',
                'pretrained': True,
                'freeze_backbone': False,
                'progressive_unfreezing': False  # Disable for fair comparison
            }
        })
    elif model_type.startswith('mdmt'):
        # MDMT-specific settings
        standard_config.update({
            'model': {
                'architecture': 'unet',
                'pretrained': False,
                'depth': 4
            }
        })
    
    # Merge with base config
    final_config = {**standard_config, **base_config}
    
    return final_config


if __name__ == "__main__":
    # Example usage
    comparison = ModelComparisionFramework()
    
    # This would be used in the actual comparison script
    print("Model Comparison Framework initialized successfully!")
    print(f"Results will be saved to: {comparison.output_dir}")