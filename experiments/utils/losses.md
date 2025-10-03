# Multitask Loss Functions for Geospatial Modeling

This document details the loss functions used for training the prioritized multimodal multitask model. The selection of appropriate loss functions is critical for successfully training a model on multiple, potentially disparate tasks. We have two primary tasks:

1. **Water/Non-Water Segmentation**: A binary classification problem for each pixel.

2. **D8 Flow Direction Prediction**: A multiclass classification problem for each pixel.

To handle these two tasks simultaneously, we use specialized loss functions for each and a dynamic weighting mechanism to combine them effectively.

## 1. Task 1: Water Segmentation Loss (`WaterSegmentationLoss`)

This task involves identifying which pixels in an image correspond to water. This is a classic binary semantic segmentation problem.

### Motivation & Selection

A simple Binary Cross-Entropy (BCE) loss can work, but it treats every pixel equally, which can be problematic in geospatial data where water bodies might occupy a very small fraction of the image (class imbalance). To address this, we use a **combination of BCE and Dice Loss**.

* **`BCEWithLogitsLoss`**: This is the standard per-pixel loss for binary classification. It provides a strong, stable gradient for learning the fundamental features of water vs. non-water.

* **`DiceLoss`**: This loss measures the spatial overlap between the predicted water mask and the ground truth. It was popularized as a loss function for deep learning by Milletari et al. (2016) and is highly effective at handling class imbalance. It focuses on the quality of the foreground segmentation rather than getting all the background pixels correct. By maximizing the Dice Coefficient (which is what minimizing Dice Loss does), the model is incentivized to produce contiguous, well-shaped water body predictions.

The final loss is a weighted sum: `Loss_Water = BCE_Loss + dice_weight * Dice_Loss`. This combination ensures both pixel-level accuracy and high-quality spatial segmentation.

## 2. Task 2: D8 Flow Direction Loss (`D8FlowDirectionLoss`)

This task requires predicting one of eight discrete flow directions for each pixel, based on the D8 standard (1, 2, 4, 8, 16, 32, 64, 128). This is a multiclass semantic segmentation problem.

### Motivation & Selection

The standard and most effective loss function for multiclass classification is **Cross-Entropy Loss (`CrossEntropyLoss`)**.

* **How it works**: The model's decoder for this task outputs 8 channels of raw scores (logits) per pixel, representing the model's confidence for each of the 8 possible flow directions. The `CrossEntropyLoss` function compares these logits against the ground truth class.

* **Target Conversion**: A crucial implementation detail is that `CrossEntropyLoss` expects target labels to be class *indices* (i.e., 0, 1, 2, ..., 7). Our ground truth data uses D8 values (1, 2, 4, ...). The `D8FlowDirectionLoss` class handles this automatically by internally mapping the D8 values to the corresponding class indices before computing the loss.

## 3. Dynamic Loss Weighting (`DynamicLossWeighter`)

When combining losses from different tasks, a significant challenge arises: the losses may have vastly different magnitudes. If `Loss_D8` is naturally 100x larger than `Loss_Water`, it will dominate the training process, and the model will neglect the water segmentation task.

### Motivation & Selection

Instead of manually tuning fixed weights (e.g., `0.5 * L1 + 0.5 * L2`), which is brittle and non-adaptive, we use a dynamic, uncertainty-based approach proposed by Kendall et al. (2018).

* **The Core Idea**: Treat the weight for each task's loss as a learnable parameter that represents the model's confidence (or inverse uncertainty) for that task. The model learns to automatically balance the tasks by adjusting these weights.

* **How it works**: The `DynamicLossWeighter` module introduces a learnable `log_vars` parameter for each task. The total loss is formulated as:

  `L_total = exp(-s_1)*L_1 + exp(-s_2)*L_2 + s_1 + s_2`

  Where `L_1` and `L_2` are the task losses, and `s_1` and `s_2` are the learnable log-variance parameters. To minimize this total loss, the optimizer must not only decrease the task losses (`L_1`, `L_2`) but also adjust the `s_i` terms. If a task has high loss, the model can "down-weigh" it by increasing its uncertainty (`s_i`), preventing it from overwhelming the other task. This creates a self-balancing system that adapts throughout training.

This approach is more robust and principled than static weighting and often leads to better performance on all tasks.

## 4. Citations

1. **Kendall, A., Gal, Y., & Cipolla, R. (2018).** *Multi-Task Learning Using Uncertainty to Weigh Losses for Scene Geometry and Semantics.* In Proceedings of the IEEE Conference on Computer Vision and Pattern Recognition (CVPR) (pp. 7482-7491).

2. **Milletari, F., Navab, N., & Ahmadi, S. A. (2016).** *V-Net: Fully Convolutional Neural Networks for Volumetric Medical Image Segmentation.* In 2016 Fourth International Conference on 3D Vision (3DV) (pp. 565-571). IEEE.