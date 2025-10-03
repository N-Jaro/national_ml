import torch
import torch.nn as nn
import torch.nn.functional as F

# --- Loss Function for Task 1: Water Segmentation ---

# ---------- ClDice helpers (soft morphology) ----------

def _soft_erode(x):
    # MinPool via -MaxPool(-x)
    x1 = -F.max_pool2d(-x, kernel_size=3, stride=1, padding=1)
    x2 = -F.max_pool2d(-x, kernel_size=(1,3), stride=1, padding=(0,1))
    x3 = -F.max_pool2d(-x, kernel_size=(3,1), stride=1, padding=(1,0))
    return torch.min(torch.min(x1, x2), x3)

def _soft_dilate(x):
    return F.max_pool2d(x, kernel_size=3, stride=1, padding=1)

def _soft_open(x):
    return _soft_dilate(_soft_erode(x))

def _soft_skeletonize(x, iters=50):
    """
    Differentiable skeletonization (Shit et al., CVPR'21, ClDice).
    Works on probabilities in [0,1]. Returns a soft skeleton in [0,1].
    """
    # Ensure numeric stability in range
    x = torch.clamp(x, 0.0, 1.0)
    skel = torch.zeros_like(x)
    for _ in range(iters):
        opened = _soft_open(x)
        resid = torch.relu(x - opened)
        skel = torch.relu(skel + torch.relu(x - _soft_erode(opened)))
        x = opened
        # Early stop when almost nothing changes
        if torch.mean(resid) < 1e-4:
            break
    return torch.clamp(skel, 0.0, 1.0)

# ---------- ClDice Loss (Topology-preserving) ----------

class ClDiceLoss(nn.Module): 
    """ 
    ClDice loss for connectivity of thin structures.
    Ref: Shit et al., "ClDice - A Novel Topology-Preserving Loss Function for Tubular Structure Segmentation", CVPR 2021.
    Input: logits (B,1,H,W) or (B,H,W), targets (same shape), binary targets in {0,1}.
    """
    def __init__(self, skel_iters=50, eps=1e-6):
        super().__init__()
        self.skel_iters = skel_iters
        self.eps = eps

    def forward(self, logits, targets):
        if logits.dim() == 4 and logits.size(1) == 1:
            probs = torch.sigmoid(logits)
            gt = targets
        else:
            # assume shape (B,H,W)
            probs = torch.sigmoid(logits.unsqueeze(1))
            gt = targets.unsqueeze(1)

        gt = gt.float()
        probs = torch.clamp(probs, 0.0, 1.0)

        # Soft skeletons
        skel_pred = _soft_skeletonize(probs, iters=self.skel_iters)
        skel_gt = _soft_skeletonize(gt, iters=self.skel_iters)

        # Topology precision & sensitivity
        # T_prec = |S(P) ∩ G| / (|S(P)| + eps)
        t_prec_num = (skel_pred * gt).sum(dim=(1,2,3))
        t_prec_den = skel_pred.sum(dim=(1,2,3)) + self.eps
        t_prec = t_prec_num / t_prec_den

        # T_sens = |S(G) ∩ P| / (|S(G)| + eps)
        t_sens_num = (skel_gt * probs).sum(dim=(1,2,3))
        t_sens_den = skel_gt.sum(dim=(1,2,3)) + self.eps
        t_sens = t_sens_num / t_sens_den

        # ClDice = 1 - (2 * T_prec * T_sens) / (T_prec + T_sens + eps)
        cldice = 1.0 - (2.0 * t_prec * t_sens) / (t_prec + t_sens + self.eps)
        return cldice.mean()

# ---------- Optional: Skeleton Dice (centerline Dice) ----------

class SkeletonDiceLoss(nn.Module):
    """
    Dice computed on soft skeletons (centerlines). Can be used on its own or inside a hybrid loss.
    """
    def __init__(self, skel_iters=50, smooth=1e-6):
        super().__init__()
        self.skel_iters = skel_iters
        self.smooth = smooth

    def forward(self, logits, targets):
        if logits.dim() == 4 and logits.size(1) == 1:
            probs = torch.sigmoid(logits)
            gt = targets
        else:
            probs = torch.sigmoid(logits.unsqueeze(1))
            gt = targets.unsqueeze(1)

        probs = torch.clamp(probs, 0.0, 1.0).float()
        gt = gt.float()

        skel_pred = _soft_skeletonize(probs, iters=self.skel_iters)
        skel_gt = _soft_skeletonize(gt, iters=self.skel_iters)

        inter = (skel_pred * skel_gt).sum(dim=(1,2,3))
        denom = skel_pred.sum(dim=(1,2,3)) + skel_gt.sum(dim=(1,2,3)) + self.smooth
        dice = (2.0 * inter + self.smooth) / denom
        return 1.0 - dice.mean()


class AdaptiveLoss(nn.Module):
    def __init__(self, pooling_size=(8, 8)):
        """
        Initializes the AdaptiveLoss function.
        
        Args:
            pooling_size (tuple): The size of the spatial max pooling window.
                                  The paper suggests 8x8.
        """
        super(AdaptiveLoss, self).__init__()
        self.pooling_size = pooling_size

    def forward(self, input, target):
        """
        Computes the adaptive loss. 
        Refer: https://doi.org/10.1109/LGRS.2018.2811754 for details.

        Args:
            input (torch.Tensor): The predicted label probabilities from the model.
                                  Expected shape: (N, C, H, W) where C is the number of classes.
            target (torch.Tensor): The ground truth target labels.
                                   Expected shape: (N, H, W).
        """
        # Step 1: Compute pixel-wise cross-entropy loss
        # The cross_entropy function already computes the negative log likelihood loss.
        # We set reduction='none' to get a loss value for each pixel.
        pixelwise_loss = F.cross_entropy(input, target, reduction='none')
        
        # Add a channel dimension for pooling (from HxW to 1xHxW)
        pixelwise_loss = pixelwise_loss.unsqueeze(1)

        # Step 2: Apply spatial max pooling to the pixel-wise loss
        # The pooling operation will take the maximum loss value in each local window.
        # This is the core of the adaptive rerouting mechanism.
        max_pooled_loss = F.max_pool2d(pixelwise_loss, kernel_size=self.pooling_size)
        
        # Step 3: Compute the mean of the max-pooled loss values to get the final scalar loss.
        # The mean is taken over the entire batch and the downsampled loss map.
        final_loss = torch.mean(max_pooled_loss)
        
        return final_loss

class FocalLoss(nn.Module):
    """
    Focal Loss for binary segmentation.
    """
    def __init__(self, alpha=0.25, gamma=2.0):
        super(FocalLoss, self).__init__()
        self.alpha = alpha  # Weighting factor for the positive class
        self.gamma = gamma  # Focusing parameter

    def forward(self, inputs, targets):
        # We need to apply a sigmoid to get probabilities
        # since the input 'inputs' is logits
        BCE_loss = F.binary_cross_entropy_with_logits(inputs, targets, reduction='none')
        
        # Calculate the probabilities
        pt = torch.exp(-BCE_loss)
        
        # Calculate the focal loss
        F_loss = self.alpha * (1-pt)**self.gamma * BCE_loss
        
        return F_loss.mean()

class DiceLoss(nn.Module):
    """
    Computes the Dice Loss, a common metric for segmentation tasks.
    The Dice Loss is calculated as 1 - Dice Coefficient.
    """
    def __init__(self, smooth=1e-6):
        super(DiceLoss, self).__init__()
        self.smooth = smooth

    def forward(self, logits, targets):
        # Apply sigmoid to logits to get probabilities
        probs = torch.sigmoid(logits)
        
        # Flatten label and prediction tensors
        probs = probs.view(-1)
        targets = targets.view(-1)
        
        intersection = (probs * targets).sum()
        dice_coeff = (2. * intersection + self.smooth) / (probs.sum() + targets.sum() + self.smooth)
        
        return 1 - dice_coeff

class CombinedFocalDiceLoss(nn.Module):
    def __init__(self, focal_weight=1.0, dice_weight=0.5, focal_alpha=0.25, focal_gamma=2.0):
        super(CombinedFocalDiceLoss, self).__init__()
        self.focal_loss = FocalLoss(alpha=focal_alpha, gamma=focal_gamma)
        self.dice_loss = DiceLoss()
        self.focal_weight = focal_weight
        self.dice_weight = dice_weight
        
    def forward(self, logits, targets):
        focal_l = self.focal_loss(logits, targets)
        dice_l = self.dice_loss(logits, targets)
        
        return self.focal_weight * focal_l + self.dice_weight * dice_l



class WaterSegmentationLoss(nn.Module):
    """
    A combined loss for water segmentation that includes both
    Binary Cross-Entropy and Dice Loss.
    """
    def __init__(self, dice_weight=0.5):
        super(WaterSegmentationLoss, self).__init__()
        # We can weight the contribution of each loss
        self.bce_loss = nn.BCEWithLogitsLoss()
        self.dice_loss = DiceLoss()
        self.dice_weight = dice_weight

    def forward(self, logits, targets):
        bce = self.bce_loss(logits, targets)
        dice = self.dice_loss(logits, targets)
        
        # Combine the two losses
        combined_loss = bce + self.dice_weight * dice
        return combined_loss

# --- Loss Function for Task 2: D8 Flow Direction ---

class D8FlowDirectionLoss(nn.Module):
    """
    Computes the Cross-Entropy Loss for D8 flow direction prediction.
    It expects raw logits from the model and a target tensor with D8 values.
    """
    def __init__(self):
        super(D8FlowDirectionLoss, self).__init__()
        # Mapping from D8 values to class indices (0-7)
        self.d8_to_idx = {1: 0, 2: 1, 4: 2, 8: 3, 16: 4, 32: 5, 64: 6, 128: 7}
        self.idx_to_d8 = {v: k for k, v in self.d8_to_idx.items()}
        
        # Create a tensor for mapping, which can be moved to the correct device
        # This is more efficient than a dictionary lookup on the fly.
        # We create a mapping for values from 0 to 128.
        mapping = torch.zeros(129, dtype=torch.long)
        for d8_val, idx in self.d8_to_idx.items():
            mapping[d8_val] = idx
        self.register_buffer('mapping_tensor', mapping)

        # Standard Cross-Entropy Loss for multi-class classification
        # We can add class weights here if some directions are more/less common
        self.ce_loss = nn.CrossEntropyLoss()

    def convert_targets(self, targets):
        """Converts a tensor of D8 values (1, 2, 4...) to class indices (0, 1, 2...)."""
        # Ensure targets are integer type
        targets_int = targets.long()
        # Use the mapping tensor for efficient conversion
        return self.mapping_tensor[targets_int]

    def forward(self, logits, targets):
        # The model should output 8 channels for this task
        # logits shape: [B, 8, H, W]
        # targets shape: [B, 1, H, W] or [B, H, W]
        
        # Convert the D8 target values to class indices
        target_indices = self.convert_targets(targets.squeeze(1)) # Squeeze channel dim

        return self.ce_loss(logits, target_indices)


# --- Dynamic Loss Weighting ---

class DynamicLossWeighter(nn.Module):
    """
    Dynamically weighs multiple loss functions using uncertainty, as proposed in:
    "Multi-Task Learning Using Uncertainty to Weigh Losses for Scene Geometry and Semantics"
    (Kendall, Gal, & Cipolla, CVPR 2018)
    """
    def __init__(self, num_tasks=2):
        super(DynamicLossWeighter, self).__init__()
        # Initialize log-variances (uncertainties) for each task.
        # These are learnable parameters.
        initial_log_vars = torch.zeros(num_tasks)
        self.log_vars = nn.Parameter(initial_log_vars)

    def forward(self, loss1, loss2):
        """
        Calculates the combined, dynamically weighted loss.
        L_total = exp(-s_1)*L_1 + exp(-s_2)*L_2 + s_1 + s_2
        where s_i is the log-variance for task i.
        """
        # Precision terms (1 / (2*sigma^2))
        precision1 = torch.exp(-self.log_vars[0])
        precision2 = torch.exp(-self.log_vars[1])
        
        # Weighted losses
        term1 = precision1 * loss1
        term2 = precision2 * loss2
        
        # Regularization terms (log(sigma))
        reg1 = self.log_vars[0]
        reg2 = self.log_vars[1]
        
        # The factor of 0.5 is often used but can be omitted as it's a constant scaling
        total_loss = term1 + term2 + 0.5 * reg1 + 0.5 * reg2
        
        return total_loss


# if __name__ == '__main__':
#     # --- Example Usage ---
#     device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
#     batch_size = 4
#     h, w = 224, 224

#     # --- Task 1: Water Segmentation ---
#     print("--- Testing Water Segmentation Loss ---")
#     water_loss_fn = WaterSegmentationLoss().to(device)
#     # Model output for water task (1 channel)
#     water_logits = torch.randn(batch_size, 1, h, w, device=device)
#     # Ground truth water mask (binary)
#     water_targets = torch.randint(0, 2, (batch_size, 1, h, w), device=device).float()
    
#     loss_water = water_loss_fn(water_logits, water_targets)
#     print(f"Water Loss: {loss_water.item():.4f}")

#     # --- Task 2: D8 Flow Direction ---
#     print("\n--- Testing D8 Flow Direction Loss ---")
#     d8_loss_fn = D8FlowDirectionLoss().to(device)
#     # Model output for D8 task (8 channels)
#     d8_logits = torch.randn(batch_size, 8, h, w, device=device)
#     # Ground truth D8 values
#     d8_values = [1, 2, 4, 8, 16, 32, 64, 128]
#     d8_targets = torch.tensor([d8_values[i % 8] for i in range(batch_size * h * w)], device=device).view(batch_size, 1, h, w)
    
#     loss_d8 = d8_loss_fn(d8_logits, d8_targets)
#     print(f"D8 Loss: {loss_d8.item():.4f}")

#     # --- Dynamic Weighting ---
#     print("\n--- Testing Dynamic Loss Weighter ---")
#     dynamic_weighter = DynamicLossWeighter(num_tasks=2).to(device)
    
#     # The parameters of the weighter must be included in the optimizer
#     # optimizer = torch.optim.Adam(list(model.parameters()) + list(dynamic_weighter.parameters()), lr=1e-4)
    
#     total_loss = dynamic_weighter(loss_water, loss_d8)
#     print(f"Initial Log Variances: {dynamic_weighter.log_vars.data.cpu().numpy()}")
#     print(f"Total Combined Loss: {total_loss.item():.4f}")
    
#     # In a training loop, you would do:
#     # total_loss.backward()
#     # optimizer.step()
#     # The log_vars would be updated automatically.
