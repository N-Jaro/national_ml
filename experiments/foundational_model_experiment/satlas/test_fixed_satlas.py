#!/usr/bin/env python3
"""
Test fixed SATLAS model with a few training steps to verify it works.
"""

import torch
import numpy as np
import sys
from pathlib import Path
import yaml

# Add project paths
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "experiments"))

# Import SATLAS model and data
from training.train_satlas_satlaspretrain import SatlasPretrainedModel
from data.four_modal_dataset_adapter import FourModalDataModule

def test_fixed_satlas():
    """Test the fixed SATLAS model to ensure it can learn."""
    
    print("🔧 Testing Fixed SATLAS Model")
    print("=" * 40)
    
    # Load config
    config_path = Path(__file__).parent / "configs" / "satlas_pretrained_config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Modify config for quick testing
    config["data"]["huc_codes"] = ["03030005"]  # Single HUC
    config["training"]["batch_size"] = 4
    config["training"]["learning_rate"] = 1e-4  # Higher learning rate for quick test
    
    try:
        # Set up data
        print("📊 Setting up data...")
        data_module = FourModalDataModule(config)
        data_module.setup("fit")
        
        train_loader = data_module.train_dataloader()
        val_loader = data_module.val_dataloader()
        
        print(f"✅ Data ready: {len(data_module.train_dataset)} train, {len(data_module.val_dataset)} val")
        
        # Create model
        print("🧠 Creating fixed model...")
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = SatlasPretrainedModel(config)
        model = model.to(device)
        
        print(f"✅ Model created with {model.count_parameters():,} parameters")
        
        # Set up optimizer
        optimizer = torch.optim.AdamW(
            model.parameters(), 
            lr=config["training"]["learning_rate"],
            weight_decay=config["training"]["weight_decay"]
        )
        
        # Get sample batches
        train_batch = next(iter(train_loader))
        val_batch = next(iter(val_loader))
        
        # Move to device
        train_image = train_batch['image'].to(device)
        train_mask = train_batch['mask'].to(device)
        val_image = val_batch['image'].to(device)
        val_mask = val_batch['mask'].to(device)
        
        print("\n🔄 Testing forward pass...")
        
        # Initial evaluation
        model.eval()
        with torch.no_grad():
            val_output_initial = model(val_image)
            val_loss_initial = model.criterion(val_output_initial, val_mask)
            
            # Compute initial metrics
            val_pred_initial = torch.sigmoid(val_output_initial) > 0.5
            val_pred_initial = val_pred_initial.squeeze(1) if val_pred_initial.dim() == 4 else val_pred_initial
            
            val_acc_initial = (val_pred_initial == val_mask).float().mean()
            val_water_pred_initial = val_pred_initial.sum().item()
            val_water_actual = val_mask.sum().item()
            
            print(f"Initial validation - Loss: {val_loss_initial.item():.4f}, Acc: {val_acc_initial.item():.4f}")
            print(f"Water pixels - Predicted: {val_water_pred_initial}, Actual: {val_water_actual}")
        
        print("\n🏋️ Training for a few steps...")
        
        model.train()
        for step in range(10):  # Train for 10 steps
            optimizer.zero_grad()
            
            # Forward pass
            train_output = model(train_image)
            train_loss = model.criterion(train_output, train_mask)
            
            # Backward pass
            train_loss.backward()
            optimizer.step()
            
            if step % 3 == 0:  # Log every 3 steps
                with torch.no_grad():
                    train_pred = torch.sigmoid(train_output) > 0.5
                    train_pred = train_pred.squeeze(1) if train_pred.dim() == 4 else train_pred
                    train_water_pred = train_pred.sum().item()
                    train_water_actual = train_mask.sum().item()
                    
                    print(f"Step {step:2d} - Loss: {train_loss.item():.4f}, Water pred: {train_water_pred:5.0f}, actual: {train_water_actual:5.0f}")
        
        print("\n🔍 Final evaluation...")
        
        # Final evaluation
        model.eval()
        with torch.no_grad():
            val_output_final = model(val_image)
            val_loss_final = model.criterion(val_output_final, val_mask)
            
            # Compute final metrics
            val_pred_final = torch.sigmoid(val_output_final) > 0.5
            val_pred_final = val_pred_final.squeeze(1) if val_pred_final.dim() == 4 else val_pred_final
            
            val_acc_final = (val_pred_final == val_mask).float().mean()
            val_water_pred_final = val_pred_final.sum().item()
            
            print(f"Final validation - Loss: {val_loss_final.item():.4f}, Acc: {val_acc_final.item():.4f}")
            print(f"Water pixels - Predicted: {val_water_pred_final}, Actual: {val_water_actual}")
        
        # Check for improvement
        loss_improved = val_loss_final.item() < val_loss_initial.item()
        predicting_water = val_water_pred_final > 0
        
        print(f"\n📈 Results:")
        print(f"Loss improved: {'✅ Yes' if loss_improved else '❌ No'} ({val_loss_initial.item():.4f} -> {val_loss_final.item():.4f})")
        print(f"Predicting water: {'✅ Yes' if predicting_water else '❌ No'} ({val_water_pred_final} pixels)")
        
        if loss_improved and predicting_water:
            print("🎉 SUCCESS: Model is learning and predicting water pixels!")
            return True
        else:
            print("⚠️  Model may still have issues...")
            return False
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_fixed_satlas()