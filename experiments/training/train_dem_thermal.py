#!/usr/bin/env python3
"""
Training script for DEM + Thermal multimodal multitask model.
This script demonstrates how to train the simplified 2-modality model.
"""

import os
import sys
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torch.optim import Adam, AdamW
from torch.optim.lr_scheduler import StepLR
import argparse

# Add the experiments directory to path to import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from models.mdmt_dem_thermal import MultimodalMultitaskModel_DEM_Thermal
from data.patchDataLoader_dem_thermal import create_dataloader_dem_thermal

def train_epoch(model, dataloader, criterion, optimizer, device):
    """Train for one epoch."""
    model.train()
    total_loss = 0.0
    num_batches = 0
    
    for batch_idx, batch in enumerate(dataloader):
        # Move data to device
        dem = batch['dem'].to(device)
        thermal = batch['thermal'].to(device)
        hydro_mask = batch['hydro_mask'].to(device)
        flow_dir = batch['flow_dir'].to(device)
        
        # Forward pass
        optimizer.zero_grad()
        pred_hydro, pred_flow = model(dem, thermal)
        
        # Calculate losses (example - adjust based on your specific tasks)
        # Assuming task 1 is hydro mask prediction, task 2 is flow direction
        loss1 = criterion(pred_hydro.squeeze(1), hydro_mask)
        loss2 = criterion(pred_flow.squeeze(1), flow_dir.float())
        
        total_loss_batch = loss1 + loss2
        
        # Backward pass
        total_loss_batch.backward()
        optimizer.step()
        
        total_loss += total_loss_batch.item()
        num_batches += 1
        
        if batch_idx % 10 == 0:
            print(f'Batch {batch_idx}/{len(dataloader)}, Loss: {total_loss_batch.item():.4f}')
    
    return total_loss / num_batches

def validate_epoch(model, dataloader, criterion, device):
    """Validate for one epoch."""
    model.eval()
    total_loss = 0.0
    num_batches = 0
    
    with torch.no_grad():
        for batch in dataloader:
            # Move data to device
            dem = batch['dem'].to(device)
            thermal = batch['thermal'].to(device)
            hydro_mask = batch['hydro_mask'].to(device)
            flow_dir = batch['flow_dir'].to(device)
            
            # Forward pass
            pred_hydro, pred_flow = model(dem, thermal)
            
            # Calculate losses
            loss1 = criterion(pred_hydro.squeeze(1), hydro_mask)
            loss2 = criterion(pred_flow.squeeze(1), flow_dir.float())
            
            total_loss_batch = loss1 + loss2
            total_loss += total_loss_batch.item()
            num_batches += 1
    
    return total_loss / num_batches

def main():
    parser = argparse.ArgumentParser(description='Train DEM + Thermal Model')
    parser.add_argument('--data_path', type=str, required=True,
                        help='Path to patch data directory')
    parser.add_argument('--huc_codes', type=str, nargs='+', required=True,
                        help='List of HUC codes to use for training')
    parser.add_argument('--batch_size', type=int, default=8,
                        help='Batch size for training')
    parser.add_argument('--epochs', type=int, default=50,
                        help='Number of training epochs')
    parser.add_argument('--lr', type=float, default=1e-3,
                        help='Learning rate')
    parser.add_argument('--output_dir', type=str, default='./outputs',
                        help='Directory to save model checkpoints')
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Device setup
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    
    # Split HUC codes for train/validation (simple split for example)
    split_idx = int(0.8 * len(args.huc_codes))
    train_hucs = args.huc_codes[:split_idx]
    val_hucs = args.huc_codes[split_idx:]
    
    if len(val_hucs) == 0:
        val_hucs = train_hucs  # Use same data for validation if only one HUC
    
    print(f"Training HUCs: {train_hucs}")
    print(f"Validation HUCs: {val_hucs}")
    
    # Create data loaders
    train_loader = create_dataloader_dem_thermal(
        base_path=args.data_path,
        huc_codes=train_hucs,
        batch_size=args.batch_size,
        shuffle=True,
        num_workers=4
    )
    
    val_loader = create_dataloader_dem_thermal(
        base_path=args.data_path,
        huc_codes=val_hucs,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=4
    )
    
    print(f"Training samples: {len(train_loader.dataset)}")
    print(f"Validation samples: {len(val_loader.dataset)}")
    
    # Create model
    model = MultimodalMultitaskModel_DEM_Thermal(
        n_classes_task1=1,  # Hydro mask prediction
        n_classes_task2=8,  # Flow direction prediction
        base_channels=64
    ).to(device)
    
    # Print model info
    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"Total parameters: {total_params:,}")
    print(f"Trainable parameters: {trainable_params:,}")
    
    # Setup training
    criterion = nn.MSELoss()  # Adjust based on your tasks
    optimizer = Adam(model.parameters(), lr=args.lr)
    scheduler = StepLR(optimizer, step_size=20, gamma=0.5)
    
    # Training loop
    best_val_loss = float('inf')
    
    for epoch in range(args.epochs):
        print(f"\\nEpoch {epoch + 1}/{args.epochs}")
        print("-" * 50)
        
        # Train
        train_loss = train_epoch(model, train_loader, criterion, optimizer, device)
        
        # Validate
        val_loss = validate_epoch(model, val_loader, criterion, device)
        
        # Update learning rate
        scheduler.step()
        
        print(f"Train Loss: {train_loss:.4f}, Val Loss: {val_loss:.4f}")
        print(f"Learning Rate: {scheduler.get_last_lr()[0]:.6f}")
        
        # Save best model
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            checkpoint = {
                'epoch': epoch,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'scheduler_state_dict': scheduler.state_dict(),
                'train_loss': train_loss,
                'val_loss': val_loss,
            }
            torch.save(checkpoint, os.path.join(args.output_dir, 'best_model_dem_thermal.pth'))
            print(f"Saved new best model (val_loss: {val_loss:.4f})")
        
        # Save checkpoint every 10 epochs
        if (epoch + 1) % 10 == 0:
            checkpoint = {
                'epoch': epoch,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'scheduler_state_dict': scheduler.state_dict(),
                'train_loss': train_loss,
                'val_loss': val_loss,
            }
            torch.save(checkpoint, os.path.join(args.output_dir, f'checkpoint_epoch_{epoch + 1}.pth'))
    
    print(f"\\nTraining completed! Best validation loss: {best_val_loss:.4f}")

if __name__ == '__main__':
    main()