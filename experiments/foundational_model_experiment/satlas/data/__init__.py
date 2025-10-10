"""
SatLas Foundation Model Data Adapters

Data handling specifically designed for SatLas foundation model experiments.
"""

from .four_modal_dataset_adapter import (
    FourModalPatchDatasetAdapter,
    FourModalPatchDataset,
    FourModalDataModule
)

# Alias for backward compatibility
SatlasDataModule = FourModalDataModule

__all__ = [
    'FourModalPatchDatasetAdapter',
    'FourModalPatchDataset', 
    'FourModalDataModule',
    'SatlasDataModule'
]