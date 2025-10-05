"""
SatLas Foundation Model Data Adapters

Data handling specifically designed for SatLas foundation model experiments.
"""

from .four_modal_dataset_adapter import (
    SatlasMultimodalDataset,
    SatlasDataModule,
    FourModalDataModule  # Alias for compatibility
)

__all__ = [
    'SatlasMultimodalDataset',
    'SatlasDataModule',
    'FourModalDataModule'
]