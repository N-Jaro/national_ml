"""
SatLas Foundation Model Utilities

Utilities specifically designed for SatLas foundation model experiments.
"""

from .losses import (
    SatlasFocalLoss,
    SatlasDiceLoss, 
    SatlasCombinedLoss,
    SatlasIoULoss,
    SatlasAdaptiveLoss,
    CombinedFocalDiceLoss  # Alias for compatibility
)

__all__ = [
    'SatlasFocalLoss',
    'SatlasDiceLoss',
    'SatlasCombinedLoss', 
    'SatlasIoULoss',
    'SatlasAdaptiveLoss',
    'CombinedFocalDiceLoss'
]