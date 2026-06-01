"""TRL fine-tuning utilities for spatial Text-to-SQL."""

from .config import (
    DEFAULT_TRL_FINETUNE_CONFIG_PATH,
    FinetuneRuntimeConfig,
    SpatialText2SQLFinetuneConfig,
    load_trl_finetune_config,
    override_trl_finetune_config,
)
from .dataset import SpatialText2SQLDatasetBuilder
from .formatter import NL2SQLAlpacaFormatter
from .io import (
    load_alpaca_finetune_samples,
    load_raw_finetune_samples,
    load_prepared_finetune_samples,
    write_alpaca_finetune_samples,
    write_prepared_finetune_samples,
    write_raw_finetune_samples,
)
from .models import AlpacaFinetuneSample, PreparedFinetuneSample, RawFinetuneSample


def __getattr__(name: str):
    if name == "TRLFullFinetuner":
        from .trainer import TRLFullFinetuner

        return TRLFullFinetuner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "DEFAULT_TRL_FINETUNE_CONFIG_PATH",
    "AlpacaFinetuneSample",
    "FinetuneRuntimeConfig",
    "NL2SQLAlpacaFormatter",
    "PreparedFinetuneSample",
    "RawFinetuneSample",
    "SpatialText2SQLDatasetBuilder",
    "SpatialText2SQLFinetuneConfig",
    "TRLFullFinetuner",
    "load_alpaca_finetune_samples",
    "load_prepared_finetune_samples",
    "load_raw_finetune_samples",
    "load_trl_finetune_config",
    "override_trl_finetune_config",
    "write_alpaca_finetune_samples",
    "write_prepared_finetune_samples",
    "write_raw_finetune_samples",
]
