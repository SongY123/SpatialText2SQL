"""Utilities for making Gemma4 checkpoints loadable by stricter runtimes."""

from __future__ import annotations

import json
import logging
import os
import shutil
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import torch

LOGGER = logging.getLogger(__name__)


WEIGHT_SUFFIXES = (".safetensors", ".bin")
WEIGHT_INDEX_NAMES = {
    "model.safetensors.index.json",
    "pytorch_model.bin.index.json",
}


def materialize_gemma4_k_norm_weights(
    *,
    checkpoint_dir: str | Path,
    base_model_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    model_state_dict: Mapping[str, torch.Tensor] | Callable[[], Mapping[str, torch.Tensor] | None] | None = None,
    safe_serialization: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Materialize Gemma4 KV-shared k_norm alias keys for vLLM.

    Gemma4 E4B stores the last ``num_kv_shared_layers`` decoder layers with shared
    KV-attention state. Transformers can load checkpoints where some shared alias
    keys are omitted, but vLLM currently expects the alias keys to be present in
    the serialized checkpoint. This function adds the missing
    ``model.language_model.layers.{i}.self_attn.k_norm.weight`` keys.
    """

    source_dir = Path(checkpoint_dir).expanduser().resolve()
    if output_dir is None:
        target_dir = source_dir
    else:
        target_dir = Path(output_dir).expanduser().resolve()

    config = _load_config(source_dir)
    required_keys = _required_gemma4_k_norm_keys(config)
    if not required_keys:
        return {
            "status": "skipped",
            "reason": "not_gemma4_or_no_kv_shared_layers",
            "checkpoint_dir": str(source_dir),
            "output_dir": str(target_dir),
        }

    existing_keys = _load_weight_keys(source_dir)
    missing_keys = [key for key in required_keys if key not in existing_keys]
    if not missing_keys:
        if target_dir != source_dir and not dry_run:
            _copy_auxiliary_files(source_dir, target_dir)
            _copy_weight_files(source_dir, target_dir)
        return {
            "status": "ok",
            "reason": "all_required_keys_present",
            "checkpoint_dir": str(source_dir),
            "output_dir": str(target_dir),
            "required_key_count": len(required_keys),
            "missing_key_count": 0,
        }

    result: dict[str, Any] = {
        "status": "missing",
        "checkpoint_dir": str(source_dir),
        "output_dir": str(target_dir),
        "required_key_count": len(required_keys),
        "missing_key_count": len(missing_keys),
        "missing_keys": missing_keys,
    }
    if dry_run:
        return result

    state_dict_provider = _resolve_state_dict_provider(model_state_dict)
    patch_tensors: dict[str, torch.Tensor] = {}
    state_dict: Mapping[str, torch.Tensor] | None = None
    for key in missing_keys:
        tensor = None
        if state_dict_provider is not None:
            if state_dict is None:
                state_dict = state_dict_provider()
            if state_dict is not None:
                tensor = _tensor_from_state_dict(state_dict, key)
        if tensor is None and base_model_dir:
            tensor = _read_tensor_from_safetensors(Path(base_model_dir).expanduser().resolve(), key)
        if tensor is None:
            raise KeyError(
                f"Failed to materialize required Gemma4 key {key!r}. "
                "Provide a base_model_dir containing the original Gemma4 weights."
            )
        patch_tensors[key] = _clone_tensor_for_serialization(tensor)

    target_state = _load_safetensors_state_dict(source_dir)
    target_state.update(patch_tensors)
    _copy_auxiliary_files(source_dir, target_dir)
    _write_state_dict(
        target_state,
        target_dir,
        safe_serialization=safe_serialization,
    )
    result.update(
        {
            "status": "repaired",
            "written_format": "safetensors" if safe_serialization else "bin",
            "written_key_count": len(target_state),
            "patched_key_count": len(patch_tensors),
        }
    )
    return result


def _load_config(checkpoint_dir: Path) -> dict[str, Any]:
    config_path = checkpoint_dir / "config.json"
    if not config_path.is_file():
        raise FileNotFoundError(f"Missing config.json in checkpoint directory: {checkpoint_dir}")
    return json.loads(config_path.read_text(encoding="utf-8"))


def _required_gemma4_k_norm_keys(config: Mapping[str, Any]) -> list[str]:
    if str(config.get("model_type") or "").lower() != "gemma4":
        return []
    text_config = config.get("text_config") or {}
    try:
        num_hidden_layers = int(text_config.get("num_hidden_layers") or 0)
        num_kv_shared_layers = int(text_config.get("num_kv_shared_layers") or 0)
    except (TypeError, ValueError):
        return []
    if num_hidden_layers <= 0 or num_kv_shared_layers <= 0:
        return []
    first_shared_layer = max(num_hidden_layers - num_kv_shared_layers, 0)
    return [
        f"model.language_model.layers.{index}.self_attn.k_norm.weight"
        for index in range(first_shared_layer, num_hidden_layers)
    ]


def _resolve_state_dict_provider(
    model_state_dict: Mapping[str, torch.Tensor] | Callable[[], Mapping[str, torch.Tensor] | None] | None,
) -> Callable[[], Mapping[str, torch.Tensor] | None] | None:
    if model_state_dict is None:
        return None
    if callable(model_state_dict):
        return model_state_dict
    return lambda: model_state_dict


def _load_weight_keys(checkpoint_dir: Path) -> set[str]:
    keys: set[str] = set()
    for path in _safetensors_files(checkpoint_dir):
        from safetensors import safe_open

        with safe_open(path, framework="pt", device="cpu") as handle:
            keys.update(handle.keys())
    for path in _bin_files(checkpoint_dir):
        payload = torch.load(path, map_location="cpu", weights_only=True)
        if isinstance(payload, Mapping):
            keys.update(str(key) for key in payload.keys())
    return keys


def _load_safetensors_state_dict(checkpoint_dir: Path) -> dict[str, torch.Tensor]:
    files = _safetensors_files(checkpoint_dir)
    if not files:
        raise FileNotFoundError(f"No root-level safetensors files found in {checkpoint_dir}")
    state: dict[str, torch.Tensor] = {}
    from safetensors import safe_open

    for path in files:
        with safe_open(path, framework="pt", device="cpu") as handle:
            for key in handle.keys():
                state[key] = _clone_tensor_for_serialization(handle.get_tensor(key))
    return state


def _read_tensor_from_safetensors(checkpoint_dir: Path, key: str) -> torch.Tensor | None:
    from safetensors import safe_open

    for path in _safetensors_files(checkpoint_dir):
        with safe_open(path, framework="pt", device="cpu") as handle:
            if key in handle.keys():
                return handle.get_tensor(key)
    return None


def _tensor_from_state_dict(state_dict: Mapping[str, torch.Tensor], key: str) -> torch.Tensor | None:
    tensor = state_dict.get(key)
    if tensor is None:
        return None
    if getattr(tensor, "is_meta", False):
        return None
    if tensor.numel() == 0:
        return None
    return tensor


def _clone_tensor_for_serialization(tensor: torch.Tensor) -> torch.Tensor:
    return tensor.detach().cpu().contiguous().clone()


def _safetensors_files(checkpoint_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in checkpoint_dir.glob("*.safetensors")
        if path.is_file() and not path.name.startswith("optimizer")
    )


def _bin_files(checkpoint_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in checkpoint_dir.glob("*.bin")
        if path.is_file() and path.name.startswith("pytorch_model")
    )


def _copy_auxiliary_files(source_dir: Path, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    for item in source_dir.iterdir():
        if _is_weight_artifact(item):
            continue
        if item.is_dir() and item.name.startswith("checkpoint-"):
            continue
        destination = target_dir / item.name
        if item.is_dir():
            if destination.exists():
                shutil.rmtree(destination)
            shutil.copytree(item, destination)
        elif item.is_file():
            shutil.copy2(item, destination)


def _copy_weight_files(source_dir: Path, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    for item in source_dir.iterdir():
        if not _is_weight_artifact(item):
            continue
        if item.is_file():
            shutil.copy2(item, target_dir / item.name)


def _is_weight_artifact(path: Path) -> bool:
    if path.name in WEIGHT_INDEX_NAMES:
        return True
    if path.suffix in WEIGHT_SUFFIXES:
        return True
    return False


def _write_state_dict(
    state_dict: Mapping[str, torch.Tensor],
    target_dir: Path,
    *,
    safe_serialization: bool,
) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    _remove_weight_artifacts(target_dir)
    if safe_serialization:
        from safetensors.torch import save_file

        tmp_path = target_dir / "model.safetensors.tmp"
        final_path = target_dir / "model.safetensors"
        save_file(dict(state_dict), str(tmp_path), metadata={"format": "pt"})
        os.replace(tmp_path, final_path)
        return

    tmp_path = target_dir / "pytorch_model.bin.tmp"
    final_path = target_dir / "pytorch_model.bin"
    torch.save(dict(state_dict), tmp_path)
    os.replace(tmp_path, final_path)


def _remove_weight_artifacts(target_dir: Path) -> None:
    for item in list(target_dir.iterdir()):
        if _is_weight_artifact(item) and item.is_file():
            item.unlink()
