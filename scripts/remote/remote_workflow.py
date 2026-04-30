#!/usr/bin/env python3
"""Local helpers for syncing code to the campus server and pulling results back."""
from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Dict, List, Sequence, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / ".remote-experiment.env"
FALLBACK_CONFIG_PATH = REPO_ROOT / ".remote-experiment.env.example"
PROFILE_CHOICES = ("runtime", "runtime+tests", "all")
RUNTIME_ROOTS = {"src", "scripts", "config", "prompts"}
RUNTIME_FILES = {"requirements.txt"}
ALWAYS_SKIP_ROOTS = {".git", ".pytest_cache", "__pycache__", "data", "results", "server_outputs"}
ARTIFACT_SKIP_PATTERNS = (
    "./.git/*",
    "./.pytest_cache/*",
    "./__pycache__/*",
    "./*/__pycache__/*",
    "*.pyc",
)


@dataclass(frozen=True)
class ChangeEntry:
    status: str
    path: str
    old_path: str | None = None


@dataclass
class SyncPlan:
    sync_groups: Dict[str, List[Path]]
    delete_paths: List[Path]
    skipped_paths: List[Path]


def split_shell_words(value: str) -> List[str]:
    return shlex.split(value, posix=True) if value else []


def render_command(argv: Sequence[str]) -> str:
    return shlex.join(list(argv))


def run_subprocess(
    argv: Sequence[str],
    *,
    capture_output: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        cwd=REPO_ROOT,
        check=check,
        text=True,
        capture_output=capture_output,
    )


def print_command(argv: Sequence[str]) -> None:
    print(f"$ {render_command(argv)}")


def parse_env_file(path: Path) -> Dict[str, str]:
    config: Dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in raw_line:
            raise ValueError(f"{path}:{line_number} is not KEY=VALUE format")
        key, raw_value = raw_line.split("=", 1)
        key = key.strip()
        values = split_shell_words(raw_value.strip())
        config[key] = " ".join(values) if values else ""
    return config


def load_config(config_arg: str | None) -> Dict[str, str]:
    config_path = Path(config_arg).expanduser() if config_arg else DEFAULT_CONFIG_PATH
    using_fallback = False
    if not config_path.exists():
        if config_arg:
            raise FileNotFoundError(f"config file not found: {config_path}")
        if not FALLBACK_CONFIG_PATH.exists():
            raise FileNotFoundError(
                "missing .remote-experiment.env and .remote-experiment.env.example"
            )
        config_path = FALLBACK_CONFIG_PATH
        using_fallback = True

    config = {
        "REMOTE_LOG_ROOT": "remote_runs",
        "LOCAL_FETCH_ROOT": "server_outputs/campus204",
        "DEFAULT_SYNC_PROFILE": "runtime",
        "DEFAULT_FETCH_PATHS": "results remote_runs",
        "SSH_OPTIONS": "",
        "RSYNC_OPTIONS": "-avz --progress",
    }
    config.update(parse_env_file(config_path))
    for required_key in ("REMOTE_HOST", "REMOTE_PROJECT_ROOT"):
        if not config.get(required_key):
            raise ValueError(f"{config_path} is missing required key {required_key}")
    config["_CONFIG_PATH"] = str(config_path)
    config["_USING_FALLBACK"] = "1" if using_fallback else "0"
    return config


def git_status_payload(repo_root: Path) -> bytes:
    return subprocess.check_output(
        ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"],
        cwd=repo_root,
    )


def parse_git_status_porcelain_z(payload: bytes) -> List[ChangeEntry]:
    entries: List[ChangeEntry] = []
    parts = payload.split(b"\0")
    index = 0
    while index < len(parts):
        part = parts[index]
        index += 1
        if not part:
            continue
        if len(part) < 4 or part[2:3] != b" ":
            raise ValueError(f"unexpected porcelain entry: {part!r}")
        status = part[:2].decode("utf-8", "replace")
        path = part[3:].decode("utf-8", "surrogateescape")
        old_path = None
        if "R" in status or "C" in status:
            if index >= len(parts) or not parts[index]:
                raise ValueError("rename/copy entry missing original path")
            old_path = parts[index].decode("utf-8", "surrogateescape")
            index += 1
        entries.append(ChangeEntry(status=status, path=path, old_path=old_path))
    return entries


def discover_changed_entries(repo_root: Path) -> List[ChangeEntry]:
    return parse_git_status_porcelain_z(git_status_payload(repo_root))


def is_runtime_path(path: Path) -> bool:
    if not path.parts:
        return False
    if any(part == "__pycache__" for part in path.parts):
        return False
    root = path.parts[0]
    if root in ALWAYS_SKIP_ROOTS:
        return False
    if path.as_posix() in RUNTIME_FILES:
        return True
    return root in RUNTIME_ROOTS


def matches_profile(path: Path, profile: str) -> bool:
    if profile == "runtime":
        return is_runtime_path(path)
    if profile == "runtime+tests":
        return is_runtime_path(path) or path.parts[0] == "test"
    if profile == "all":
        return path.parts[0] not in ALWAYS_SKIP_ROOTS and "__pycache__" not in path.parts
    raise ValueError(f"unsupported profile: {profile}")


def add_unique_path(target: List[Path], seen: set[str], path: Path) -> None:
    path_text = path.as_posix()
    if path_text in seen:
        return
    seen.add(path_text)
    target.append(path)


def build_sync_plan(
    repo_root: Path,
    entries: Sequence[ChangeEntry],
    *,
    profile: str,
    delete_removed: bool,
) -> SyncPlan:
    groups: Dict[str, List[Path]] = defaultdict(list)
    group_seen: Dict[str, set[str]] = defaultdict(set)
    delete_paths: List[Path] = []
    delete_seen: set[str] = set()
    skipped_paths: List[Path] = []
    skipped_seen: set[str] = set()

    for entry in entries:
        current_path = Path(entry.path)
        current_exists = (repo_root / current_path).is_file()
        current_allowed = matches_profile(current_path, profile) if current_path.parts else False

        if entry.old_path and delete_removed:
            old_path = Path(entry.old_path)
            if matches_profile(old_path, profile):
                add_unique_path(delete_paths, delete_seen, old_path)

        if "D" in entry.status and not current_exists and entry.old_path is None:
            if current_allowed and delete_removed:
                add_unique_path(delete_paths, delete_seen, current_path)
            else:
                add_unique_path(skipped_paths, skipped_seen, current_path)
            continue

        if current_exists and current_allowed:
            group_key = current_path.parent.as_posix() if current_path.parent.as_posix() != "." else "."
            if current_path.as_posix() not in group_seen[group_key]:
                group_seen[group_key].add(current_path.as_posix())
                groups[group_key].append(current_path)
            continue

        add_unique_path(skipped_paths, skipped_seen, current_path)

    sorted_groups: Dict[str, List[Path]] = {}
    for group_key in sorted(groups):
        sorted_groups[group_key] = sorted(groups[group_key], key=lambda item: item.as_posix())

    delete_paths.sort(key=lambda item: item.as_posix())
    skipped_paths.sort(key=lambda item: item.as_posix())
    return SyncPlan(sync_groups=sorted_groups, delete_paths=delete_paths, skipped_paths=skipped_paths)


def remote_root(config: Dict[str, str]) -> PurePosixPath:
    return PurePosixPath(config["REMOTE_PROJECT_ROOT"])


def remote_path(config: Dict[str, str], relative_path: Path | str) -> str:
    rel_text = relative_path.as_posix() if isinstance(relative_path, Path) else str(relative_path)
    rel_posix = PurePosixPath(rel_text)
    return str(remote_root(config) / rel_posix)


def local_fetch_root(config: Dict[str, str]) -> Path:
    configured = Path(config.get("LOCAL_FETCH_ROOT", "server_outputs/campus204")).expanduser()
    return configured if configured.is_absolute() else REPO_ROOT / configured


def state_dir(config: Dict[str, str]) -> Path:
    return local_fetch_root(config) / "_remote_state"


def last_run_state_path(config: Dict[str, str]) -> Path:
    return state_dir(config) / "last_run.json"


def local_batch_state_path(config: Dict[str, str]) -> Path:
    return state_dir(config) / "local_batch.json"


def save_last_run(config: Dict[str, str], payload: Dict[str, object]) -> None:
    state_directory = state_dir(config)
    state_directory.mkdir(parents=True, exist_ok=True)
    last_run_state_path(config).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def load_last_run(config: Dict[str, str]) -> Dict[str, object]:
    path = last_run_state_path(config)
    if not path.exists():
        raise FileNotFoundError("no previous remote run metadata found")
    return json.loads(path.read_text(encoding="utf-8"))


def save_local_batch_state(config: Dict[str, str], payload: Dict[str, object]) -> None:
    state_directory = state_dir(config)
    state_directory.mkdir(parents=True, exist_ok=True)
    local_batch_state_path(config).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def load_local_batch_state(config: Dict[str, str]) -> Dict[str, object] | None:
    path = local_batch_state_path(config)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def entry_signature(entry: ChangeEntry) -> str:
    return f"{entry.status}|{entry.path}|{entry.old_path or ''}"


def entry_display_path(entry: ChangeEntry) -> str:
    return entry.path


def entry_current_mtime_ns(repo_root: Path, entry: ChangeEntry) -> int | None:
    candidate = repo_root / entry.path
    if candidate.is_file():
        return candidate.stat().st_mtime_ns
    return None


def capture_local_batch_state(
    repo_root: Path,
    entries: Sequence[ChangeEntry],
    *,
    note: str,
) -> Dict[str, object]:
    captured_at_ns = time.time_ns()
    snapshot: Dict[str, Dict[str, object]] = {}
    for entry in entries:
        snapshot[entry_signature(entry)] = {
            "status": entry.status,
            "path": entry.path,
            "old_path": entry.old_path or "",
            "mtime_ns": entry_current_mtime_ns(repo_root, entry),
        }
    return {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "captured_at_ns": captured_at_ns,
        "note": note,
        "entries": snapshot,
    }


def mark_local_batch(
    config: Dict[str, str],
    *,
    entries: Sequence[ChangeEntry] | None = None,
    note: str,
) -> Dict[str, object]:
    current_entries = list(entries) if entries is not None else discover_changed_entries(REPO_ROOT)
    payload = capture_local_batch_state(REPO_ROOT, current_entries, note=note)
    save_local_batch_state(config, payload)
    return payload


def select_recent_entries(
    repo_root: Path,
    entries: Sequence[ChangeEntry],
    baseline_state: Dict[str, object] | None,
    *,
    include_all_dirty: bool,
) -> Tuple[List[ChangeEntry], List[ChangeEntry]]:
    if include_all_dirty or baseline_state is None:
        return list(entries), []

    baseline_entries = baseline_state.get("entries", {})
    if not isinstance(baseline_entries, dict):
        baseline_entries = {}
    baseline_time_ns = baseline_state.get("captured_at_ns", 0)
    if not isinstance(baseline_time_ns, int):
        baseline_time_ns = 0

    recent_entries: List[ChangeEntry] = []
    old_entries: List[ChangeEntry] = []
    for entry in entries:
        signature = entry_signature(entry)
        snapshot_entry = baseline_entries.get(signature)
        current_mtime_ns = entry_current_mtime_ns(repo_root, entry)

        include_entry = snapshot_entry is None
        if not include_entry and current_mtime_ns is not None and current_mtime_ns > baseline_time_ns:
            include_entry = True

        if include_entry:
            recent_entries.append(entry)
        else:
            old_entries.append(entry)
    return recent_entries, old_entries


def build_ssh_command(config: Dict[str, str], remote_command: str) -> List[str]:
    return ["ssh", *split_shell_words(config.get("SSH_OPTIONS", "")), config["REMOTE_HOST"], remote_command]


def build_rsync_base_command(config: Dict[str, str]) -> List[str]:
    command = ["rsync", *split_shell_words(config.get("RSYNC_OPTIONS", "-avz --progress"))]
    ssh_options = config.get("SSH_OPTIONS", "").strip()
    if ssh_options:
        command.extend(["-e", f"ssh {ssh_options}"])
    return command


def build_rsync_command(config: Dict[str, str], source_paths: Sequence[Path], destination: str) -> List[str]:
    command = build_rsync_base_command(config)
    command.extend(str(REPO_ROOT / path) for path in source_paths)
    command.append(destination)
    return command


def build_fetch_rsync_command(config: Dict[str, str], source: str, destination: Path) -> List[str]:
    command = build_rsync_base_command(config)
    command.extend([source, str(destination) + "/"])
    return command


def build_fetch_file_rsync_command(config: Dict[str, str], source: str, destination_dir: Path) -> List[str]:
    command = build_rsync_base_command(config)
    command.extend([source, str(destination_dir) + "/"])
    return command


def build_mkdir_commands(config: Dict[str, str], sync_groups: Dict[str, List[Path]]) -> List[List[str]]:
    remote_directories = []
    for group_key in sync_groups:
        if group_key == ".":
            continue
        remote_directories.append(remote_path(config, group_key))
    if not remote_directories:
        return []
    remote_directories = sorted(set(remote_directories))
    remote_command = "mkdir -p " + " ".join(shlex.quote(item) for item in remote_directories)
    return [build_ssh_command(config, remote_command)]


def build_delete_commands(config: Dict[str, str], delete_paths: Sequence[Path]) -> List[List[str]]:
    if not delete_paths:
        return []
    remote_command = "rm -f " + " ".join(shlex.quote(remote_path(config, item)) for item in delete_paths)
    return [build_ssh_command(config, remote_command)]


def build_sync_commands(config: Dict[str, str], sync_groups: Dict[str, List[Path]]) -> List[List[str]]:
    commands: List[List[str]] = []
    for group_key, paths in sync_groups.items():
        if not paths:
            continue
        target_dir = remote_path(config, group_key)
        destination = f"{config['REMOTE_HOST']}:{target_dir}/"
        commands.append(build_rsync_command(config, paths, destination))
    return commands


def execute_commands(commands: Sequence[Sequence[str]], *, capture_output: bool = False) -> List[subprocess.CompletedProcess[str]]:
    completed: List[subprocess.CompletedProcess[str]] = []
    for command in commands:
        print_command(command)
        completed_process = run_subprocess(command, capture_output=capture_output)
        if capture_output:
            if completed_process.stdout:
                print(completed_process.stdout, end="")
            if completed_process.stderr:
                print(completed_process.stderr, end="", file=sys.stderr)
        completed.append(completed_process)
    return completed


def summarize_sync_plan(plan: SyncPlan) -> str:
    sync_count = sum(len(paths) for paths in plan.sync_groups.values())
    return (
        f"sync {sync_count} files across {len(plan.sync_groups)} directories, "
        f"delete {len(plan.delete_paths)} paths, skip {len(plan.skipped_paths)} paths"
    )


def resolve_sync_profile(config: Dict[str, str], explicit_profile: str | None) -> str:
    profile = explicit_profile or config.get("DEFAULT_SYNC_PROFILE", "runtime")
    if profile not in PROFILE_CHOICES:
        raise ValueError(f"unsupported sync profile: {profile}")
    return profile


def sync_action(
    config: Dict[str, str],
    *,
    profile: str,
    delete_removed: bool,
    dry_run: bool,
    all_dirty: bool,
) -> int:
    current_entries = discover_changed_entries(REPO_ROOT)
    baseline_state = load_local_batch_state(config)
    if baseline_state is None and current_entries and not all_dirty:
        raise ValueError(
            "no local batch baseline found; run ./scripts/remote/mark_local.sh once "
            "to ignore older dirty files, or pass --all-dirty for a one-off full dirty sync"
        )

    recent_entries, old_entries = select_recent_entries(
        REPO_ROOT,
        current_entries,
        baseline_state,
        include_all_dirty=all_dirty,
    )
    plan = build_sync_plan(
        REPO_ROOT,
        recent_entries,
        profile=profile,
        delete_removed=delete_removed,
    )

    print(f"Using config: {config['_CONFIG_PATH']}")
    if config["_USING_FALLBACK"] == "1":
        print("Using tracked example config because .remote-experiment.env was not found.")
    print(f"Profile: {profile}")
    print(summarize_sync_plan(plan))
    if old_entries:
        old_preview = 30
        print(f"Older dirty paths ignored by current local batch baseline (showing up to {old_preview}):")
        for entry in old_entries[:old_preview]:
            print(f"  - {entry_display_path(entry)}")
        if len(old_entries) > old_preview:
            print(f"  ... and {len(old_entries) - old_preview} more")

    if plan.skipped_paths:
        skipped_preview = 40
        print(f"Skipped paths (showing up to {skipped_preview}):")
        for path in plan.skipped_paths[:skipped_preview]:
            print(f"  - {path.as_posix()}")
        if len(plan.skipped_paths) > skipped_preview:
            print(f"  ... and {len(plan.skipped_paths) - skipped_preview} more")

    commands = [
        *build_mkdir_commands(config, plan.sync_groups),
        *build_sync_commands(config, plan.sync_groups),
        *build_delete_commands(config, plan.delete_paths),
    ]

    if not commands:
        print("No sync commands to run.")
        return 0

    if dry_run:
        print("Commands:")
        for command in commands:
            print(render_command(command))
        return 0

    execute_commands(commands)
    mark_local_batch(config, entries=current_entries, note="post-sync baseline")
    return 0


def mark_local_action(config: Dict[str, str]) -> int:
    entries = discover_changed_entries(REPO_ROOT)
    payload = mark_local_batch(config, entries=entries, note="manual local baseline")
    print(f"Using config: {config['_CONFIG_PATH']}")
    print(
        f"Local batch baseline saved with {len(entries)} current dirty entries at "
        f"{payload['captured_at']}"
    )
    print("Files changed after this point will be treated as the next sync batch.")
    return 0


def sanitize_label(label: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", label.strip())
    cleaned = cleaned.strip("-_.")
    return cleaned or "run"


def build_run_id(label: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{timestamp}_{sanitize_label(label)}" if label else timestamp


def normalize_command_parts(parts: Sequence[str]) -> List[str]:
    command = list(parts)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise ValueError("missing remote command after --")
    return command


def build_launch_remote_command(
    config: Dict[str, str],
    *,
    run_id: str,
    command_parts: Sequence[str],
) -> tuple[List[str], str, str]:
    log_root = config.get("REMOTE_LOG_ROOT", "remote_runs").strip("/") or "remote_runs"
    log_relative_path = PurePosixPath(log_root) / f"{run_id}.log"
    marker_relative_path = PurePosixPath(log_root) / ".markers" / f"{run_id}.start"
    env_command = config.get("REMOTE_ENV_COMMAND", "").strip()
    base_command = shlex.join(list(command_parts))
    launch_command = f"PYTHONUNBUFFERED=1 {env_command} {base_command}".strip() if env_command else f"PYTHONUNBUFFERED=1 {base_command}"
    remote_command = (
        "set -euo pipefail; "
        f"cd {shlex.quote(config['REMOTE_PROJECT_ROOT'])}; "
        f"mkdir -p {shlex.quote(log_root)} {shlex.quote(str(marker_relative_path.parent))}; "
        f": > {shlex.quote(str(marker_relative_path))}; "
        f"nohup bash -lc {shlex.quote(launch_command)} > {shlex.quote(str(log_relative_path))} 2>&1 < /dev/null & "
        'pid=$!; '
        f"printf 'RUN_ID=%s\\nPID=%s\\nLOG=%s\\nMARKER=%s\\n' "
        f"{shlex.quote(run_id)} \"$pid\" {shlex.quote(str(log_relative_path))} {shlex.quote(str(marker_relative_path))}"
    )
    return build_ssh_command(config, remote_command), str(log_relative_path), str(marker_relative_path)


def parse_key_value_output(stdout: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key] = value
    return result


def run_action(
    config: Dict[str, str],
    *,
    profile: str,
    delete_removed: bool,
    skip_sync: bool,
    all_dirty: bool,
    label: str,
    run_id: str | None,
    command_parts: Sequence[str],
) -> int:
    if not skip_sync:
        sync_exit_code = sync_action(
            config,
            profile=profile,
            delete_removed=delete_removed,
            dry_run=False,
            all_dirty=all_dirty,
        )
        if sync_exit_code != 0:
            return sync_exit_code

    effective_run_id = run_id or build_run_id(label)
    launch_command, log_relative_path, marker_relative_path = build_launch_remote_command(
        config,
        run_id=effective_run_id,
        command_parts=normalize_command_parts(command_parts),
    )

    print_command(launch_command)
    completed = run_subprocess(launch_command, capture_output=True)
    details = parse_key_value_output(completed.stdout)

    remote_log_path = str(remote_root(config) / PurePosixPath(details.get("LOG", log_relative_path)))
    metadata = {
        "run_id": details.get("RUN_ID", effective_run_id),
        "remote_pid": details.get("PID", ""),
        "remote_host": config["REMOTE_HOST"],
        "remote_project_root": config["REMOTE_PROJECT_ROOT"],
        "remote_log_path": remote_log_path,
        "remote_marker_path": str(
            remote_root(config) / PurePosixPath(details.get("MARKER", marker_relative_path))
        ),
        "command": normalize_command_parts(command_parts),
        "launched_at": datetime.now().isoformat(timespec="seconds"),
    }
    save_last_run(config, metadata)

    print(f"Run ID: {metadata['run_id']}")
    print(f"Remote PID: {metadata['remote_pid'] or '<unknown>'}")
    print(f"Remote log: {metadata['remote_log_path']}")
    print("Next commands:")
    print(f"  ./scripts/remote/tail_remote.sh {metadata['run_id']}")
    print("  ./scripts/remote/fetch_remote.sh")
    return 0


def fetch_paths_from_args(config: Dict[str, str], explicit_paths: Sequence[str]) -> List[str]:
    if explicit_paths:
        return [item.rstrip("/") for item in explicit_paths]
    defaults = split_shell_words(config.get("DEFAULT_FETCH_PATHS", "results remote_runs"))
    return [item.rstrip("/") for item in defaults]


def normalize_relative_find_path(path_text: str) -> str:
    normalized = path_text.strip()
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def discover_remote_artifacts_since(
    config: Dict[str, str],
    *,
    marker_path: str,
    excluded_roots: Sequence[str],
) -> List[str]:
    command_parts = [
        "set -euo pipefail;",
        f"cd {shlex.quote(config['REMOTE_PROJECT_ROOT'])};",
        f"find . -type f -newer {shlex.quote(marker_path)}",
    ]
    for pattern in ARTIFACT_SKIP_PATTERNS:
        command_parts.append(f"-not -path {shlex.quote(pattern)}")
    for root in excluded_roots:
        if not root:
            continue
        command_parts.append(f"-not -path {shlex.quote('./' + root)}")
        command_parts.append(f"-not -path {shlex.quote('./' + root + '/*')}")
    command_parts.append("-print0")
    remote_command = " ".join(command_parts)
    payload = subprocess.check_output(
        build_ssh_command(config, remote_command),
        cwd=REPO_ROOT,
    )
    artifacts = [normalize_relative_find_path(item.decode("utf-8", "surrogateescape")) for item in payload.split(b"\0") if item]
    return sorted({item for item in artifacts if item})


def fetch_action(config: Dict[str, str], *, paths: Sequence[str]) -> int:
    fetch_root = local_fetch_root(config)
    fetch_root.mkdir(parents=True, exist_ok=True)
    print(f"Using config: {config['_CONFIG_PATH']}")
    print(f"Fetch root: {fetch_root}")
    fetch_paths = fetch_paths_from_args(config, paths)

    commands: List[List[str]] = []
    for relative in fetch_paths:
        source = f"{config['REMOTE_HOST']}:{remote_path(config, relative)}/"
        destination = fetch_root / relative
        destination.mkdir(parents=True, exist_ok=True)
        commands.append(build_fetch_rsync_command(config, source, destination))

    execute_commands(commands)

    try:
        last_run = load_last_run(config)
    except FileNotFoundError:
        print("No recorded remote run metadata found; skipped extra artifact discovery.")
        return 0

    marker_path = last_run.get("remote_marker_path")
    if not isinstance(marker_path, str) or not marker_path:
        print("Latest remote run metadata has no marker path; skipped extra artifact discovery.")
        return 0

    extra_artifacts = discover_remote_artifacts_since(
        config,
        marker_path=marker_path,
        excluded_roots=fetch_paths,
    )
    if not extra_artifacts:
        print("No extra remote artifacts detected outside the default fetch roots.")
        return 0

    print(f"Extra remote artifacts detected: {len(extra_artifacts)}")
    artifact_commands: List[List[str]] = []
    for relative in extra_artifacts:
        destination_dir = (fetch_root / relative).parent
        destination_dir.mkdir(parents=True, exist_ok=True)
        source = f"{config['REMOTE_HOST']}:{remote_path(config, relative)}"
        artifact_commands.append(build_fetch_file_rsync_command(config, source, destination_dir))

    execute_commands(artifact_commands)
    return 0


def resolve_log_path(config: Dict[str, str], run_id: str | None) -> str:
    if run_id:
        log_root = config.get("REMOTE_LOG_ROOT", "remote_runs").strip("/") or "remote_runs"
        return str(remote_root(config) / PurePosixPath(log_root) / f"{run_id}.log")
    metadata = load_last_run(config)
    remote_log_path = metadata.get("remote_log_path")
    if not isinstance(remote_log_path, str) or not remote_log_path:
        raise ValueError("last run metadata does not contain remote_log_path")
    return remote_log_path


def tail_action(config: Dict[str, str], *, run_id: str | None, lines: int) -> int:
    log_path = resolve_log_path(config, run_id)
    command = build_ssh_command(config, f"tail -n {lines} -f {shlex.quote(log_path)}")
    print_command(command)
    return subprocess.call(command, cwd=REPO_ROOT)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Remote experiment helper for SpatialText2SQL.")
    parser.add_argument("--config", default="", help="Optional path to a remote workflow env file.")

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    sync_parser = subparsers.add_parser("sync", help="Sync changed files to the remote project.")
    sync_parser.add_argument("--profile", choices=PROFILE_CHOICES, default=None)
    sync_parser.add_argument("--delete-removed", action="store_true", help="Delete removed files on the remote side.")
    sync_parser.add_argument("--dry-run", action="store_true", help="Show the commands without executing them.")
    sync_parser.add_argument("--all-dirty", action="store_true", help="Ignore the local batch baseline and sync all dirty files.")

    subparsers.add_parser("mark-local", help="Freeze the current dirty workspace as the local sync baseline.")

    run_parser = subparsers.add_parser("run", help="Sync code and launch a remote command in the background.")
    run_parser.add_argument("--profile", choices=PROFILE_CHOICES, default=None)
    run_parser.add_argument("--delete-removed", action="store_true", help="Delete removed files on the remote side.")
    run_parser.add_argument("--skip-sync", action="store_true", help="Launch without syncing first.")
    run_parser.add_argument("--all-dirty", action="store_true", help="Ignore the local batch baseline and sync all dirty files.")
    run_parser.add_argument("--label", default="", help="Short label appended to the generated run id.")
    run_parser.add_argument("--run-id", default="", help="Explicit run id instead of an auto-generated one.")
    run_parser.add_argument("command", nargs=argparse.REMAINDER, help="Remote command after --")

    fetch_parser = subparsers.add_parser("fetch", help="Fetch remote results and logs back to the local machine.")
    fetch_parser.add_argument("--path", action="append", default=[], help="Relative remote path to fetch; repeatable.")

    tail_parser = subparsers.add_parser("tail", help="Tail the latest or a specific remote run log.")
    tail_parser.add_argument("run_id", nargs="?", default="", help="Specific run id to tail.")
    tail_parser.add_argument("--lines", type=int, default=80, help="Initial number of log lines to show.")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config or None)
        if args.subcommand == "sync":
            profile = resolve_sync_profile(config, args.profile)
            return sync_action(
                config,
                profile=profile,
                delete_removed=args.delete_removed,
                dry_run=args.dry_run,
                all_dirty=args.all_dirty,
            )
        if args.subcommand == "mark-local":
            return mark_local_action(config)
        if args.subcommand == "run":
            profile = resolve_sync_profile(config, args.profile)
            return run_action(
                config,
                profile=profile,
                delete_removed=args.delete_removed,
                skip_sync=args.skip_sync,
                all_dirty=args.all_dirty,
                label=args.label,
                run_id=args.run_id or None,
                command_parts=args.command,
            )
        if args.subcommand == "fetch":
            return fetch_action(config, paths=args.path)
        if args.subcommand == "tail":
            return tail_action(config, run_id=args.run_id or None, lines=args.lines)
        raise ValueError(f"unsupported subcommand: {args.subcommand}")
    except (FileNotFoundError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if isinstance(exc, subprocess.CalledProcessError):
            if exc.stdout:
                print(exc.stdout, file=sys.stderr, end="")
            if exc.stderr:
                print(exc.stderr, file=sys.stderr, end="")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
