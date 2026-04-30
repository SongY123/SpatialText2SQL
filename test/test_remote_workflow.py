from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "remote" / "remote_workflow.py"
SPEC = importlib.util.spec_from_file_location("remote_workflow", MODULE_PATH)
remote_workflow = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = remote_workflow
SPEC.loader.exec_module(remote_workflow)


class ParseGitStatusTests(unittest.TestCase):
    def test_parse_git_status_handles_rename_delete_and_untracked(self):
        payload = (
            b"R  scripts/new_name.py\0scripts/old_name.py\0"
            b" D config/obsolete.yaml\0"
            b"?? test/test_remote.py\0"
        )

        entries = remote_workflow.parse_git_status_porcelain_z(payload)

        self.assertEqual(
            entries,
            [
                remote_workflow.ChangeEntry(
                    status="R ",
                    path="scripts/new_name.py",
                    old_path="scripts/old_name.py",
                ),
                remote_workflow.ChangeEntry(
                    status=" D",
                    path="config/obsolete.yaml",
                    old_path=None,
                ),
                remote_workflow.ChangeEntry(
                    status="??",
                    path="test/test_remote.py",
                    old_path=None,
                ),
            ],
        )


class SyncPlanTests(unittest.TestCase):
    def test_runtime_profile_skips_tests_and_docs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            (repo_root / "src").mkdir()
            (repo_root / "src" / "core.py").write_text("print('ok')\n", encoding="utf-8")
            (repo_root / "test").mkdir()
            (repo_root / "test" / "test_core.py").write_text("pass\n", encoding="utf-8")
            (repo_root / "docs").mkdir()
            (repo_root / "docs" / "note.md").write_text("note\n", encoding="utf-8")
            (repo_root / "requirements.txt").write_text("pyyaml\n", encoding="utf-8")

            plan = remote_workflow.build_sync_plan(
                repo_root,
                [
                    remote_workflow.ChangeEntry(status=" M", path="src/core.py"),
                    remote_workflow.ChangeEntry(status="??", path="test/test_core.py"),
                    remote_workflow.ChangeEntry(status="??", path="docs/note.md"),
                    remote_workflow.ChangeEntry(status=" M", path="requirements.txt"),
                ],
                profile="runtime",
                delete_removed=False,
            )

            self.assertEqual([path.as_posix() for path in plan.sync_groups["src"]], ["src/core.py"])
            self.assertEqual([path.as_posix() for path in plan.sync_groups["."]], ["requirements.txt"])
            self.assertEqual(
                [path.as_posix() for path in plan.skipped_paths],
                ["docs/note.md", "test/test_core.py"],
            )

    def test_runtime_plus_tests_includes_test_files_and_rename_delete(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            (repo_root / "src").mkdir()
            (repo_root / "src" / "new_name.py").write_text("print('new')\n", encoding="utf-8")
            (repo_root / "test").mkdir()
            (repo_root / "test" / "test_core.py").write_text("pass\n", encoding="utf-8")

            plan = remote_workflow.build_sync_plan(
                repo_root,
                [
                    remote_workflow.ChangeEntry(
                        status="R ",
                        path="src/new_name.py",
                        old_path="src/old_name.py",
                    ),
                    remote_workflow.ChangeEntry(status="??", path="test/test_core.py"),
                ],
                profile="runtime+tests",
                delete_removed=True,
            )

            self.assertEqual([path.as_posix() for path in plan.sync_groups["src"]], ["src/new_name.py"])
            self.assertEqual([path.as_posix() for path in plan.sync_groups["test"]], ["test/test_core.py"])
            self.assertEqual([path.as_posix() for path in plan.delete_paths], ["src/old_name.py"])


class LocalBatchTests(unittest.TestCase):
    def test_select_recent_entries_ignores_old_dirty_files_but_keeps_newer_edits(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            (repo_root / "src").mkdir()
            old_file = repo_root / "src" / "old_dirty.py"
            new_file = repo_root / "src" / "recent_dirty.py"
            old_file.write_text("print('old')\n", encoding="utf-8")
            new_file.write_text("print('new')\n", encoding="utf-8")

            baseline_time_ns = time_ns = 1_750_000_000_000_000_000
            os.utime(old_file, ns=(baseline_time_ns - 5_000_000_000, baseline_time_ns - 5_000_000_000))
            os.utime(new_file, ns=(baseline_time_ns + 5_000_000_000, baseline_time_ns + 5_000_000_000))

            old_entry = remote_workflow.ChangeEntry(status=" M", path="src/old_dirty.py")
            new_entry = remote_workflow.ChangeEntry(status=" M", path="src/recent_dirty.py")
            baseline_state = {
                "captured_at_ns": baseline_time_ns,
                "entries": {
                    remote_workflow.entry_signature(old_entry): {
                        "status": old_entry.status,
                        "path": old_entry.path,
                        "old_path": "",
                        "mtime_ns": baseline_time_ns - 5_000_000_000,
                    },
                    remote_workflow.entry_signature(new_entry): {
                        "status": new_entry.status,
                        "path": new_entry.path,
                        "old_path": "",
                        "mtime_ns": baseline_time_ns - 1,
                    },
                },
            }

            recent_entries, old_entries = remote_workflow.select_recent_entries(
                repo_root,
                [old_entry, new_entry],
                baseline_state,
                include_all_dirty=False,
            )

            self.assertEqual([entry.path for entry in recent_entries], ["src/recent_dirty.py"])
            self.assertEqual([entry.path for entry in old_entries], ["src/old_dirty.py"])

    @patch.object(remote_workflow.subprocess, "check_output")
    def test_discover_remote_artifacts_since_returns_relative_paths(self, mock_check_output):
        mock_check_output.return_value = (
            b"./scripts/spatialsql/geometry_validation_report.json\0"
            b"./docs/generated_summary.json\0"
        )
        config = {
            "REMOTE_HOST": "example@server",
            "REMOTE_PROJECT_ROOT": "/remote/project",
            "SSH_OPTIONS": "",
        }

        artifacts = remote_workflow.discover_remote_artifacts_since(
            config,
            marker_path="/remote/project/remote_runs/.markers/run.start",
            excluded_roots=["results", "remote_runs"],
        )

        self.assertEqual(
            artifacts,
            [
                "docs/generated_summary.json",
                "scripts/spatialsql/geometry_validation_report.json",
            ],
        )
        ssh_args = mock_check_output.call_args.args[0]
        self.assertEqual(ssh_args[:2], ["ssh", "example@server"])
        self.assertIn("find . -type f -newer /remote/project/remote_runs/.markers/run.start", ssh_args[2])


class DiscoverChangedEntriesTests(unittest.TestCase):
    def test_discover_changed_entries_reads_temp_repo_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            subprocess.run(["git", "init", "-q"], cwd=repo_root, check=True)
            subprocess.run(["git", "config", "user.email", "tester@example.com"], cwd=repo_root, check=True)
            subprocess.run(["git", "config", "user.name", "Tester"], cwd=repo_root, check=True)

            (repo_root / "src").mkdir()
            (repo_root / "src" / "core.py").write_text("print('v1')\n", encoding="utf-8")
            subprocess.run(["git", "add", "src/core.py"], cwd=repo_root, check=True)
            subprocess.run(["git", "commit", "-qm", "init"], cwd=repo_root, check=True)

            (repo_root / "src" / "core.py").write_text("print('v2')\n", encoding="utf-8")
            (repo_root / "test").mkdir()
            (repo_root / "test" / "test_core.py").write_text("pass\n", encoding="utf-8")

            entries = remote_workflow.discover_changed_entries(repo_root)
            entry_map = {entry.path: entry.status for entry in entries}

            self.assertEqual(entry_map["src/core.py"], " M")
            self.assertEqual(entry_map["test/test_core.py"], "??")


if __name__ == "__main__":
    unittest.main()
