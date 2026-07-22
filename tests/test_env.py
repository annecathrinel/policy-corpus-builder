import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from policy_corpus_builder.env import find_local_env, load_local_env  # noqa: E402


class LocalEnvTests(unittest.TestCase):
    def test_load_local_env_reads_repo_env_without_overriding_existing_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            module_dir = repo_root / "src" / "policy_corpus_builder"
            module_dir.mkdir(parents=True)
            env_path = repo_root / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "REGULATIONS_GOV_API_KEY=local-us-key",
                        "EURLEX_USER=local-user",
                        "EURLEX_WEB_PASS='quoted-pass'",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            # Importing policy_corpus_builder already ran its own module-level
            # load_local_env() against this *real* repo's .env, so
            # REGULATIONS_GOV_API_KEY/EURLEX_WEB_PASS may already be sitting in
            # os.environ before this test even starts. Snapshot and clear them so
            # the "don't override an existing value" behavior under test is
            # actually exercised against a known, controlled starting state.
            original_values = {
                key: os.environ.get(key)
                for key in ("EURLEX_USER", "REGULATIONS_GOV_API_KEY", "EURLEX_WEB_PASS")
            }
            os.environ.pop("REGULATIONS_GOV_API_KEY", None)
            os.environ.pop("EURLEX_WEB_PASS", None)
            os.environ["EURLEX_USER"] = "existing-user"
            try:
                loaded_path = load_local_env(start_path=module_dir)
                self.assertEqual(loaded_path, env_path)
                self.assertEqual(find_local_env(start_path=module_dir), env_path)
                self.assertEqual(os.environ.get("REGULATIONS_GOV_API_KEY"), "local-us-key")
                self.assertEqual(os.environ.get("EURLEX_USER"), "existing-user")
                self.assertEqual(os.environ.get("EURLEX_WEB_PASS"), "quoted-pass")
            finally:
                for key, original_value in original_values.items():
                    if original_value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = original_value


if __name__ == "__main__":
    unittest.main()
