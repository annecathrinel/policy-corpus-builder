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

            original_user = os.environ.get("EURLEX_USER")
            os.environ["EURLEX_USER"] = "existing-user"
            try:
                loaded_path = load_local_env(start_path=module_dir)
                self.assertEqual(loaded_path, env_path)
                self.assertEqual(find_local_env(start_path=module_dir), env_path)
                self.assertEqual(os.environ.get("REGULATIONS_GOV_API_KEY"), "local-us-key")
                self.assertEqual(os.environ.get("EURLEX_USER"), "existing-user")
                self.assertEqual(os.environ.get("EURLEX_WEB_PASS"), "quoted-pass")
            finally:
                for key in ("REGULATIONS_GOV_API_KEY", "EURLEX_WEB_PASS"):
                    os.environ.pop(key, None)
                if original_user is None:
                    os.environ.pop("EURLEX_USER", None)
                else:
                    os.environ["EURLEX_USER"] = original_user


if __name__ == "__main__":
    unittest.main()
