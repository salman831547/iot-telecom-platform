"""Config-driven Terraform wrapper: the ONLY way Terraform is invoked (locally and in CI).

Every layer gets its backend bucket/prefix and ``TF_VAR_env`` from
``config/envs/<env>.yaml``, so no backend values are hand-typed anywhere.

Usage:
    uv run python scripts/tf.py bootstrap                  # first-time: local state -> GCS
    uv run python scripts/tf.py init  <layer> [--env dev]
    uv run python scripts/tf.py plan  <layer> [-- <extra terraform args>]
    uv run python scripts/tf.py apply <layer> [-- -auto-approve]
    uv run python scripts/tf.py destroy <layer>
    uv run python scripts/tf.py output <layer> [-- -json]

GitHub repo settings (variables, environment, ruleset): scripts/github_repo.py

Layers: bootstrap | foundation | composer
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from iot_pipeline.config import EnvConfig, load_config  # noqa: E402

TF_ROOT = REPO_ROOT / "infra" / "terraform"
LAYER_DIRS: dict[str, Path] = {
    "bootstrap": TF_ROOT / "00-bootstrap",
    "foundation": TF_ROOT / "10-foundation",
    "composer": TF_ROOT / "20-composer",
}
LOCAL_OVERRIDE = "backend_override.tf"  # git-ignored (*_override.tf)


def _log(msg: str) -> None:
    print(f"\n==> {msg}", flush=True)


def _terraform() -> str:
    exe = shutil.which("terraform")
    if not exe:
        sys.exit("terraform not found on PATH")
    return exe


def run(args: Sequence[str], cwd: Path, env: dict[str, str], check: bool = True) -> int:
    _log(" ".join(["terraform", *args]) + f"   (in {cwd.relative_to(REPO_ROOT)})")
    result = subprocess.run([_terraform(), *args], cwd=cwd, env=env, check=False)  # noqa: S603
    if check and result.returncode != 0:
        sys.exit(result.returncode)
    return result.returncode


def tf_env(cfg: EnvConfig) -> dict[str, str]:
    env = os.environ.copy()
    env["TF_VAR_env"] = cfg.env
    if os.environ.get("CI"):
        env["TF_IN_AUTOMATION"] = "1"  # terser output, no "run terraform apply" hints
        env["TF_INPUT"] = "0"  # never block on interactive prompts in CI
    return env


def backend_args(cfg: EnvConfig, layer: str) -> list[str]:
    prefix = getattr(cfg.terraform.state_prefixes, layer)
    return [
        f"-backend-config=bucket={cfg.terraform.state_bucket}",
        f"-backend-config=prefix={prefix}",
    ]


def cmd_init(cfg: EnvConfig, layer: str, extra: list[str]) -> None:
    run(["init", "-reconfigure", *backend_args(cfg, layer), *extra], LAYER_DIRS[layer], tf_env(cfg))


def cmd_passthrough(cfg: EnvConfig, layer: str, action: str, extra: list[str]) -> None:
    env = tf_env(cfg)
    # `.terraform/terraform.tfstate` records the initialised backend; a bare `.terraform/`
    # (e.g. from `init -backend=false` for validation) is not enough to plan or apply.
    if not (LAYER_DIRS[layer] / ".terraform" / "terraform.tfstate").is_file():
        cmd_init(cfg, layer, [])
    run([action, *extra], LAYER_DIRS[layer], env)


def _state_bucket_exists(cfg: EnvConfig) -> bool:
    from google.api_core.exceptions import Forbidden
    from google.cloud import storage

    client = storage.Client(project=cfg.project.id)
    try:
        return client.lookup_bucket(cfg.terraform.state_bucket) is not None
    except Forbidden:
        sys.exit(f"bucket {cfg.terraform.state_bucket} exists but is not accessible")


def cmd_bootstrap(cfg: EnvConfig, extra: list[str]) -> None:
    """Chicken-and-egg: 00-bootstrap creates the bucket that stores its own state.

    1. If the bucket already exists -> normal init/apply against GCS (idempotent re-run).
    2. Otherwise apply with a temporary local backend, then migrate the state into GCS
       and remove the local state file.
    """
    layer_dir = LAYER_DIRS["bootstrap"]
    env = tf_env(cfg)
    override = layer_dir / LOCAL_OVERRIDE

    if _state_bucket_exists(cfg):
        _log("state bucket exists -> regular apply on remote state")
        cmd_init(cfg, "bootstrap", [])
        run(["apply", *extra], layer_dir, env)
        return

    _log("state bucket missing -> first apply with local state")
    override.write_text('terraform {\n  backend "local" {}\n}\n', encoding="utf-8")
    try:
        run(["init", "-reconfigure"], layer_dir, env)
        run(["apply", *extra], layer_dir, env)
    finally:
        override.unlink(missing_ok=True)

    _log("migrating local state -> gs://" + cfg.terraform.state_bucket)
    run(
        ["init", "-migrate-state", "-force-copy", *backend_args(cfg, "bootstrap")],
        layer_dir,
        env,
    )
    for leftover in ("terraform.tfstate", "terraform.tfstate.backup"):
        (layer_dir / leftover).unlink(missing_ok=True)
    run(["state", "list"], layer_dir, env)  # proves remote state is readable


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    extra: list[str] = []
    if "--" in argv:
        idx = argv.index("--")
        argv, extra = argv[:idx], argv[idx + 1 :]

    parser = argparse.ArgumentParser(prog="scripts/tf.py", description=__doc__.split("\n")[0])
    parser.add_argument(
        "command", choices=["bootstrap", "init", "plan", "apply", "destroy", "output"]
    )
    parser.add_argument("layer", nargs="?", choices=sorted(LAYER_DIRS))
    parser.add_argument("--env", default=os.environ.get("IOT_ENV", "dev"))
    args = parser.parse_args(argv)

    cfg = load_config(REPO_ROOT / "config" / "envs" / f"{args.env}.yaml")

    if args.command == "bootstrap":
        cmd_bootstrap(cfg, extra)
    else:
        if not args.layer:
            parser.error(f"'{args.command}' requires a layer")
        if args.command == "init":
            cmd_init(cfg, args.layer, extra)
        else:
            cmd_passthrough(cfg, args.layer, args.command, extra)


if __name__ == "__main__":
    main()
