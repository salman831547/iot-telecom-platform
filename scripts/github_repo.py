"""GitHub repository settings as code, derived from config/envs/<env>.yaml.

Idempotent: every command uses PUT, or upserts by name, so re-running it is safe.

Usage:
    uv run python scripts/github_repo.py vars         # bootstrap outputs -> repo variables
    uv run python scripts/github_repo.py environment  # gated deploy environment
    uv run python scripts/github_repo.py ruleset      # protect the deploy branch
    uv run python scripts/github_repo.py all [--env dev]

Requires: `gh auth login` by a repo admin, and the bootstrap layer applied.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from iot_pipeline.config import EnvConfig, load_config  # noqa: E402

RULESET_NAME = "protect-deploy-branch"
# Status checks become required once the CI workflow exists (Phase F); listed in config later.
REQUIRED_STATUS_CHECKS: list[str] = []


def _gh() -> str:
    return shutil.which("gh") or sys.exit("gh CLI not found on PATH (run `gh auth login`)")


def gh_api(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    args = [_gh(), "api", "--method", method, path, "-H", "Accept: application/vnd.github+json"]
    if body is not None:
        args += ["--input", "-"]
    result = subprocess.run(  # noqa: S603
        args,
        input=json.dumps(body) if body is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        sys.exit(f"gh api {method} {path} failed:\n{result.stderr or result.stdout}")
    return json.loads(result.stdout) if result.stdout.strip() else None


def cmd_vars(cfg: EnvConfig) -> None:
    """Publish non-secret bootstrap outputs as repository variables."""
    tf = shutil.which("terraform") or sys.exit("terraform not found on PATH")
    out = subprocess.run(  # noqa: S603
        [tf, "output", "-json", "github_variables"],
        cwd=REPO_ROOT / "infra" / "terraform" / "00-bootstrap",
        env={**os.environ, "TF_VAR_env": cfg.env},
        capture_output=True,
        text=True,
        check=True,
    )
    variables: dict[str, str] = json.loads(out.stdout)
    variables["IOT_ENV"] = cfg.env
    for name, value in sorted(variables.items()):
        print(f"  var {name}")
        subprocess.run(  # noqa: S603
            [_gh(), "variable", "set", name, "--body", value, "--repo", cfg.github.full_name],
            check=True,
        )


def cmd_environment(cfg: EnvConfig) -> None:
    """Deployment environment: owner approval + only the deploy branch may deploy."""
    repo, env_name = cfg.github.full_name, cfg.github.environment
    gh_api(
        "PUT",
        f"/repos/{repo}/environments/{env_name}",
        {
            "wait_timer": 0,
            # Solo maintainer must be able to approve their own deployment.
            "prevent_self_review": False,
            "reviewers": [{"type": "User", "id": int(cfg.github.owner_id)}],
            "deployment_branch_policy": {
                "protected_branches": False,
                "custom_branch_policies": True,
            },
        },
    )
    policies = gh_api("GET", f"/repos/{repo}/environments/{env_name}/deployment-branch-policies")
    existing = {p["name"] for p in policies.get("branch_policies", [])}
    if cfg.github.deploy_branch not in existing:
        gh_api(
            "POST",
            f"/repos/{repo}/environments/{env_name}/deployment-branch-policies",
            {"name": cfg.github.deploy_branch, "type": "branch"},
        )
    print(f"  environment '{env_name}': reviewer={cfg.github.owner}")
    print(f"  deployable branch: {cfg.github.deploy_branch}")


def cmd_ruleset(cfg: EnvConfig) -> None:
    """Branch ruleset: PRs only, linear history, no force-push or deletion."""
    repo = cfg.github.full_name
    rules: list[dict[str, Any]] = [
        {"type": "deletion"},
        {"type": "non_fast_forward"},
        {"type": "required_linear_history"},
        {
            "type": "pull_request",
            "parameters": {
                # Solo repo: GitHub forbids approving your own PR, so 0 approvals; the PR
                # itself (with CI checks + CODEOWNERS visibility) is still mandatory.
                "required_approving_review_count": 0,
                "dismiss_stale_reviews_on_push": True,
                "require_code_owner_review": False,
                "require_last_push_approval": False,
                "required_review_thread_resolution": True,
            },
        },
    ]
    if REQUIRED_STATUS_CHECKS:
        rules.append(
            {
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": True,
                    "required_status_checks": [{"context": c} for c in REQUIRED_STATUS_CHECKS],
                },
            }
        )
    body = {
        "name": RULESET_NAME,
        "target": "branch",
        "enforcement": "active",
        "conditions": {
            "ref_name": {"include": [f"refs/heads/{cfg.github.deploy_branch}"], "exclude": []}
        },
        "rules": rules,
    }
    existing = gh_api("GET", f"/repos/{repo}/rulesets") or []
    match = next((r for r in existing if r["name"] == RULESET_NAME), None)
    if match:
        gh_api("PUT", f"/repos/{repo}/rulesets/{match['id']}", body)
        print(f"  ruleset '{RULESET_NAME}' updated (id={match['id']})")
    else:
        created = gh_api("POST", f"/repos/{repo}/rulesets", body)
        print(f"  ruleset '{RULESET_NAME}' created (id={created['id']})")


def main() -> None:
    parser = argparse.ArgumentParser(prog="scripts/github_repo.py")
    parser.add_argument("command", choices=["vars", "environment", "ruleset", "all"])
    parser.add_argument("--env", default=os.environ.get("IOT_ENV", "dev"))
    args = parser.parse_args()
    cfg = load_config(REPO_ROOT / "config" / "envs" / f"{args.env}.yaml")

    steps = {"vars": cmd_vars, "environment": cmd_environment, "ruleset": cmd_ruleset}
    for name, fn in steps.items():
        if args.command in (name, "all"):
            print(f"==> {name} ({cfg.github.full_name})")
            fn(cfg)


if __name__ == "__main__":
    main()
