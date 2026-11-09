# Command Log

Chronological record of every command run to build this platform, grouped by phase.
Shell: PowerShell on Windows unless noted. `<…>` marks values that come from config.

Conventions:
- 🔍 read-only / inspection · 🛠️ changes local files · ☁️ changes cloud resources
- Commands that CI runs later are noted. They are not run by hand in steady state.

---

## Step 1 · Phase A: Repository foundation

### A.1 Inspect workstation and GCP project 🔍

```powershell
# Which CLIs are installed?
foreach ($t in 'gcloud','terraform','gh','git','python','docker','uv','pre-commit','winget') {
  $c = Get-Command $t -ErrorAction SilentlyContinue
  if ($c) { "$t -> $($c.Source)" } else { "$t -> MISSING" }
}
gcloud config list --format=json          # active account + default project
terraform version
python --version
uv python list --only-installed

# Project facts: parent org (none), billing, enabled APIs
gcloud projects describe project-2026-ai --format=json
gcloud billing projects describe project-2026-ai --format=json
gcloud services list --enabled --format="value(config.name)" --project project-2026-ai
```

Findings: Terraform 1.17.0, gcloud, git and uv are present. `gh` and Docker are missing, and Docker isn't needed in Step 1. The project has billing and no Organization.

### A.2 Initialise git 🛠️

```powershell
git init -b main
git config core.autocrlf false            # .gitattributes enforces LF; no silent CRLF conversion
git config user.name                      # verify commit identity
git config user.email
```

### A.3 Python toolchain (uv, Python 3.11 to match Composer) 🛠️

```powershell
uv python install 3.11
uv sync                                   # creates .venv from pyproject.toml + uv.lock
uv run python -m iot_pipeline.config validate config/envs/dev.yaml
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pre-commit autoupdate              # bump hook revs to latest tags
uv run pre-commit install
uv run pre-commit run --all-files
```

Result: config valid, 9/9 tests pass (91% coverage), ruff and mypy (strict) clean. All hooks pass except
`no-commit-to-branch`, which is expected while still on `main`.

### A.4 Initial commit 🛠️

The repository's root commit is the only commit made directly to `main`, so it's the only time the
branch guard is skipped. All later work uses feature branches and PRs.

```powershell
git add -A
$env:SKIP = "no-commit-to-branch"; git commit -m "chore: repository foundation (config schema, tooling, governance)"; Remove-Item Env:SKIP
git log --oneline
```
