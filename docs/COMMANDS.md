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

Ruff hook id renamed `ruff` → `ruff-check` (the old name is a legacy alias in ruff-pre-commit ≥ 0.13).

Result: config valid, 9/9 tests pass (91% coverage), ruff and mypy (strict) clean. All hooks pass except
`no-commit-to-branch`, which is expected while still on `main`.

### A.4 Initial commit 🛠️

The repository's root commit is the only commit made directly to `main`, so it's the only time the
branch guard is skipped. All later work uses feature branches and PRs.

```powershell
uv run pre-commit install                 # hooks now run on every commit
git add -A
# Multi-line message from a file (PowerShell can't pipe a here-string to `git commit -F -`)
$env:SKIP = "no-commit-to-branch"; git commit -F <path-to>/commit-msg.txt; Remove-Item Env:SKIP
git log --stat --oneline
```

Result: root commit `0814aa5`, 16 files.

---

## Step 1 · Phase B: Terraform bootstrap (remote state, APIs, WIF, CI identities)

> Raw `terraform` calls below run in **bash** (Git Bash). PowerShell splits arguments such as
> `-out=x.tfplan`. In steady state, always use `scripts/tf.py`, which works in any shell.

### B.1 Branch and tooling 🛠️

```powershell
git checkout -b feat/phase-b-bootstrap
winget install --id GitHub.cli -e --source winget --accept-source-agreements --accept-package-agreements --silent
gh --version                                                    # 2.103.3
```

### B.2 Look up inputs 🔍

```powershell
# Immutable GitHub owner id -> config github.owner_id (used in the WIF attribute condition)
(Invoke-RestMethod https://api.github.com/users/salman831547) | Select-Object login,id
# Latest google provider -> versions.tf constraint "~> 8.10"
(Invoke-RestMethod https://registry.terraform.io/v1/providers/hashicorp/google/versions).versions.version
# Terraform authenticates with Application Default Credentials (your user, owner role)
gcloud auth application-default print-access-token > $null; $LASTEXITCODE    # 0 = OK
```

### B.3 Static checks 🔍

```bash
uv run pytest -q && uv run ruff check . && uv run ruff format .
terraform -chdir=infra/terraform/00-bootstrap fmt -check -recursive
terraform -chdir=infra/terraform/00-bootstrap init -backend=false -input=false
terraform -chdir=infra/terraform/00-bootstrap validate
```

### B.4 First apply (local state), reviewed plan first ☁️

The bootstrap layer creates the bucket that stores its own state, so the first run uses a
temporary git-ignored `backend_override.tf` that switches to local state.
`uv run python scripts/tf.py bootstrap` automates B.4 and B.5. The steps below are the manual
equivalent, used here so the plan could be reviewed before applying.

```bash
cd infra/terraform/00-bootstrap
export TF_VAR_env=dev
printf 'terraform {\n  backend "local" {}\n}\n' > backend_override.tf
terraform init -reconfigure -input=false
terraform plan -input=false -out=bootstrap.tfplan              # Plan: 40 to add, 0 change, 0 destroy
terraform apply -input=false bootstrap.tfplan                  # Apply complete! 40 added
```

### B.5 Migrate state to GCS ☁️

```bash
rm -f backend_override.tf bootstrap.tfplan
terraform init -migrate-state -force-copy -input=false \
  -backend-config=bucket=project-2026-ai-iot-tfstate -backend-config=prefix=bootstrap
rm -f terraform.tfstate terraform.tfstate.backup               # no local state left behind
gcloud storage ls -r gs://project-2026-ai-iot-tfstate/         # bootstrap/default.tfstate
terraform state list | wc -l                                   # 40
```

### B.6 Prove idempotency through the wrapper 🔍

```bash
uv run python scripts/tf.py plan bootstrap -- -detailed-exitcode   # "No changes", exit 0
```
