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

### B.7 Lock provider hashes for every platform 🛠️

`init` on Windows only records Windows hashes, and Linux CI would then fail to verify the provider.

```bash
terraform -chdir=infra/terraform/00-bootstrap providers lock \
  -platform=linux_amd64 -platform=windows_amd64 -platform=darwin_arm64 -platform=darwin_amd64
```

### B.8 GitHub login (repo owner, interactive) 🛠️

```powershell
# A terminal opened before the winget install needs PATH reloaded (or open a new terminal):
$env:Path = [Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [Environment]::GetEnvironmentVariable('Path','User')
gh auth login --hostname github.com --git-protocol https --web
gh auth status
```

### B.9 Pre-publish privacy check, commit email → GitHub no-reply 🛠️

The repo is **public**, so history is scanned before the first push, and unpushed commits are
re-authored with the no-reply address so the personal email is never published.

```bash
git grep -nIE 'gmail|billingAccount|gho_|AIza|BEGIN .*PRIVATE' $(git rev-list --all) -- .   # no hits
uv run pre-commit run gitleaks --all-files                                                 # Passed
git config user.email "48931885+salman831547@users.noreply.github.com"                     # repo-local
git stash
git rebase --root --exec 'git commit --amend --no-edit --reset-author -q' feat/phase-b-bootstrap
git branch -f main $(git rev-list --max-parents=0 HEAD)
git stash pop
git log --format='%h %ae %s' --all                                                         # verify
```

### B.10 Create the public repo and push ☁️

```powershell
gh repo create salman831547/iot-telecom-platform --public --description "<...>" --source . --remote origin
git push -u origin main
git push -u origin feat/phase-b-bootstrap
gh repo edit salman831547/iot-telecom-platform --delete-branch-on-merge --enable-squash-merge `
  --enable-merge-commit=false --enable-rebase-merge=false --enable-wiki=false
```

### B.11 Repo settings as code (variables, environment, ruleset) ☁️

```powershell
uv run python scripts/github_repo.py all        # idempotent; re-run any time config changes
gh variable list --repo salman831547/iot-telecom-platform
gh api repos/salman831547/iot-telecom-platform/environments/dev --jq '{name, rules: [.protection_rules[].type]}'
gh api repos/salman831547/iot-telecom-platform/rulesets --jq '.[] | {name, enforcement}'
```

Result:
- 8 repo variables are set, among them `GCP_WIF_PROVIDER` and the 3 service-account emails.
- Environment `dev` requires reviewers (the owner) and only allows deployments from `main`.
- Ruleset `protect-deploy-branch` (active) blocks deletion and force-push, and requires linear history and a PR.

### B.12 Open the Phase B pull request ☁️

```powershell
git add -A; git commit -F <path-to>/commit-msg.txt
git push
gh pr create --base main --head feat/phase-b-bootstrap --title "<...>" --body-file <path-to>/pr-body.md
```

---

## Step 1 · Phase C: Foundation layer (network, buckets, BigQuery, runtime IAM, secret)

### C.1 Merge Phase B and branch ☁️

```powershell
gh pr merge 1 --repo salman831547/iot-telecom-platform --squash --delete-branch
git checkout main; git pull --ff-only; git branch -D feat/phase-b-bootstrap
git checkout -b feat/phase-c-foundation
```

### C.2 Static checks 🔍

```bash
terraform -chdir=infra/terraform/10-foundation fmt
terraform -chdir=infra/terraform/10-foundation init -backend=false -input=false
TF_VAR_env=dev terraform -chdir=infra/terraform/10-foundation validate
uv run pytest -q          # 28 passed: config + table-definition contracts
uv run ruff check . && uv run ruff format . && uv run mypy
```

### C.3 Plan → review → apply ☁️

```bash
uv run python scripts/tf.py init foundation
uv run python scripts/tf.py plan foundation -- -out=foundation.tfplan   # Plan: 32 to add, 0 change, 0 destroy
# The PII salt is write-only, so the saved plan must not contain it:
TF_VAR_env=dev terraform -chdir=infra/terraform/10-foundation show -json foundation.tfplan \
  | grep -o '"secret_data_wo":[^,]*'                                        # "secret_data_wo":null
TF_VAR_env=dev terraform -chdir=infra/terraform/10-foundation apply foundation.tfplan   # 32 added
rm infra/terraform/10-foundation/foundation.tfplan
```

### C.4 Verify 🔍

```bash
uv run python scripts/tf.py plan foundation -- -detailed-exitcode     # No changes, exit 0
# Secret value must never be in state (expect 0):
TF_VAR_env=dev terraform -chdir=infra/terraform/10-foundation state pull | grep -c '"secret_data"\s*:\s*"[^"]'
gcloud secrets versions list iot-dev-pii-hash-salt                    # 1 enabled
uv run python scripts/verify_foundation.py                            # PASSED (buckets, datasets, tables)
```

> In Git Bash, the `bq` and `gcloud storage` wrappers fail with `python3.11: command not found`.
> `scripts/verify_foundation.py` uses the Python clients instead and works in every shell.

### C.5 Lock providers, commit, PR ☁️

```bash
terraform -chdir=infra/terraform/10-foundation providers lock \
  -platform=linux_amd64 -platform=windows_amd64 -platform=darwin_arm64 -platform=darwin_amd64
git add -A && git commit -F <path-to>/commit-msg.txt && git push -u origin feat/phase-c-foundation
gh pr create --base main --head feat/phase-c-foundation --title "<...>" --body-file <path-to>/pr-body.md
```

---

## Step 1 · Phase D: Library (raw contract, data generator, GCS/BigQuery helpers)

### D.1 Merge Phase C and branch ☁️

```powershell
gh pr merge 2 --repo salman831547/iot-telecom-platform --squash --delete-branch
git checkout main; git pull --ff-only; git branch -D feat/phase-c-foundation
git checkout -b feat/phase-d-library
```

### D.2 Inspect the raw contract 🔍

```powershell
uv run python -c "from iot_pipeline.contracts import LANDING_COLUMNS as L; print(len(L)); print(L)"   # 22 columns
```

### D.3 Quality gates 🔍

```bash
uv run ruff format . && uv run ruff check . --output-format concise
uv run mypy                       # strict; google-cloud-storage is untyped -> override in pyproject
uv run pytest -q                  # 64 passed, 94% coverage
```

Fixes made along the way:
- Python's built-in `hash()` is salted per process, which broke determinism, so a stable cell assignment from the device id is used instead.
- `from google.cloud import storage` became `import google.cloud.storage as storage` so mypy resolves it.

### D.4 Generate locally (no cloud) 🛠️

```bash
uv run python -m iot_pipeline.generator --start 2026-11-09T00:00:00Z --hours 3 --out ./output   # git-ignored
```

### D.5 Generate into the staging bucket and prove the re-run writes nothing ☁️

```bash
uv run python -m iot_pipeline.generator --start 2026-11-09T00:00:00Z --hours 3 --upload   # [uploaded] x3
uv run python -m iot_pipeline.generator --start 2026-11-09T00:00:00Z --hours 3 --upload   # [unchanged (idempotent skip)] x3
```

Result: 3 CSVs (735, 1,044 and 1,403 rows × 22 columns) under
`gs://project-2026-ai-iot-dev-staging/raw/iot_telemetry/dt=2026-11-09/hour=HH/`, plus one manifest per batch
under `manifests/iot_telemetry/...` with the row count, MD5 and counts of injected anomalies. Each object has a single
generation, because the re-run uploaded nothing.

### D.6 Commit and PR ☁️

```bash
git add -A && git commit -F <path-to>/commit-msg.txt && git push -u origin feat/phase-d-library
gh pr create --base main --head feat/phase-d-library --title "<...>" --body-file <path-to>/pr-body.md
```
