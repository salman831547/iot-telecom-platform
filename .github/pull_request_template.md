## What & why

<!-- One or two sentences. Link the issue if there is one. -->

## Type
- [ ] Infra (Terraform)
- [ ] Pipeline (DAG / SQL / library)
- [ ] CI/CD
- [ ] Config only
- [ ] Docs

## Checklist
- [ ] `uv run pre-commit run --all-files` passes
- [ ] `uv run pytest` passes
- [ ] Terraform plan reviewed (CI comment) — no unexpected destroys
- [ ] New config keys added to `config/envs/*.yaml` **and** the schema in `src/iot_pipeline/config.py`
- [ ] DAG/SQL changes are idempotent (safe to re-run the same interval)
- [ ] Commands used are recorded in `docs/COMMANDS.md`
