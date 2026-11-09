"""Versioned SQL templates in ``sql/<layer>/<name>.sql``.

Only *identifiers* (fully-qualified table names from validated config) are
substituted with ``${name}``. Every *value* is passed as a typed BigQuery query
parameter, so there's no string-built SQL and no injection surface.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from string import Template

COMPOSER_SQL_DIR = Path("/home/airflow/gcs/data/sql")
REPO_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

# project.dataset.table: letters, digits, underscore, hyphen (project ids), dots.
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+){0,2}$")


def sql_dir() -> Path:
    return COMPOSER_SQL_DIR if COMPOSER_SQL_DIR.is_dir() else REPO_SQL_DIR


class _SqlTemplate(Template):
    """Substitutes ONLY ``${identifier}``. Any other ``$``, such as regex anchors
    (``r'...$'``) or BigQuery ``$``-prefixed names, is left untouched. The default
    Template treats every ``$`` as a placeholder."""

    pattern = r"""
        \$\{(?P<braced>[a-z_][a-z0-9_]*)\}   # ${identifier}
        |(?P<escaped>(?!))                   # never matches: no $$ escape needed
        |(?P<named>(?!))                     # never matches: bare $name is not a placeholder
        |(?P<invalid>(?!))                   # never matches: stray $ is literal SQL
    """  # type: ignore[assignment]


@lru_cache(maxsize=32)
def _template(path: Path) -> Template:
    return _SqlTemplate(path.read_text(encoding="utf-8"))


def render(name: str, base: Path | None = None, /, **identifiers: str) -> str:
    """Render ``sql/<name>.sql``. Fails if a placeholder is missing or an identifier
    looks like anything other than a table reference."""
    for key, value in identifiers.items():
        if not _IDENTIFIER.match(value):
            raise ValueError(f"unsafe SQL identifier for {key!r}: {value!r}")
    path = (base or sql_dir()) / f"{name}.sql"
    return _template(path).substitute(identifiers)
