"""Preview or remove expired non-canonical MarketForge artifacts."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from observability.resource_guardrails import load_budget


SCOPES = (
    Path("data/raw/.tmp"), Path("data/staging"), Path("data/quarantine"),
    Path("benchmarks/artifacts"),
)


def candidates(root: Path, *, older_than: datetime) -> list[Path]:
    found = []
    for relative in SCOPES:
        scope = root / relative
        if not scope.is_dir() or scope.is_symlink():
            continue
        for path in scope.rglob("*"):
            if path.is_file() and not path.is_symlink():
                modified = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
                if modified < older_than:
                    found.append(path)
    return sorted(found)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--budget", type=Path, default=Path("config/resource_budget.yaml"))
    parser.add_argument("--older-than-days", type=int)
    parser.add_argument("--apply", action="store_true", help="Delete listed files; default is dry-run")
    args = parser.parse_args()
    root = args.root.resolve()
    days = args.older_than_days
    if days is None:
        days = int(load_budget(args.budget)["storage"]["cleanup_retention_days"])
    if days < 1:
        parser.error("retention must be at least one day")
    selected = candidates(root, older_than=datetime.now(timezone.utc) - timedelta(days=days))
    bytes_selected = sum(path.stat().st_size for path in selected)
    if args.apply:
        for path in selected:
            path.unlink()
        # Remove empty directories only inside the four explicitly managed scopes.
        for relative in SCOPES:
            scope = root / relative
            if scope.is_dir() and not scope.is_symlink():
                for directory in sorted(
                    (path for path in scope.rglob("*") if path.is_dir() and not path.is_symlink()),
                    key=lambda path: len(path.parts), reverse=True,
                ):
                    try:
                        directory.rmdir()
                    except OSError:
                        pass
    print(json.dumps({"mode": "apply" if args.apply else "dry_run", "retention_days": days,
                      "files": [str(path.relative_to(root)) for path in selected],
                      "file_count": len(selected), "bytes": bytes_selected}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
