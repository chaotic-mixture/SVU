"""Build a research snapshot directly from the database and configuration."""
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import yaml
ROOT = Path(__file__).resolve().parents[1]


def main(output_dir=None, db_path=None):
    cfg = yaml.safe_load((ROOT / "config/svu_v03_candidate.yaml").read_text(encoding="utf-8"))
    tables = {"datasets": "research_datasets", "source_assets": "research_asset_entity_map",
              "economic_entities": "research_entity_registry",
              "unique_price_observations": "research_price_observations_unique",
              "unique_macro_observations": "research_macro_observations_unique"}
    with closing(sqlite3.connect(Path(db_path or ROOT / "svu.db").resolve().as_uri() + "?mode=ro", uri=True)) as con:
        counts = {key: con.execute(f"select count(*) from {table}").fetchone()[0] for key, table in tables.items()}
        latest = con.execute("select max(observed_at) from research_price_observations_unique").fetchone()[0]
    snap = {"snapshot": "SVU-v0.3-research", "status": "diagnostic_only",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "latest_observation": latest, "config": cfg, "database": counts,
            "validation_status": "not_inferred_from_database_counts",
            "claims_forbidden": ["production_index", "macro_forecast", "welfare_judgment", "causal_effect"]}
    reports = Path(output_dir) if output_dir is not None else ROOT / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "svu_v03_snapshot.json").write_text(json.dumps(snap, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    lines = ["# SVU research snapshot", "", "Diagnostic only. SVU = 100; ICATI is the dynamic trend.", "",
             f"Generated: {snap['generated_at_utc']}", f"Latest observation: {latest}", "",
             *[f"- {key}: {value}" for key, value in counts.items()], "",
             "Counts do not establish data freshness, validation success or economic validity."]
    (reports / "svu_v03_snapshot.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return snap


if __name__ == "__main__":
    main()
