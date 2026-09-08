"""Metadata-only NIM inventories, written before full-text retrieval."""
from pathlib import Path
import json
import pandas as pd
from .surface import EU_ISO3_TO_NAME


def write_nim_overview(acts: pd.DataFrame, measures: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = measures.copy()
    for col in ("celex", "national_measure_id", "nim_celex", "member_state_iso3", "nim_date"):
        if col not in rows:
            rows[col] = ""
        rows[col] = rows[col].fillna("").astype(str).str.strip()
    rows["member_state_iso3"] = rows["member_state_iso3"].replace("", "unknown")
    rows["measure_key"] = rows["national_measure_id"].where(rows["national_measure_id"].ne(""), rows["nim_celex"])
    # Retain unidentified records separately instead of collapsing all blanks.
    missing = rows["measure_key"].eq("")
    rows.loc[missing, "measure_key"] = [f"unidentified-{i}" for i in range(int(missing.sum()))]
    rows = rows.drop_duplicates(["celex", "member_state_iso3", "measure_key"])
    dates = pd.to_datetime(rows["nim_date"], errors="coerce", utc=True)
    rows["nim_year"] = dates.dt.year.astype("Int64").astype("string").fillna("unknown")
    statuses = {r["celex"]: r for r in measures.attrs.get("discovery_statuses", [])}
    countries = sorted((set(EU_ISO3_TO_NAME) - {"GBR"}) | set(rows["member_state_iso3"]))
    count_rows, act_rows = [], []
    for celex in sorted(set(acts.get("celex", pd.Series(dtype=str)).astype(str)) | set(rows["celex"])):
        subset = rows.loc[rows["celex"].eq(celex)]
        status = statuses.get(celex, {"discovery_status": "complete", "discovery_error": ""})
        failed = status["discovery_status"] == "failed"
        act_rows.append({"celex": celex, "nim_count": None if failed else len(subset), **status})
        for country in countries:
            count_rows.append({"celex": celex, "member_state_iso3": country,
                "member_state_name": EU_ISO3_TO_NAME.get(country, country),
                "nim_count": None if failed else int(subset["member_state_iso3"].eq(country).sum()),
                "discovery_status": status["discovery_status"]})
    yearly = rows.groupby(["celex", "member_state_iso3", "nim_year"], dropna=False).size().reset_index(name="nim_count")
    yearly["member_state_name"] = yearly["member_state_iso3"].map(EU_ISO3_TO_NAME).fillna("unknown")
    tables = {
        "nim_inventory.csv": rows,
        "nim_by_act.csv": pd.DataFrame(act_rows, columns=["celex", "nim_count", "discovery_status", "discovery_error"]),
        "nim_by_act_country.csv": pd.DataFrame(count_rows, columns=["celex", "member_state_iso3", "member_state_name", "nim_count", "discovery_status"]),
        "nim_by_act_country_year.csv": yearly,
    }
    for name, frame in tables.items():
        temporary = output_dir / (name + ".tmp")
        frame.to_csv(temporary, index=False, encoding="utf-8-sig")
        temporary.replace(output_dir / name)
    (output_dir / "overview.json").write_text(json.dumps({
        "stage": "before_fulltext", "count_unit": "distinct national measure within each act and country",
        "year_basis": "NIM document date (nim_date), not EU act year or notification year",
        "missing_year": "unknown", "fulltext_limits_applied": False,
        "zero_counts": "Zero only after successful discovery; failed discovery has blank counts.",
        "yearly_rows": "Observed country/year combinations only; zero combinations omitted.",
        "files": list(tables),
    }, indent=2), encoding="utf-8")
    print(f"[NIM OVERVIEW] Saved {len(rows)} measures for {len(act_rows)} acts -> {output_dir.resolve()}", flush=True)
