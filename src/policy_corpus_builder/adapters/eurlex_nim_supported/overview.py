"""Metadata-only NIM inventories, written before full-text retrieval."""
from pathlib import Path
from datetime import date
import json
import pandas as pd
from .surface import EU_ISO3_TO_NAME


TIMING_COLUMNS = ["first_nim_date", "last_nim_date", "implementation_update_span_days"]


def validate_min_year(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 9999:
        raise ValueError("nim_min_valid_year must be an integer between 1 and 9999.")
    return value


def _timing(values, minimum):
    valid = []
    for value in values:
        try:
            parsed = date.fromisoformat(str(value)[:10])
        except ValueError:
            continue
        if minimum <= parsed.year <= date.today().year:
            valid.append(parsed)
    if not valid:
        return dict.fromkeys(TIMING_COLUMNS, None)
    first, last = min(valid), max(valid)
    return dict(zip(TIMING_COLUMNS, (first.isoformat(), last.isoformat(), (last-first).days)))


def write_nim_overview(acts: pd.DataFrame, measures: pd.DataFrame, output_dir: Path,
                       *, min_valid_year: int = 1950) -> None:
    validate_min_year(min_valid_year)
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
    seeds = sorted(set(acts.get("celex", pd.Series(dtype=str)).astype(str)) | set(rows["celex"]))
    for celex in seeds:
        subset = rows.loc[rows["celex"].eq(celex)]
        status = statuses.get(celex, {"discovery_status": "complete", "discovery_error": ""})
        failed = status["discovery_status"] == "failed"
        act_rows.append({"celex": celex, "nim_count": None if failed else len(subset), **status})
        for country in countries:
            count_rows.append({"celex": celex, "member_state_iso3": country,
                "member_state_name": EU_ISO3_TO_NAME.get(country, country),
                "nim_count": None if failed else int(subset["member_state_iso3"].eq(country).sum()),
                "discovery_status": status["discovery_status"],
                **(_timing([], min_valid_year) if failed else
                   _timing(subset.loc[subset["member_state_iso3"].eq(country), "nim_date"], min_valid_year))})
    yearly = rows.groupby(["celex", "member_state_iso3", "nim_year"], dropna=False).size().reset_index(name="nim_count")
    yearly["member_state_name"] = yearly["member_state_iso3"].map(EU_ISO3_TO_NAME).fillna("unknown")
    any_failed = any(row["discovery_status"] == "failed" for row in act_rows)
    country_rows, wide_rows = [], []
    cells = {(row["member_state_iso3"], row["celex"]): row["nim_count"] for row in count_rows}
    for country in countries:
        subset = rows.loc[rows["member_state_iso3"].eq(country)]
        identity = {"member_state_iso3": country, "member_state_name": EU_ISO3_TO_NAME.get(country, country)}
        country_rows.append({**identity, "nim_count": None if any_failed else len(subset),
                             "act_count": None if any_failed else subset["celex"].nunique(),
                             **(_timing([], min_valid_year) if any_failed else _timing(subset["nim_date"], min_valid_year))})
        wide_rows.append({**identity, **{celex: cells[country, celex] for celex in seeds},
                          "TOTAL": None if any_failed else sum(cells[country, celex] for celex in seeds)})
    excluded = sum(bool(value) and _timing([value], min_valid_year)["first_nim_date"] is None for value in rows["nim_date"])
    tables = {
        "nim_inventory.csv": rows,
        "nim_by_act.csv": pd.DataFrame(act_rows, columns=["celex", "nim_count", "discovery_status", "discovery_error"]),
        "nim_by_act_country.csv": pd.DataFrame(count_rows, columns=["celex", "member_state_iso3", "member_state_name", "nim_count", "discovery_status", *TIMING_COLUMNS]),
        "nim_by_act_country_year.csv": yearly,
        "nim_by_country.csv": pd.DataFrame(country_rows),
        "nim_country_x_act.csv": pd.DataFrame(wide_rows),
    }
    for name, frame in tables.items():
        temporary = output_dir / (name + ".tmp")
        frame.to_csv(temporary, index=False, encoding="utf-8-sig")
        temporary.replace(output_dir / name)
    temporary_overview = output_dir / "overview.json.tmp"
    temporary_overview.write_text(json.dumps({
        "schema_version": "1.1",
        "stage": "before_fulltext", "count_unit": "distinct national measure within each act and country",
        "year_basis": "NIM document date (nim_date), not EU act year or notification year",
        "missing_year": "unknown", "fulltext_limits_applied": False,
        "zero_counts": "Zero only after successful discovery; failed discovery has blank counts.",
        "yearly_rows": "Observed country/year combinations only; zero combinations omitted.",
        "country_count_unit": "Sum of distinct act/country/measure combinations; the same measure implementing two acts counts twice.",
        "act_count_unit": "Distinct seed acts with at least one measure in the country.",
        "total_behavior": "Country counts, act counts, timing and wide TOTAL are blank if any seed discovery failed; successful act cells remain available. Page-limited counts are observed lower bounds.",
        "discovery_statuses": act_rows,
        "date_rule": {"minimum_valid_year": min_valid_year, "maximum_valid_year": date.today().year,
                      "scope": "first/last/span only; raw nim_date and counts are preserved", "excluded_date_count": int(excluded)},
        "warnings": ([f"Excluded {excluded} nonempty invalid or implausible dates from timing summaries."] if excluded else [])
                    + (["Some act discovery failed; country totals are unknown."] if any_failed else [])
                    + (["Page-limited discovery: counts and timing may be incomplete."] if any(r["discovery_status"] == "page_limited" for r in act_rows) else []),
        "files": list(tables),
    }, indent=2), encoding="utf-8")
    temporary_overview.replace(output_dir / "overview.json")
    print(f"[NIM OVERVIEW] Saved {len(rows)} measures for {len(act_rows)} acts -> {output_dir.resolve()}", flush=True)
