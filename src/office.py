"""
office.py — OfficeConfig abstraction for multi-office forecasting.

The original model targeted only the Ohio State House (99 districts). To extend
to State Senate, US House, and Governor, every module that previously hard-
coded "99 districts," "state house," or output paths like `oh_house_*` needs to
operate generically against a configuration object.

OfficeConfig is that object. The OFFICES registry is the single source of
truth — modify here, and composite/classify/simulate/export pick it up.

Design notes
------------
- `governor` is a degenerate single-district "office." Most legislative-chamber
  fields are None for it; modules that don't apply (e.g. seat counts) check
  `is_statewide` and short-circuit.
- `seats_up_2026` differs from `district_range` only for the State Senate,
  whose 4-year staggered terms put just the odd-numbered districts on the
  2026 ballot. Composite lean is computed for all 33 districts (chamber
  context); seat math and pickup ladders use `seats_up_2026` only.
- District-shapefile column is `DISTRICT` for all three legislative shapefiles
  (verified by inspection). District values come through as strings and need
  int conversion before joining to project-internal int district keys.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SHAPEFILES = REPO_ROOT / "data" / "shapefiles"
PROCESSED = REPO_ROOT / "data" / "processed"


@dataclass(frozen=True)
class OfficeConfig:
    """Configuration for one office that the model forecasts."""

    # Identity
    name: str                              # canonical key: "house" | "senate" | "congress" | "governor"
    display_name: str                      # human label for output
    output_prefix: str                     # filename prefix, e.g. "oh_house"

    # Districts
    n_districts: int                       # 99, 33, 15, or 1
    district_range: tuple[int, ...]        # full set of district numbers
    seats_up_2026: tuple[int, ...]         # subset on the 2026 ballot

    # Geometry (None for governor / statewide-only)
    shapefile_path: Path | None
    shapefile_district_col: str | None

    # Block backbone
    block_district_map_path: Path | None
    map_plan_label: str                    # backbone uses this to name parquets

    # SOS ingestion
    sos_race_attr: str                     # field on SosFile, e.g. "house"
    statewide_race_code: str | None        # set only for governor: "gov"

    # Chamber rules (None for governor)
    is_statewide: bool
    chamber_size: int | None
    majority_threshold: int | None
    current_d_seats: int | None


# ---------------------------------------------------------------------------
# Concrete configs
# ---------------------------------------------------------------------------

HOUSE = OfficeConfig(
    name="house",
    display_name="Ohio State House",
    output_prefix="oh_house",
    n_districts=99,
    district_range=tuple(range(1, 100)),
    seats_up_2026=tuple(range(1, 100)),  # all 99 up every 2 years
    shapefile_path=SHAPEFILES / "Corrected Sept 29 2023 Unified Bipartisan Redistricting Plan HD SHP.shp",
    shapefile_district_col="DISTRICT",
    block_district_map_path=PROCESSED / "block_district_map_house_2024.parquet",
    map_plan_label="house_2024",
    sos_race_attr="house",
    statewide_race_code=None,
    is_statewide=False,
    chamber_size=99,
    majority_threshold=50,
    current_d_seats=34,  # verified in reports/session3/oh_house_targeting.csv
)

SENATE = OfficeConfig(
    name="senate",
    display_name="Ohio State Senate",
    output_prefix="oh_senate",
    n_districts=33,
    district_range=tuple(range(1, 34)),
    # 4-year staggered terms: odd-numbered districts up in 2022/2026/2030,
    # even-numbered up in 2024/2028/2032. So 17 odd seats are on 2026 ballot.
    seats_up_2026=tuple(range(1, 34, 2)),
    shapefile_path=SHAPEFILES / "sldu_2026" / "Corrected Sept 29 2023 Unified Bipartisan Redistricting Plan SD SHP.shp",
    shapefile_district_col="DISTRICT",
    block_district_map_path=PROCESSED / "block_district_map_senate_2024.parquet",  # Sept 2023 plan, in effect 2024+2026
    map_plan_label="senate_2024",
    sos_race_attr="senate",
    statewide_race_code=None,
    is_statewide=False,
    chamber_size=33,
    majority_threshold=17,
    current_d_seats=9,  # post-2024 (24R-9D)
)

CONGRESS = OfficeConfig(
    name="congress",
    display_name="Ohio US House",
    output_prefix="oh_congress",
    n_districts=15,
    district_range=tuple(range(1, 16)),
    seats_up_2026=tuple(range(1, 16)),  # all 15 up every 2 years
    shapefile_path=SHAPEFILES / "cd_2026" / "Districts_2025_11_03_SHP.shp",
    shapefile_district_col="DISTRICT",
    block_district_map_path=PROCESSED / "block_district_map_congress_2026.parquet",  # NEW map adopted Oct 31, 2025
    map_plan_label="congress_2026",
    sos_race_attr="congressional",
    statewide_race_code=None,
    is_statewide=False,
    chamber_size=15,
    majority_threshold=None,  # federal majority lives at the national level; n/a for Ohio delegation
    current_d_seats=5,  # post-2024 (Kaptur OH-9, Beatty OH-3, Brown OH-11, Sykes OH-13, Landsman OH-1)
)

GOVERNOR = OfficeConfig(
    name="governor",
    display_name="Ohio Governor",
    output_prefix="oh_governor",
    n_districts=1,
    district_range=(0,),               # 0 = the placeholder "statewide" district
    seats_up_2026=(0,),
    shapefile_path=None,
    shapefile_district_col=None,
    block_district_map_path=None,
    map_plan_label="statewide",
    sos_race_attr="statewide",
    statewide_race_code="gov",
    is_statewide=True,
    chamber_size=None,
    majority_threshold=None,
    current_d_seats=None,
)


# ---------------------------------------------------------------------------
# Registry + helpers
# ---------------------------------------------------------------------------

OFFICES: dict[str, OfficeConfig] = {
    "house": HOUSE,
    "senate": SENATE,
    "congress": CONGRESS,
    "governor": GOVERNOR,
}


def get_office(name: str) -> OfficeConfig:
    """Look up an office by canonical name (case-insensitive)."""
    key = name.lower()
    if key not in OFFICES:
        raise KeyError(f"Unknown office '{name}'. Valid: {sorted(OFFICES)}")
    return OFFICES[key]


def all_offices() -> list[OfficeConfig]:
    """Return all configured offices in canonical order."""
    return [HOUSE, SENATE, CONGRESS, GOVERNOR]
