# One place for names and settings, so nothing is hard-coded in the notebooks

# Unity Catalog: catalog.schema where all tables live
SCHEMA = "workspace.betting"

# The volume where the source system drops its files (like an ADLS Gen2 container)
LANDING = "/Volumes/workspace/betting/landing"

# Business date of each batch. Used as valid_from for the SCD2 customer history.
BATCH_DATES = {
    "day1": "2026-10-26",
    "day2": "2026-10-27",
}

# The run fails if more than this share of bets is rejected
MAX_REJECT_RATE = 0.10
