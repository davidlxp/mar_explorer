# ------------------------------
# Module: constants.py
# Description: Constants for the services
# ------------------------------

# The folder path for the MAR files
MAR_FILES_FOLDER_PATH_STR = "storage/snapshots/mar"

# Supported MAR tabs to parse
MAR_SHEETS_TO_FILE_MAPPINGS = {
    'ADV - M': 'mar_adv_m.parquet',
    'Volume - M': 'mar_volume_m.parquet',
}

# MAR file to table mappings
MAR_FILE_TO_TABLE_MAPPINGS = {
    'mar_adv_m.parquet': 'mar_adv_m',
    'mar_volume_m.parquet': 'mar_volume_m',
}

PR_URL_DOMAIN = "https://www.tradeweb.com/"

EXCLUDE_HEADERS = [
    "Media Contacts",
    "Market and Industry Data",
    "Forward-Looking Statements",
]

# Quarter mapping
PR_QUARTER_MAPPING = {
    "first": "q1",
    "second": "q2",
    "third": "q3",
    "fourth": "q4"
}

### URL-last-part patterns for monthly, quarterly and yearly press releases. Easy to update.
# Monthly URL-last-part looks like "tradeweb-reports-august-2025-..."
# Quarterly URL-last-part looks like "tradeweb-reports-first-quarter-2025-..."
# Yearly URL-last-part looks like "tradeweb-reports-fourth-quarter-and-full-year-2025-..."
MONTHLY_PR_PATTERN = r"tradeweb-reports-(january|february|march|april|may|june|july|august|september|october|november|december)-\d{4}(?!.*quarter)(?!.*year)"
QUARTERLY_PR_PATTERN = r"tradeweb-reports-(first|second|third|fourth)-quarter-\d{4}(?!.*full-year)"
YEARLY_PR_PATTERN = r"tradeweb-reports-(first|second|third|fourth)-quarter-and-full-year-\d{4}"