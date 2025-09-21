# ------------------------------
# Module: constants.py
# Description: Constants for the services
# ------------------------------

# :::::: Provider Related :::::: #

DB_PROVIDER = "snowflake"
EMBED_PROVIDER = "openai"

# :::::: FILE PATHS Related :::::: #

# The folder path for the MAR files
MAR_FILES_FOLDER_PATH_STR = "storage/snapshots/mar"

# The folder path for the PR files
PR_FILES_FOLDER_PATH_STR = "storage/raw_files/pr_files"

# :::::: MAR Parsing Related :::::: #

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

# The file names for the MAR files to be ingested into the database
MAR_FILE_NAMES_FOR_DB = ['mar_combined_m.parquet']

# :::::: PR Parsing Related :::::: #

PR_URL_DOMAIN = "https://www.tradeweb.com/"

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

# The prefix for the press release md file naming
PR_FILE_PREFIX = "tradeweb_reports"

# Used for broad matching lines, remove all the sections include and above these lines 
PR_M_MD_HEAD_RM_PATTERNS = [
    '| tradeweb',
    '| tradeweb markets',
]

# Used for broad matching lines, remove all the sections include and below these lines 
PR_M_MD_TAIL_RM_PATTERNS = [
    'for the complete report go to https://',
    'please refer to the report posted to https://',
    'to access the complete report containing additional data points and commentary, go to https://',
    'click here to read the full news release.'
]

# Used for exact matching lines, remove all the sections include and below these lines 
PR_M_MD_TAIL_SECTION_RM_PATTERNS = [
    'about tradeweb markets',
    'media contact',
    'investor contact',
    'basis of presentation',
    'market and industry data',
    'forward-looking statements',
]

# Used for exact matching lines, remove them
PR_M_MD_LINES_TO_RM = ['twitter', 'linkedin', 'print', 'email']

# The patterns of line separating summary section and content section
PR_M_MD_CONTENT_SEPARATE_PATTERN = [' highlights_**', ' highlights**', ' highlights*', ' highlights_', ' highlights***', 
                                    ' highlight_**', ' highlight**', ' highlight*', ' highlight_', ' highlight***']

PR_M_ASSET_CLASS = ['rates', 'credit', 'equities', 'money markets']


# :::::: General Embedding Related :::::: #

CHUNK_TOKEN_MAX_FOR_EMBEDDING = 512             # Please making this no less than 200. To maintain the best context preservation, we avoid cut sentence. Usually, a sentence can't be 300 tokens longer.

DEFAULT_EMBEDDING_MODEL = 'text-embedding-3-large'

DEFAULT_TAG_CONTENT_ALLOWED_TOKEN = 30

DEFAULT_LINE_SPLIT_BY = 3

DEFAULT_LINE_SPLIT_OVERLAP = 10                 # Num of tokens allowed to overlap between the splitted lines

DEFAULT_CHUNK_OVERLAP_LINES = 1                 # Num of lines allowed to overlap between the chunks


# :::::: PR Embedding Related :::::: #

PR_DEFAULT_TOKEN_MAX_FOR_EMBEDDING = 512         # Please making this no less than 200. To maintain the best context preservation, we avoid cut sentence. Usually, a sentence can't be 300 tokens longer.

PR_DEFAULT_EMBEDDING_MODEL = 'text-embedding-3-large'

PR_DEFAULT_TAG_CONTENT_ALLOWED_TOKEN = 30

PR_DEFAULT_LINE_SPLIT_BY = 3

PR_DEFAULT_LINE_SPLIT_OVERLAP = 0               # Num of tokens allowed to overlap between the splitted lines

PR_DEFAULT_CHUNK_OVERLAP_LINES = 1              # Num of lines allowed to overlap between the chunks