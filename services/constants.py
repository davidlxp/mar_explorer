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