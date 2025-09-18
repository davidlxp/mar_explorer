# ------------------------------
# Project entry point
# Run: streamlit run main.py
# ------------------------------

import os
import sys

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Import your Streamlit app code
from app.app import *