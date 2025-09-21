# ------------------------------
# Project entry point
# Run: streamlit run main.py
# ------------------------------

# import os
# import sys

# # Add the project root to Python path
# project_root = os.path.dirname(os.path.abspath(__file__))
# sys.path.insert(0, project_root)

# # Import your Streamlit app code
# from app.app import *



from services import task_handle_mar
from services.db import Database
from services import task_handle_pr
import asyncio
import services.crawler as crawler
import tests.test_runs as test_runs

if __name__ == "__main__":

    # # Create all tables when starting the app
    # db = Database()
    # db._run_migrations()

    # test_runs.test_run_mar_update()
    # test_runs.test_run_print_all_tables()
    # test_runs.test_run_query1()
    # test_runs.test_run_query_pr_index()
    # test_runs.test_run_ALL()
    # test_runs.test_run_crawler_one()
    test_runs.test_run_fetch_press_release()
    # test_runs.test_run_fetch_many_press_releases()




