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

db = Database()

if __name__ == "__main__":



    task_handle_pr.fetch_press_release()


    # the_file = "storage/raw_files/mar_files/tw-historical-adv-and-day-count-through-august-2025.xlsx"
    # task_handle_mar.handle_mar_update(the_file)

    

    # test_query2 = """
    #     SELECT count(*)
    #     FROM mar_adv_m 
    # """

    # # Create a simple test query
    # df = db.fetchdf(test_query2)
    # print(df)
