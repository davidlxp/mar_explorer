# ------------------------------------------------------------------
# Project's Testing Entry Point
# Run: python main.py
# Note: Please run the Streamlit app using: streamlit run app/app.py
# ------------------------------------------------------------------

# from services import task_handle_mar
# from services.db import get_database
# from services import task_handle_pr
# import asyncio
import services.crawler as crawler
# import tests.test_runs as test_runs
# import services.utils as utils
# import os
# from services.constants import *
# from services.vectorstores import pinecone_store
import services.ai_workflow.mar_orchestrator as mar_orchestrator
# from services.ai_workflow.utils.common_utils import load_available_products, get_mar_table_schema, execute_sql_query, execute_vector_query
import services.task_handle_mar as task_handle_mar
import services.ai_workflow.utils.common_utils as common_utils
from services.db import get_database
import services.ai_workflow.agents.query_breaker as query_breaker
import services.ai_workflow.agents.task_planner as task_planner



db = get_database()

if __name__ == "__main__":

    # # Create all tables when starting the app
    # db = get_database()
    # db._run_migrations()

    # test_runs.test_run_mar_update()
    # test_runs.test_run_mar_update_with_latest_file()
    # test_runs.test_run_print_all_tables()
    # test_runs.test_run_query1()
    # test_runs.test_run_ALL()
    # test_runs.test_run_crawler_one()
    # test_runs.test_run_fetch_press_release()
    # test_runs.test_run_fetch_many_press_releases()
    # test_runs.test_run_parse_pr_m()

    # test_runs.test_run_ingest_all_pr_md_in_storage()
    # test_runs.test_run_ingest_one_pr_md_file()

    # test_runs.test_run_query_pr_index_count()

    # matches = pinecone_store.search_content(
    #     query = "What is the total trading volume of Tradeweb in August 2025?",
    #     fields = [])
    # print(matches)

    # pinecone_store.confirm_and_delete_all_records()

    # task_handle_mar.crawl_latest_mar_file()

    # from services.nlq import run_cli
    # run_cli()

    # print(mar_prompts.SQL_HELPER_CATALOG_STR)


    # the_query = "What is the total trading volume of Tradeweb in August 2025?"
    # the_query = "Why did Aug 2025 differ from Aug 2026?"
    # the_query = "What is ADV for Credit in Aug 2025?"
    # the_query = "What is ADV for cash productsin Aug 2025?"

    # the_query = "What is ADV for cash productsin Aug 2025? And how about for credit?"

    # the_query = "YoY comparison of ADV for cash products for August, comparing 2025 to 2024"

    # the_query = "What's our market share in credit products and why did it change?"
    # tasks = mar_orchestrator.handle_user_query(user_query=the_query)
    # print(tasks)

    # the_query = "Hi!"
    # tasks = mar_orchestrator.handle_user_query(user_query=the_query)
    # print(tasks)


    # info = submit_sql_query("SELECT * FROM mar_combined_m LIMIT 10")
    # info = execute_vector_query("US Government bond decline reason in August 2025?")
    # print(info)
    # print(info.result.hits[0].fields['report_name'])
    # print(info.result.hits[0].fields['url'])

    # print(common_utils.get_mar_table_schema_str())

    # print(common_utils.execute_sql_query("SELECT DISTINCT year FROM mar_combined_m"))


    # print(common_utils.get_pr_available_in_storage_str())
    
    completed_tasks = []
    completed_results = []
    query = "Why did Credit volumes drop in Aug 2025?"
    current_task = query_breaker.break_down_query(query, completed_tasks, completed_results)
    print(current_task)

    plan = task_planner.plan_query_action(current_task)
    print(plan)

    pass




