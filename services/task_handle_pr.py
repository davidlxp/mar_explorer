# from sentence_transformers import SentenceTransformer
from services.db import Database
import requests
from bs4 import BeautifulSoup
import services.crawler as crawler
import logging
import asyncio
import os
import hashlib
import json
import re
from datetime import datetime
from services.constants import PR_URL_DOMAIN, MONTHLY_PR_PATTERN, QUARTERLY_PR_PATTERN, YEARLY_PR_PATTERN, PR_QUARTER_MAPPING

from mistletoe import Document
from mistletoe.block_token import Heading, Paragraph, List
from mistletoe.ast_renderer import ASTRenderer


import services.utils as utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# def handle_pr_upload(file):
#     text = file.read().decode("utf-8", errors="ignore")
#     chunks = [p.strip() for p in text.split("\n") if p.strip()]

#     model = SentenceTransformer("all-MiniLM-L6-v2")
#     embeddings = model.encode(chunks)

#     con = get_con()
#     con.execute("DELETE FROM pr_index;")
#     for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
#         con.execute("INSERT INTO pr_index VALUES (?, ?, ?)", [i, chunk, emb.tolist()])


def fetch_press_release(url: str) -> str:
    
    logger.info("Running fetch_press_release")

    file_dir = 'storage/raw_files/pr_files/'

    report_type = get_report_type(url)
    report_date = get_report_date(url, report_type)

    print(report_type, report_date)

    
    # file_name = url.split("/")[-1]

    # Crawl and save the markdown file
    # crawler.crawl_and_save_markdown(url, file_dir, file_name_strategy = 'url_hash')



    # file_dir = 'storage/raw_files/pr_files/'
    # file_name = 'tradeweb-reports-august-2025-total-trading-volume-of--$54.1-trillion-and-average-daily-volume-of-$2.5-trillion'
    # file_path = f'{file_dir}{file_name}.md'

    # md_raw = utils.read_file(file_path)

    # # Parse and produce AST
    # with ASTRenderer() as renderer:
    #     ast = renderer.render(Document(md_raw))

    # # print(json.dumps(ast, indent=2, ensure_ascii=False))
    # print_tree(ast)
    


def get_report_type(url: str) -> str:
    '''
        Check if the URL is a monthly, quarterly or yearly report.
        Return the type of the report.
    '''
    logger.info(f"Checking if the URL is a monthly, quarterly or yearly report: {url}")

    # If it's not Tradeweb domain, return None
    if not url.startswith(PR_URL_DOMAIN):
        logger.info(f"The URL is not a Tradeweb domain: {url}")
        return None

    # If it's Tradeweb domain, do the check
    if is_monthly_report(url):
        return "monthly"
    elif is_quarterly_report(url):
        return "quarterly"
    elif is_yearly_report(url):
        return "yearly"
    else:
        return None


def is_monthly_report(url: str) -> bool:
    '''
        Check if the URL is a monthly report.
        The URL last part like "tradeweb-reports-august-2025-..."
    '''
    # Get the URL last part
    url_last_part = utils.get_url_last_part(url)

    pattern = re.compile(MONTHLY_PR_PATTERN, re.IGNORECASE)
    return bool(pattern.search(url_last_part))

def is_quarterly_report(url: str) -> bool:
    '''
        Check if the URL is a quarterly report.
        The URL last part like "tradeweb-reports-first-quarter-2025-..."
    '''
    # Get the URL last part
    url_last_part = utils.get_url_last_part(url)

    pattern = re.compile(QUARTERLY_PR_PATTERN, re.IGNORECASE)
    return bool(pattern.search(url_last_part))


def is_yearly_report(url: str) -> bool:
    '''
        Check if the URL is a yearly report.
        The URL last part like "tradeweb-reports-fourth-quarter-and-full-year-2025-..."
    '''
    # Get the URL last part
    url_last_part = utils.get_url_last_part(url)

    pattern = re.compile(YEARLY_PR_PATTERN, re.IGNORECASE)
    return bool(pattern.search(url_last_part))


def get_report_date(url: str, report_type: str) -> str:
    '''
        Get the report date from the URL.
        Args:
            url: The URL to get the report date from
            report_type: The type of the report
        Returns:
            The report date
    '''
    logger.info(f"Getting the report date from the URL: {url} for report type: {report_type}")

    url_last_part = utils.get_url_last_part(url)

    if report_type is None:
        out = None

    elif report_type == "monthly":
        # Extract the year and month
        year = url_last_part.split("-")[3]
        month = url_last_part.split("-")[2]

        # Convert the month to the format MM
        month = datetime.strptime(month, "%B").strftime("%m")

        # Format the report date
        out = f"{year}_{month}"

    elif report_type == "quarterly":
        # Extract the year and quarter
        year = url_last_part.split("-")[4]
        quarter_str = url_last_part.split("-")[2]

        # Convert the quarter to the format Q1, quarter is current in format "first", "second", "third", "fourth"
        quarter = PR_QUARTER_MAPPING[quarter_str]
        
        # Format the report date
        out = f"{year}_{quarter}"

    elif report_type == "yearly":
        # Extract the year
        year = url_last_part.split("-")[7]
        
        # Format the report date
        out = f"{year}"
    
    else:
        raise ValueError(f"Invalid report type: {report_type}")
    
    logger.info(f"The report date is: {out}")
    return out

