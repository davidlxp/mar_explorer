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
import copy

from services.constants import *
import services.chunk_utils as chunk_utils

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


# def handle_pr_embed_andupload(file):
#     text = file.read().decode("utf-8", errors="ignore")
#     chunks = [p.strip() for p in text.split("\n") if p.strip()]

#     model = SentenceTransformer("all-MiniLM-L6-v2")
#     embeddings = model.encode(chunks)

#     con = get_con()
#     con.execute("DELETE FROM pr_index;")
#     for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
#         con.execute("INSERT INTO pr_index VALUES (?, ?, ?)", [i, chunk, emb.tolist()])


def fetch_press_release(url: str) -> str:

    # :::::: Crawl and download MD  :::::: #
    
    # logger.info("Running fetch_press_release")

    # file_dir = 'storage/raw_files/pr_files/'

    # report_type = get_report_type(url)
    # report_date = get_report_date(url, report_type)

    # file_name = f"{PR_FILE_PREFIX}-{report_type}-{report_date}"

    # print(report_type, report_date)

    # if report_type is not None:
    #     # Crawl and save the markdown file
    #     crawler.crawl_and_save_markdown(url, file_dir, file_name_strategy = 'customized', user_defined_file_name = file_name)


    # :::::: Check the crawl one result  :::::: #

    # result = asyncio.run(crawler.crawl_one(url))
    # html = result['cleaned_html']

    # soup = BeautifulSoup(html, "html.parser")

    # # select the second section under main
    # target = soup.select("main > section:nth-of-type(2) > div > div")
    # print(target)



    # :::::: Parse the press release :::::: #


    file_dir = 'storage/raw_files/pr_files/'

    # file_name = 'tradeweb_reports-monthly-2025_08'
    # file_name = 'tradeweb_reports-monthly-2023_05'
    # file_name = 'tradeweb_reports-monthly-2019_02'
    # file_name = 'tradeweb_reports-quarterly-2025_q1'
    file_name = 'tradeweb_reports-yearly-2021'

    file_path = f'{file_dir}{file_name}.md'

    parse_pr_m(file_path)


def try_rm_junk_part_for_pr(md_text: str) -> str:
    '''
        Try to remove the no-need part of press release. Handles monthly, quarterly or yearly press releases.

        Removal strategy:
        1. Section before "| Tradeweb" or "| Tradeweb Markets" line can be removed. Usually the headline of article.
        2. The line which is a list only contains 'twitter', 'linkedin', 'print', 'email' line can be removed.
        3. Start to remove (remove all the lines after) from the earliest section among "About Tradeweb Markets", "Media Contact", "Investor Contact", "Basis of Presentation", "Market and Industry Data", "Forward-Looking Statements".
        4. Start to remove the section after a line like "go to https://... for more infomation" section. There's a lot of pattern like this.
        [Find the smallest index between 3 and 4]

        The head part is including or before the "| Tradeweb" or "| Tradeweb Markets" line.
    '''
    lines = md_text.splitlines()

    # The indexes tracks where to start removing the head and tail part
    head_rm_idx = None
    min_tail_rm_idx = None

    # :::::: Start Removal Strategy :::::: #

    for i in range(len(lines)):
        line = lines[i]

        # Find the index to remove the head part
        if head_rm_idx is None and any(pattern in line.strip().lower() for pattern in PR_M_MD_HEAD_RM_PATTERNS):
            head_rm_idx = i

        # Find the index to remove the tail section part 
        if min_tail_rm_idx is None:
            line_regularized = line.replace('#', '').strip().lower()
            if any(pattern == line_regularized for pattern in PR_M_MD_TAIL_SECTION_RM_PATTERNS) or \
                any(pattern in line.strip().lower() for pattern in PR_M_MD_TAIL_RM_PATTERNS):
                    min_tail_rm_idx = i

    # :::::: Preparing Output :::::: #

    out = copy.deepcopy(lines)

    # Remove the tail part (MUST remove the TAIL before HEAD or running into index error!)
    if min_tail_rm_idx is not None:
        out = out[:min_tail_rm_idx]

    # Remove the head part
    if head_rm_idx is not None:
        out = out[head_rm_idx + 1:]

    # Remove the lines only consists of information in the PR_M_MD_LINES_TO_RM list
    out = [line for line in out if line.strip().lower() not in PR_M_MD_LINES_TO_RM]

    # Remove empty lines
    out = [line for line in out if line.strip().lower() != '']

    return "\n".join(out)


def turn_md_into_blocks_pr(md_text: str) -> tuple[list[str], list[list[str]]]:
    '''
        Only for Tradeweb Press Release for now.
        Given list of lines in markdown, split them into different blocks.
        Only try to split by 1 level of hierarchy.
        
        The md could look like:
        parent_line_1
           child_line_1
           child_line_2
        parent_line_2
           child_line_1
           child_line_2
           child_line_3

        Parent line Identification Rules (OR):
        1. A line with no leading spaces.
        2. A line starts with '**'.

    '''
    def is_parent(line: str) -> bool:
        return len(line) == len(line.lstrip(' ')) or line.strip().startswith('**')

    # Split the md text into lines
    lines = md_text.splitlines()

    # :::::: Identify Parents and Children :::::: #

    # Walk to identify the starting of the block (As parent) and associate children lines of it.
    # Didn't use dict to avoid potential duplicate parent lines
    parents = []
    children_groups = []

    # Handle edge case - if the article starts without parent line
    if not is_parent(lines[0]):
        current_parent = ''
        parents.append(current_parent)
        children_groups.append([])
    else:
        current_parent = None

    # Normal process
    for i, line in enumerate(lines):

        # If a line starts with '**' or it's a line with no leading spaces, it's the starting of a new block
        if is_parent(line):
            current_parent = line
            parents.append(current_parent)
            children_groups.append([])
        else:
            children_groups[-1].append(line)

    return (parents, children_groups)


def split_md_to_chunks_pr_m(md_text: str) -> str:
    '''
        Try to separate the content into chunks, it's preparing for the content embedding process.

        If can find the separate pattern, do separation.
        If can't find the separate pattern, treat the time we find the next line indentation to detect subline.
    '''
    lines = md_text.splitlines()

    # :::::: Split into head and content section, they are handled differently :::::: #

    # Find the index to separate the content
    idx_to_separate = None
    for i, line in enumerate(lines):
        if any(pattern in line.strip().lower() for pattern in PR_M_MD_CONTENT_SEPARATE_PATTERN):
            idx_to_separate = i
            break
    
    # Split the article into head and content section
    if idx_to_separate is not None:
        head_section = lines[:idx_to_separate]
        content_section = lines[idx_to_separate + 1:]
    else:
        head_section = lines
        content_section = None

    # Prepare to store the final output
    chunks_out = []

    # :::::: Handle Head Section :::::: #

    if head_section is not None:

        # Split the head section into chunks
        head_chunks = chunk_utils.split_into_chunks(lines = head_section, 
                                                    max_token_count = PR_DEFAULT_TOKEN_MAX_FOR_EMBEDDING, 
                                                    model_name = PR_DEFAULT_EMBEDDING_MODEL, 
                                                    tag_content = '',
                                                    tag_content_allowed_token = PR_DEFAULT_TAG_CONTENT_ALLOWED_TOKEN,
                                                    chunk_overlap_lines = PR_DEFAULT_CHUNK_OVERLAP_LINES)
        
        chunks_out.extend(head_chunks)

    # :::::: Handle Content Section :::::: #

    if content_section is not None:

        # Turn the content section into blocks
        parents, children_groups = turn_md_into_blocks_pr('\n'.join(content_section))
        
        # Split the children into chunks
        for i, children in enumerate(children_groups):
            parent_tag = parents[i] + "\n"
            curr_chunks = chunk_utils.split_into_chunks(lines = children, 
                                                        max_token_count = PR_DEFAULT_TOKEN_MAX_FOR_EMBEDDING, 
                                                        model_name = PR_DEFAULT_EMBEDDING_MODEL, 
                                                        tag_content = parent_tag,
                                                        tag_content_allowed_token = PR_DEFAULT_TAG_CONTENT_ALLOWED_TOKEN,
                                                        chunk_overlap_lines = PR_DEFAULT_CHUNK_OVERLAP_LINES)
            chunks_out.extend(curr_chunks)
        
    return chunks_out


def parse_pr_m(file_path: str) -> str:
    '''
        Parse the monthly press release.
    '''
    md_raw = utils.read_file(file_path)
    md_raw = try_rm_junk_part_for_pr(md_raw)
    md_chunks = split_md_to_chunks_pr_m(md_raw)

    print("="*100)
    for chunk in md_chunks:
        print(chunk)
        print("\n\n")
        print("="*100)


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
        logger.info(f"The URL is not a monthly, quarterly or yearly report: {url}")
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

