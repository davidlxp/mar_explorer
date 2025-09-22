from services.db import get_database
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
import numpy as np

from services.constants import *
import services.chunk_utils as chunk_utils
from services.embeddings import get_embedder
import services.utils as utils
import services.vectorstores.pinecone_store as pinecone_store

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Instantiate DB object
db = get_database()

async def fetch_many_press_releases(urls: list[str]) -> list[str]:
    """
        Fetch many press releases concurrently.
    """
    logger.info(f"Fetching many press releases: {urls}")

    # Prepare tasks
    tasks = []
    for url in urls:
        report_type, report_date, file_name, file_dir, meta_data = prepare_pr_info_for_fetch(url)
        if report_type is not None:
            tasks.append(crawler.crawl_and_save_markdown(url, 
                                                        file_dir, 
                                                        file_name_strategy = 'customized', 
                                                        user_defined_file_name = file_name, 
                                                        meta_data = meta_data))

    # Fetch many press releases concurrently
    if len(tasks) == 0:
        logger.info(f"No press releases to fetch")
    else:
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"Fetched many press releases: {urls}")
    

def prepare_pr_info_for_fetch(url: str) -> dict:
    '''
        Prepare the press release information for fetching.
    '''
    file_dir = PR_FILES_FOLDER_PATH_STR

    report_type = get_report_type(url)
    report_date = get_report_date(url, report_type)

    file_name = f"{PR_FILE_PREFIX}-{report_type}-{report_date}"

    # Start with None data
    pr_year = None
    pr_month = None
    pr_quarter = None

    # Prepare meta data
    if report_type == "monthly":
        pr_year = int(report_date.split("_")[0])
        pr_month = int(report_date.split("_")[1])
    elif report_type == "quarterly":
        pr_year = int(report_date.split("_")[0])
        pr_quarter = int(report_date.split("_")[1].replace("q", ""))
    elif report_type == "yearly":
        pr_year = int(report_date)

    meta_data = {
        'url': url,
        'report_name': file_name,
        'report_type': report_type,
        'year': pr_year,
        'month': pr_month,
        'quarter': pr_quarter
    }

    return report_type, report_date, file_name, file_dir, meta_data


async def fetch_press_release(url: str) -> str:
    '''
        Fetch a press release.
        Args:
            url: The URL of the press release
        Returns:
            The path to the markdown file
    '''
    # :::::: Crawl and download MD  :::::: #
    
    logger.info("Running fetch_press_release")

    report_type, report_date, file_name, file_dir, meta_data = prepare_pr_info_for_fetch(url)

    logger.info(f"Meta data of fetch press release: {meta_data}")
    
    result = None
    if report_type is not None:
        # Crawl and save the markdown file
        result = await crawler.crawl_and_save_markdown(url, 
                                        file_dir, 
                                        file_name_strategy = 'customized', 
                                        user_defined_file_name = file_name, 
                                        meta_data = meta_data)
    return result

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


def split_md_to_chunks_pr(md_text: str, metadata: dict) -> str:
    '''
        Try to separate the content into chunks, it's preparing for the content embedding process.

        If can find the separate pattern, do separation.
        If can't find the separate pattern, treat the time we find the next line indentation to detect subline.

        Args:
            md_text: The markdown text to be split into chunks
            meta_data: The meta data of the press release where this chunk belongs to
        Returns:
            A list of text, and each stands for a chunk
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
                                                    chunk_overlap_lines = PR_DEFAULT_CHUNK_OVERLAP_LINES,
                                                    chunking_strategy = DEFAULT_CHUNKING_STRATEGY)
        
        chunks_out.extend(head_chunks)

    # :::::: Handle Content Section :::::: #

    if content_section is not None:

        # Turn the content section into blocks
        parents, children_groups = turn_md_into_blocks_pr('\n'.join(content_section))
        
        if metadata['report_type'] == "monthly":
            chunk_overlap_lines = PR_M_CHUNK_OVERLAP_LINES
            chunking_strategy = PR_M_CHUNKING_STRATEGY
        else:
            chunk_overlap_lines = DEFAULT_CHUNK_OVERLAP_LINES
            chunking_strategy = DEFAULT_CHUNKING_STRATEGY

        # Split the children into chunks
        for i, children in enumerate(children_groups):
            parent_tag = parents[i] + "\n"
            curr_chunks = chunk_utils.split_into_chunks(lines = children, 
                                                        max_token_count = PR_DEFAULT_TOKEN_MAX_FOR_EMBEDDING, 
                                                        model_name = PR_DEFAULT_EMBEDDING_MODEL, 
                                                        tag_content = parent_tag,
                                                        tag_content_allowed_token = PR_DEFAULT_TAG_CONTENT_ALLOWED_TOKEN,
                                                        chunk_overlap_lines = chunk_overlap_lines,
                                                        chunking_strategy = chunking_strategy)
            chunks_out.extend(curr_chunks)

    print("="*100)
    for chunk in chunks_out:
        print("="*100)
        print(chunk)
        
    return chunks_out


def add_signature_to_chunks_pr(chunks, metadata: dict):
    """
    Add a concise, machine-friendly signature to each chunk 
    to facilitate time-based search.
    """
    report_name = metadata["report_name"]
    report_type = metadata["report_type"]
    year = metadata["year"]
    month = metadata.get("month")
    quarter = metadata.get("quarter")

    # Build date signature
    if report_type == "monthly":
        date_sig = f"{year}-{month:02d}"
    elif report_type == "quarterly":
        date_sig = f"{year}-Q{quarter}"
    elif report_type == "yearly":
        date_sig = str(year)
    else:
        raise ValueError(f"Invalid report type: {report_type}")

    # Create unified signature
    signature = f"[report={report_name} | type={report_type} | date={date_sig}]"

    # Prepend signature to chunks
    return [f"{signature}\n{chunk}" for chunk in chunks]


def upload_pr_chunks_to_vectorstore(chunks, metadata: dict):
    '''
        Upload the press release chunks to the vector store.
        The embedding is done automatically by Pinecone.
    '''
    # Get the metadata of the PR
    url = metadata["url"]
    report_name = metadata["report_name"]
    report_type = metadata["report_type"]
    year = metadata["year"]
    month = metadata.get("month")
    quarter = metadata.get("quarter")

    # Created the records for upserting
    records_to_upsert = []
    for i, chunk in enumerate(chunks):

        # deterministic ID as you had
        id_str = f"{url}-{report_name}-{chunk}"
        id_hash = hashlib.sha256(id_str.encode()).hexdigest()

        metadata_chunk = {
            "url": str(url),
            "report_name": str(report_name),
            "report_type": str(report_type),
            "year": int(year) if year is not None else -1,
            "month": int(month) if month is not None else -1,
            "quarter": int(quarter) if quarter is not None else -1,
            "chunk_index": int(i)
        }

        # Each record includes “text” instead of “values” (vector) if using automated embedding
        records_to_upsert.append({
            "id": id_hash,
            "text": chunk,
            "metadata": metadata_chunk
        })

        # upsert_records is the operation which lets Pinecone embed automatically
        # adjust batch size if needed
        batch_size = 100
        for j in range(0, len(records_to_upsert), batch_size):
            batch = records_to_upsert[j : j + batch_size]
            pinecone_store.upsert_records(records=batch)


async def ingest_pr_md_file(file_path: str) -> bool:
    '''
        Parse the press release and upload to vector database.
    '''
    logger.info(f"Ingesting press release MD file: {file_path}")
    
    # Get the raw markdown and meta data
    md_raw = utils.read_file(file_path)
    metadata = utils.get_meta_file(file_path)

    # Clean up the PR markdown file
    md_cleaned = try_rm_junk_part_for_pr(md_raw)

    # Split the PR markdown file into chunks
    md_chunks = split_md_to_chunks_pr(md_cleaned, metadata)

    # Add signature to the chunks
    if PR_ADD_SIGNATURE_TO_CHUNKS:
        md_chunks = add_signature_to_chunks_pr(md_chunks, metadata)

    # Embed and upload to DB
    upload_pr_chunks_to_vectorstore(md_chunks, metadata)

    return True


async def ingest_many_pr_md_files(file_paths: list[str]) -> bool:
    '''
        Parse many press releases concurrently.
    '''
    tasks = [ingest_pr_md_file(file_path) for file_path in file_paths]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return all(results)


async def ingest_all_pr_md_in_storage() -> bool:
    """
    Parse all press releases in the storage.
    Returns True if parsing ran, False if nothing to parse.
    """
    if not os.path.exists(PR_FILES_FOLDER_PATH_STR):
        logger.warning(f"Storage folder not found: {PR_FILES_FOLDER_PATH_STR}")
        return False

    file_paths = [
        os.path.join(PR_FILES_FOLDER_PATH_STR, file_name)
        for file_name in os.listdir(PR_FILES_FOLDER_PATH_STR)
        if file_name.endswith(".md")
    ]

    if not file_paths:
        logger.info("No .md press release files found in storage.")
        return False

    await ingest_many_pr_md_files(file_paths)
    return True


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

