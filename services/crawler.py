import requests
from bs4 import BeautifulSoup
import trafilatura
import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter
import os
import logging
import hashlib

import services.helper as helper
import services.utils as utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def crawl_one(
    url: str,
    *,
    exclude_social_media_links: bool = True,
    exclude_external_links: bool = False,
    excluded_tags: list[str] = ['nav', 'footer', 'form', 'aside', 'header'],
    ignore_links_for_markdown: bool = True,
    create_fit_markdown: bool = True,
    pruning_threshold: float = 0.6,
    threshold_type: str = 'fixed',
    word_count_threshold: int = 10,
    remove_overlay_elements: bool = True,
    headless: bool = True,
    verbose_browser: bool = False,
) -> dict:
    '''
        Crawl a single URL and return content based on needs
      
        Args:
            url: The URL to crawl
            *: It's a symbol to enforce below it must be keyword arguments
            ignore_links: Whether to ignore links
            exclude_social_media_links: Whether to exclude social media links
            exclude_external_links: Whether to exclude external links
            excluded_tags: Tags to exclude
            create_fit_markdown: Whether to create fit markdown, it's a markdown after removing unimportant content use an algorithm
            pruning_threshold: How aggressively the prune algorithm works. Higher value means more content gets removed.
            threshold_type: Type of threshold to use for pruning algorithm, choose from 'fixed' or 'dynamic'
            word_count_threshold: Minimum word count is required for keeping a block, for removing tiny text blocks
            remove_overlay_elements: Whether to close/pop modals if detected
            verbose_browser: Whether to verbose the browser

        Returns:
            - raw_markdown: markdown content without pruning algorithm
            - fit_markdown: markdown content after pruning algorithm
            - cleaned_html: cleaned HTML with tags
            - internal_links: url in the same domain as the crawled url
            - external_links: url in different domain as the crawled url
        
        [Note] Pruning algorithm:
        It assesses parts (nodes) of the HTML based on various heuristics, for example: text density, 
        link density, and tag importance. The node with low score will be removed. 

        [Note] Threshold type:
        - fixed: Every content node must meet or exceed the specified threshold value to stay.
        - dynamic: he required cutoff changes depending on contextual cues (e.g. tag type, how many links vs text in a block, how prominent the block is) — so what gets kept adapts to the page structure and content density.

    '''

    # :::::: Crawler Setttings :::::: #

    # 1. Browser config
    browser_cfg = BrowserConfig(headless = headless, 
                                verbose = verbose_browser)  # headless by default

    # 2. Create content filter only when use_fit_markdown is True
    # Warning: If you don't know what you are doing, you can easily remove important content.
    content_filter = None
    if create_fit_markdown:
        content_filter = PruningContentFilter(
            threshold = pruning_threshold,
            threshold_type = threshold_type,
            min_word_threshold = word_count_threshold
        )

    # 3. Markdown generator with a pruning filter (general-purpose junk content remover)
    md_generator = DefaultMarkdownGenerator(
        content_source = "cleaned_html",
        options = {
            "ignore_images": True,
            "ignore_links": ignore_links_for_markdown,
            "body_width": 0,  # no hard wrap; keep paragraphs intact
        },
        content_filter = content_filter,
    )

    # 4. Run config: light exclusions + sensible defaults
    run_cfg = CrawlerRunConfig(
        markdown_generator = md_generator,
        word_count_threshold = word_count_threshold,
        excluded_tags=excluded_tags,
        exclude_social_media_links = exclude_social_media_links,
        exclude_external_links = exclude_external_links,
        process_iframes = True,
        remove_overlay_elements = remove_overlay_elements,
    )

    # :::::: Run the crawler :::::: #

    async with AsyncWebCrawler(config = browser_cfg) as crawler:
        result = await crawler.arun(url = url, config = run_cfg)

    if not result.success:
        raise RuntimeError(f"Crawl failed ({result.status_code}): {result.error_message}")

    # :::::: Extract the content :::::: #

    # Take the markdown object
    md_obj = result.markdown

    # 1. Raw markdown
    # Sometimes, the markdown object is a string, so we need to convert it to a string
    raw_md = getattr(md_obj, "raw_markdown", None) or str(md_obj)
    raw_md = raw_md.strip()

    # 2. Fit markdown
    fit_md = None
    if create_fit_markdown and hasattr(md_obj, "fit_markdown"):
        fit_md = md_obj.fit_markdown.strip()
    
    # 3. Links
    internal_links = result.links.get("internal", [])
    external_links = result.links.get("external", [])

    # 4. Also cleaned HTML if needed
    cleaned_html = result.cleaned_html

    return {
        "raw_markdown": raw_md,
        "fit_markdown": fit_md,
        "cleaned_html": cleaned_html,
        "internal_links": internal_links,
        "external_links": external_links,
    }
    

async def crawl_many(urls: list[str], **kwargs) -> dict[str, str]:
    """
      Crawl many URLs concurrently; 

      Args:
        urls: A list of URLs to crawl
        **kwargs: Keyword arguments to pass to crawl_one
      
      return: 
        A dict with the URL as the key, and the value is a dict of content. For example below.

        {url1: {raw_markdown, fit_markdown, cleaned_html, internal_links, external_links}, 
         url2: {raw_markdown, fit_markdown, cleaned_html, internal_links, external_links}, 
         ...,}
    """
    tasks = [crawl_one(u, **kwargs) for u in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    out: dict[str, dict] = {}
    for u, r in zip(urls, results):
        if isinstance(r, Exception):
            out[u] = f"__ERROR__: {r}"
        else:
            out[u] = r
    return out

def download_file(url: str, dest_dir: str, file_name: str = None, chunk_size: int = 8192):
    """
    Downloads file from the given URL and saves it to dest_path.
    Uses streaming so as not to load entire file into memory.
    """
    logger.info(f"Downloading file from {url} to {dest_dir}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()  # raises exception for HTTP errors

    # Create the destination directory
    os.makedirs(dest_dir, exist_ok=True)

    # Create the destination file path
    if file_name is None:
        file_name = url.split("/")[-1]
    file_path = os.path.join(dest_dir, file_name)

    # Write the file
    try:
        with open(file_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive chunks
                    f.write(chunk)
        logger.info(f"Downloaded file to {file_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to download file: {e}")

    return file_path

def crawl_and_save_markdown(url: str, 
                            file_dir: str, 
                            file_name_strategy: str = 'url', 
                            user_defined_file_name: str = None):
    '''
        Crawl a single URL and save the markdown content to a file.

        Args:
            url: The URL to crawl
            file_dir: The directory to save the markdown file
            file_name_strategy: The strategy for the file name. (Options: 'url_last_part', 'url_hash', 'customized')
            user_defined_file_name: If file_name_strategy is 'customized', the file name must be provided.
    '''
    # Regularize the URL
    url = utils.regularize_url(url)

    # Check to enforce providing file_name
    if file_name_strategy == 'customized' and user_defined_file_name is None:
        raise ValueError("If file_name_strategy is 'customized', file_name must be provided.")

    # Create the file name
    if file_name_strategy == 'url_last_part':
        file_name = utils.get_url_last_part(url)
    elif file_name_strategy == 'url_hash':
        file_name = hashlib.sha256(url.encode()).hexdigest()
    elif file_name_strategy == 'customized':
        file_name = user_defined_file_name
    else:
        raise ValueError(f"Invalid file_name_strategy: {file_name_strategy}")

    # Crawl the site 
    result = asyncio.run(crawl_one(url))
    md_raw = result['raw_markdown']

    # Store file
    os.makedirs(file_dir, exist_ok=True)

    # Create the file path
    file_path = f'{file_dir}{file_name}.md'

    utils.write_file(md_raw, file_path)
    logger.info(f"Saved markdown from {url} to {file_path}")

    return file_path