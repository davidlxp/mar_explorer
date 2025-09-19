from services import task_handle_mar
from services.db import Database
import services.crawler as crawler
import services.task_handle_mar as task_mar
import logging
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

db = Database()

PR_URLs = [
    "https://www.tradeweb.com/newsroom/media-center/news-releases/tradeweb-reports-august-2025-total-trading-volume-of--$54.1-trillion-and-average-daily-volume-of-$2.5-trillion",
    "https://www.tradeweb.com/newsroom/media-center/news-releases/tradeweb-reports-july-2025-total-trading-volume-of--$55.0-trillion-and-average-daily-volume-of-$2.4-trillion",
    "https://www.tradeweb.com/newsroom/media-center/news-releases/tradeweb-reports-second-quarter-2025-financial-results/",
]

URL_MAR_DOMAIN = 'https://www.tradeweb.com/4a51d0/globalassets/newsroom/09.05.25-august-mar/tw-historical-adv-and-day-count-through-august-2025.xlsx'
FILE_DOWNLOAD_PATH = "storage/raw_files/tests/"

def test_run_ALL():
    test_run_mar_update()
    test_run_query1()
    test_run_crawler_one()
    test_run_crawler_many()
    test_run_download_file()

def test_run_mar_update():
  logger.info("Running test_run_query1")
  the_file = "storage/raw_files/mar_files/tw-historical-adv-and-day-count-through-august-2025.xlsx"
  task_mar.handle_mar_update(the_file)

def test_run_query1():
    logger.info("Running test_run_query1")
    test_query2 = """
        SELECT count(*)
        FROM mar_adv_m 
    """
    df = db.fetchdf(test_query2)
    print(df)

def test_run_crawler_one():
  logger.info("Running test_run_crawler_one")
  result = asyncio.run(crawler.crawl_one(PR_URLs[0]))
  print(result)

def test_run_crawler_many():
  logger.info("Running test_run_crawler_many")
  result = asyncio.run(crawler.crawl_many(PR_URLs))

  for url, dict in result.items():
      print("\n" + "=" * 80)
      print(url)
      print("-" * 80)
      print(dict.keys())  # preview keys

def test_run_download_file():
  logger.info("Running test_run_download_file")
  result = crawler.download_file(URL_MAR_DOMAIN, FILE_DOWNLOAD_PATH)
  print(result)