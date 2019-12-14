from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
import os
from pathlib import Path
import uuid
import logging

import pendulum


local_tz = pendulum.timezone('Asia/Seoul')

def crawl_daumnews(media_code, **kwargs):
    from scrapy.crawler import CrawlerProcess
    from spiders.daum_news import DaumNewsSpider
    
    logging.disable(logging.DEBUG)
    execution_date = kwargs['execution_date'].astimezone(tz=local_tz)
    temp_dir = TemporaryDirectory()
    temp_file_name = str(uuid.uuid4()) + '.json'
    temp_file = Path(temp_dir.name) / temp_file_name

    crawler_process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:64.0) Gecko/20100101 Firefox/64.0',
        'ROBOTSTXT_OBEY': False,
        'FEED_FORMAT': 'jsonlines',
        'FEED_URI': str(temp_file),
        'LOG_FILE': str(Path(temp_dir.name) / 'scrapy.log'),
        'LOG_LEVEL': 'INFO',
        'DOWNLOAD_DELAY': 0.0,
        'CLOSESPIDER_ERRORCOUNT': 1,
        'HTTPERROR_ALLOWED_CODES': [],
        'CONCURRENT_ITEMS': 8,
        'CONCURRENT_REQUESTS': 8,
    })
    
    crawler = crawler_process.create_crawler(DaumNewsSpider)
    crawler_process.crawl(crawler, media_code=media_code, reg_date=execution_date.strftime('%Y%m%d'))
    crawler_process.start(stop_after_crawl=True)

    if 'log_count/ERROR' in crawler.stats.get_stats():
        temp_dir.cleanup()
        raise Exception('Error log exists')
    
    if not 'item_scraped_count' in crawler.stats.get_stats():
        temp_dir.cleanup()
        return

    import pandas as pd
    df = pd.read_json(str(temp_file), lines=True, convert_dates=['publish_date'])
    df.to_parquet(str(temp_file) + '.parquet', index=False, compression='gzip')

    from airflow.hooks.S3_hook import S3Hook

    key = f'daum_news/media_code={media_code}/date={execution_date.format("YYYYMMDD")}/0.parquet'
    hook = S3Hook(s3_conn_id='wasabi')    
    hook.load_file(str(temp_file), key, 'ingtranet-library')
    temp_dir.cleanup()
    return temp_file_name
