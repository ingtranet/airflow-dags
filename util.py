from scrapy.crawler import CrawlerProcess
from spiders.daum_news import DaumNewsSpider

import pandas as pd

from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
import os
from pathlib import Path
import uuid
import logging

import boto3
import pendulum


local_tz = pendulum.timezone('Asia/Seoul')

def crawl(media_code, **kwargs):
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
        'DOWNLOAD_DELAY': 0.25
    })
    
    crawler = crawler_process.create_crawler(DaumNewsSpider)
    crawler_process.crawl(crawler, media_code=media_code, reg_date=execution_date.strftime('%Y%m%d'))
    crawler_process.start(stop_after_crawl=True)

    if 'log_count/ERROR' in crawler.stats.get_stats():
        raise Exception('Error log exists')

    conn = BaseHook.get_connection('wasabi')
    s3 = boto3.resource('s3',
        endpoint_url = conn.extra_dejson['endpoint_url'],
        aws_access_key_id = conn.extra_dejson['aws_access_key_id'],
        aws_secret_access_key = conn.extra_dejson['aws_secret_access_key'],
        verify=False)
    
    s3.meta.client.upload_file(str(temp_file), 'ingtranet-temp', temp_file_name)
    temp_dir.cleanup()
    return temp_file_name

def temp_json_to_parquet(media_code, **kwargs):
    logging.disable(logging.DEBUG)
    execution_date = kwargs['execution_date'].astimezone(tz=local_tz)
    temp_file_name = kwargs['task_instance'].xcom_pull(task_ids='task_crawl')

    conn = BaseHook.get_connection('wasabi')
    
    s3 = boto3.resource('s3',
        endpoint_url = conn.extra_dejson['endpoint_url'],
        aws_access_key_id = conn.extra_dejson['aws_access_key_id'],
        aws_secret_access_key = conn.extra_dejson['aws_secret_access_key'])

    with TemporaryDirectory() as temp_dir:
        temp_file = Path(temp_dir) / temp_file_name
        s3.Bucket('ingtranet-temp').download_file(temp_file_name, str(temp_file))

        dfs = pd.read_json(str(temp_file), lines=True, convert_dates=['publish_date'], chunksize=750)
        
        for i, df in enumerate(dfs):
            file_path = Path(temp_dir) / '{}.gzip.parquet'.format(i)
            df.to_parquet(str(file_path), compression='gzip')
  
            s3.meta.client.upload_file(str(file_path), 'ingtranet-library', 'daum_news/{}/{}/{}/{}/{}'.format(
                media_code, execution_date.year, execution_date.month, execution_date.day, file_path.name
            ))
    
        
