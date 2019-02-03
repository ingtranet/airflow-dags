import scrapy
from newspaper import Article
from newspaper.article import ArticleException

class DaumNewsSpider(scrapy.Spider):
    name = 'daum_news'

    def start_requests(self):
        yield scrapy.Request(url=self.generate_url(1),
            callback=self.parse_page, 
            meta={'page': 1})

    def parse_page(self, response):
        self.logger.debug('Parsing URL: {}'.format(response.url))
        article_urls = response.css('.list_allnews a.link_txt::attr("href")').extract()

        if len(article_urls):
            page = response.meta['page']
            yield scrapy.Request(url=self.generate_url(page + 1),
                callback=self.parse_page, 
                meta={'page': page + 1})
                
        for url in article_urls:
            yield self.parse_article(url)

    
    def parse_article(self, url):
        self.logger.debug('Parsing URL: {}'.format(url))
        try:
            article = Article(url, language='ko', request_timeout=30)
            article.download()
            article.parse()
        except ArticleException as e:
            if 'Gone' in str(e):
                self.logger.warn('Gone while parsing article: ' + url)
                return
            else:
                raise e

        result = {
            'title': article.title,
            'text': article.text,
            'publish_date': article.publish_date.isoformat(' '),
            'media_code': self.media_code,
            'url': url
        } 
        result.update(self.parse_meta(article.meta_data))

        return result

    def parse_meta(self, meta):
        article = meta['article']
        return {
            'url': article['txid'],
            'media_name': article['media_name'],
            'service_name': article['service_name']
        }

    def generate_url(self, page):
        BASE_URL = 'https://media.daum.net/cp/{media_code}?page={page}&regDate={reg_date}'

        return BASE_URL.format(
            media_code=self.media_code,
            page=page,
            reg_date=self.reg_date)
        
