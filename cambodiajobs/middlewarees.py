from scrapy.selector import Selector
from scrapy.http.request import Request
import tldextract, logging, random
from cambodiajobs.settings import *


class ProxiesMiddleware(object):
    def __init__(self, settings):
        pass

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        if PROXIES_LIST:
                request.meta['proxy'] = random.choice(PROXIES_LIST).strip()
                