from scrapy.selector import Selector
from scrapy.http.request import Request
import tldextract, logging, random
from cambodiajobs.settings import *

class ProxiesMiddleware(object):
    
    with open('proxies.txt') as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    PROXIES_LIST = [x.strip() for x in content]
    PROXIES_LIST = filter(None, PROXIES_LIST) 

    def __init__(self, settings):
        pass

    @classmethod
    def from_crawler(cls, crawler):
            return cls(crawler.settings)

    def process_request(self, request, spider):
        if self.PROXIES_LIST:
            request.meta['proxy'] = random.choice(self.PROXIES_LIST).strip()
                