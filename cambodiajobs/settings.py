# -*- coding: utf-8 -*-

BOT_NAME = 'cambodiajobs'

SPIDER_MODULES = ['cambodiajobs.spiders']
NEWSPIDER_MODULE = 'cambodiajobs.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

RETRY_TIMES = 20

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 20
DOWNLOADER_CLIENTCONTEXTFACTORY = 'cambodiajobs.context.CustomContextFactory'

FEED_EXPORT_ENCODING = 'utf-8'
FEED_FORMAT='json'