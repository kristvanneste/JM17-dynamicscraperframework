# -*- coding: utf-8 -*-

BOT_NAME = 'cambodiajobs'

SPIDER_MODULES = ['cambodiajobs.spiders']
NEWSPIDER_MODULE = 'cambodiajobs.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

RETRY_TIMES = 100

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 20
DOWNLOADER_CLIENTCONTEXTFACTORY = 'cambodiajobs.context.CustomContextFactory'

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

FEED_EXPORT_ENCODING = 'utf-8'
FEED_FORMAT='json'