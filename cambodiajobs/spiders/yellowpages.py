# -*- coding: utf-8 -*-
from cambodiajobs.all_imports import *   

class YpSpider(scrapy.Spider):
	name = "yp"
        
        tbl_name = "companies"
	
        cursor = connectDB()

	headers = {
	    'DNT': '1',
	    'Accept-Encoding': 'gzip, deflate, sdch, br',
	    'Accept-Language': 'en-US,en;q=0.8',
	    'Upgrade-Insecure-Requests': '1',
	    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
	    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
	    'Referer': 'https://www.yp.com.kh/',
	    'Connection': 'keep-alive',
	    'Cache-Control': 'max-age=0',
	}

	custom_settings = {
		'DOWNLOADER_MIDDLEWARES': {
		    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 200,
		    'cambodiajobs.middlewares.ProxiesMiddleware': 300,
		},
        	'ITEM_PIPELINES': {
                        'cambodiajobs.pipelines.Format': 1,
                        'cambodiajobs.pipelines.CambodiacompaniesPipeline': 200,
       		}
	}
	all_products_in_db = {}
        
        page=1
        
        searchUrl = "https://www.yp.com.kh/search_results?q=&page="
        
	def __init__(self, *args, **kwargs):
        	super(YpSpider, self).__init__(*args, **kwargs)

	def start_requests(self):
		query = "SELECT CompanyID,CompanyProfile FROM `%s`" % (self.tbl_name)
		
		execute_query(query, self.cursor)

		for row in self.cursor.fetchall():
			self.all_products_in_db[ row['CompanyID'] ] = row['CompanyProfile']

        	yield Request(url=self.searchUrl+str(self.page), callback=self.parse_listing_page, headers=self.headers)


	def parse_listing_page(self, response):
            
                results = response.css("div.search_result")
                
                if results:
                    for comp in response.css("div.search_result"):
                            CompanyID = comp.css("a::attr(href)").extract_first().lstrip("/")
                            if CompanyID in self.all_products_in_db:
                                logging.info("%s already exists in DB"%(CompanyID))
                            else:
                                yield Request(url='https://www.yp.com.kh/%s'%(CompanyID), callback=self.parse_detail_page, headers=self.headers, meta={'CompanyID':CompanyID})

                    self.page = self.page + 1 
                    next_page = self.searchUrl+str(self.page)
                    logging.info("\n\n\nGoing to next page: %s"%(next_page))
                    yield Request(url=next_page, callback=self.parse_listing_page, headers=self.headers)
                
                else:
                    logging.info("\n\n\nwas last page"%(response.url))
                    
                        
	def parse_detail_page(self, response):
                        
                data = {}
                
                data['CompanyID'] = response.meta['CompanyID']
                data['CompanyName'] = response.css("h1::text").extract_first()
                data['CompanyLogo'] = response.css("#website_logo > a > img::attr(src)").extract_first()
                data['CompanyProfile'] = response.url
                
                data['CompanyAddress'] = "".join(a.strip() for a in response.css("[itemprop='address'] *::text").extract())
                
                data['CompanyPhones'] = response.css('[itemprop="telephone"] div::text').extract_first()
                if data['CompanyPhones'] is not None:
                    data['CompanyPhones'] =  re.sub(u"\s\s+","",data['CompanyPhones'])
                    data['CompanyPhones'] = data['CompanyPhones'].split("/")

                data['CompanyFax'] = response.xpath("//li[contains(text(),'Fax Number')]/following-sibling::li[1]//text()").extract_first()
                if data['CompanyFax'] is not None:
                    data['CompanyFax'] =  re.sub(u"\s\s+","",data['CompanyFax'])
                    data['CompanyFax'] = data['CompanyFax'].split("/")
                
                data['CompanyUrl'] = response.css('[itemprop="url"]::text').extract_first()
                data['CompanyTags'] = []
                
                for tag in response.css("div.tpad.tags.font-sm > a"):
                    data['CompanyTags'].extend([tag.css("::attr(title)").extract_first()])
                
                
                GetDirection = response.xpath("//*[contains(text(),'Get Directions')]/@href").extract_first()
                
                if GetDirection:
                    GetDirection = urlparse.urlparse(GetDirection)
                    
                    GetDirection = urlparse.parse_qs(GetDirection.query)['daddr'][0]
                    
                    data['Latitude']=GetDirection.split(",")[0]
                    data['Longitude']=GetDirection.split(",")[1]
                
                yield data
                
                
                
                
                
                                  
