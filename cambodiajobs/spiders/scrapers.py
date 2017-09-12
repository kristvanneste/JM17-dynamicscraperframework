# -*- coding: utf-8 -*-
import scrapy,logging,json,re,urllib,MySQLdb,urlparse,urllib2,requests
from scrapy.http.request import Request
from scrapy.http import FormRequest
from collections import OrderedDict

from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

from cambodiajobs.db_creds import DB_CREDS

from datetime import datetime

stripHTMLregex = re.compile(r'(<script\b[^>]*>([\s\S]*?)<\/script>)|(<style\b[^>]*>([\s\S]*?)<\/style>)')
stripNonTelTags = re.compile(r'(<(?![^>]+tel:)(.|\n)*?>)')

emailsregex = re.compile('[\w\.-]+@[\w-]+\.[\w\.-]+')
mobilesregex = re.compile(r"(\(?(?<!\d)\d{3}\)?-? *\d{3}-? *-?\d{4})(?!\d)|(?<!\d)(\+\d{11})(?!\d)")

def connectDB():
	conn = MySQLdb.connect(user=DB_CREDS['user'], passwd=DB_CREDS['pass'], db=DB_CREDS['db'], host=DB_CREDS['host'], charset="utf8", use_unicode=True)
	cursor = MySQLdb.cursors.DictCursor(conn) 
	conn.autocommit(True)
	return cursor
    
def _execute_query(query, cursor, data=[]):

        try:
                if data:
                    ret = cursor.execute(query, data)
                else:
                    ret = cursor.execute(query)

        except Exception as e:
                if 'MySQL server has gone away' in str(e):
                        connectDB()
                        if data:
                            ret = cursor.execute(query, data)
                        else:
                            ret = cursor.execute(query)
                else:	
                        logging.info("Query: %s" % (query))
                        raise e

        return ret


def dedupeAndCleanList(_list):
        cleaned_list = []

        for item in _list:
                        if isinstance(item, tuple):
                                        item = ''.join(str(i.encode('utf-8')) for i in item) 
                        if item not in cleaned_list:
                                        cleaned_list.append(item)
        return cleaned_list


    
    
    


class SchedularSpider(scrapy.Spider):
    
    name = "schedular"
    
    cursor = connectDB()

    scrapydUrl = "http://localhost:6800"

    def __init__(self, schedular_id=None, *args, **kwargs):
            super(SchedularSpider, self).__init__(*args, **kwargs)
            self.spider_name = spider_name
            
            
    def start_requests(self):

            if self.schedular_id is not None:

                        logging.info("Loading all PGs associated with `schedular_id` = %s" % (self.schedular_id))

                        query = """SELECT schedular_to_pg_mapping.pg_id,
                                                spiders.spider_name,
                                                schedular.`Name` AS schedular_name
                                        FROM
                                                schedular_to_pg_mapping
                                        INNER JOIN spiders ON schedular_to_pg_mapping.pg_id = spiders.group_id
                                        INNER JOIN schedular ON schedular_to_pg_mapping.schedular_id = schedular.id
                                        WHERE
                                                schedular_id = %s""" % (self.schedular_id)

                        _execute_query(query, self.cursor)

                        for pg_id in self.cursor.fetchall():
                                
                                data = [
                                  ('project', 'default'),
                                  ('spider', pg_id['spider_name']),
                                ]
                                
                                logging.info("Running %s"%(str(data)))
                                
                                resp = requests.post(self.scrapydUrl+'/schedule.json', data=data)
                                
                                logging.info(resp.status_code)
                                logging.info(resp.text)

    
    
    
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
		
		_execute_query(query, self.cursor)

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
                
                
                
                
                
                                  
class EverjobsSpider(scrapy.Spider):
        
	name = "everjobs"
        tbl_name = "jobs"
	
        cursor = connectDB()

        headers = {
            'DNT': '1',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.8',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
        }

	custom_settings = {
		'DOWNLOADER_MIDDLEWARES': {
		    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 200,
		    'cambodiajobs.middlewares.ProxiesMiddleware': 300,
		},
        	'ITEM_PIPELINES': {
                        'cambodiajobs.pipelines.Format': 1,
                        'cambodiajobs.pipelines.CambodiajobsPipeline': 200,
       		}
	}
	all_jobs_in_db = {}
	all_jobs_scraped_this_run = {}

        page=1
        
        searchUrl = "https://www.everjobs.com.kh/en/jobs/?page="
        
	def __init__(self, *args, **kwargs):
                dispatcher.connect(self.spider_closed, signals.spider_closed)
        	super(EverjobsSpider, self).__init__(*args, **kwargs)
                

	def start_requests(self):
		query = "SELECT jobID FROM `%s`" % (self.tbl_name)
		
		_execute_query(query, self.cursor)
#
		for row in self.cursor.fetchall():
			self.all_jobs_in_db[ row['jobID'] ] = ''

                yield Request(url=self.searchUrl+str(self.page), callback=self.parse_listing_page, headers=self.headers)


        def parse_listing_page(self, response):
            
                results = response.css('.mobile-job-container[data-automation="jobseeker-jobs"]')
                
                if results:
                    for comp in results:
                            jobLink = comp.css(".headline3 a::attr(href)").extract_first().lstrip("/")
                            jobDateUpdated = comp.css(".job-date-title::text").extract_first()
                            
                            jobDateUpdated = datetime.strptime(jobDateUpdated, '%d %B %Y')
                            
                            jobLink = 'https://www.everjobs.com.kh/%s'%(jobLink)
                            
                            if jobLink.split("/")[-1].split(".html")[0] in self.all_jobs_in_db:
                                logging.info("%s already exists in DB. So skipping..."%(jobLink))
                            else:
                                
                                yield Request(url=jobLink, callback=self.parse_detail_page, headers=self.headers, meta={'jobDateUpdated': jobDateUpdated})
                            
                    self.page = self.page + 1 
                    next_page = self.searchUrl+str(self.page)
                    logging.info("\n\n\nGoing to next page: %s"%(next_page))
                    yield Request(url=next_page, callback=self.parse_listing_page, headers=self.headers)
                
                else:
                    logging.info("%s was last page"%(response.url))  
                        
                        
	def parse_detail_page(self, response):
            
            data = {}
                
            if "Sorry, this job was not found." in response.body:
                logging.info("\n\n\n%s was removed"%(response.url))
                
            else:

                data['jobDateUpdated'] = response.meta['jobDateUpdated']
                
                data['jobUrl'] = response.url
                data['positionTitle'] = " ".join(response.css("#job-header h3::text").extract())
                data['jobID'] = response.url.split("/")[-1].split(".html")[0]
                data['fullJobDescription'] = " ".join(response.xpath("//h4[contains(text(),'Job Description')]/following-sibling::div[2]//text()").extract())
                data['positionDescription'] = " ".join(response.xpath("//h4[contains(text(),'Position Requirements')]/following-sibling::div[2]//text()").extract())
                data['contractType'] = response.xpath("//dt[contains(text(),'Contract Type:')]/following-sibling::dd[1]//text()").extract_first()
                data['sizeCompany'] = response.xpath("//dt[contains(text(),' Employees:')]/following-sibling::dd[1]//text()").extract_first()
                data['locationCity'] = response.xpath("//dt[contains(text(),'City:')]/following-sibling::dd[1]//text()").extract_first()
                data['locationCountry'] = response.xpath("//dt[contains(text(),'Job Location:')]/following-sibling::dd[1]//text()").extract_first()

                data['CategoryTags'] = [response.xpath("//dt[contains(text(),'Job category:')]/following-sibling::dd[1]//text()").extract_first()]
                data['salaryRange'] = response.xpath("//dt[contains(text(),'Salary:')]/following-sibling::dd[1]//text()").extract_first()

                data['companyDescription'] = " ".join(response.xpath("//h4[contains(text(),'About the Company')]/following-sibling::div[2]//text()").extract())

                data['skillsRequirements'] = " ".join(response.xpath("//h4[contains(text(),'Professional Skills')]/../text()[last()]").extract())
                data['languageSkillsRequirements'] = " ".join(response.xpath("//h4[contains(text(),'Language Skills')]/../text()[last()]").extract())

                data['minEducationRequirements'] = " ".join(response.xpath("//dt[contains(text(),'Degree:')]/following-sibling::dd[1]//text()").extract())
                data['minExperienceRequirements'] = " ".join(response.xpath("//dt[contains(text(),'Minimum years of experience:')]/following-sibling::dd[1]//text()").extract())
                data['jobLevel'] = response.xpath("//dt[contains(text(),'Career level:')]/following-sibling::dd[1]//text()").extract_first()
                data['ApplyURL'] = response.css(".apply-wrapper .btn-apply::attr(href)").extract_first()

                try:
                    data['deadlineDate'] = response.css("p:contains('Application Deadline') strong::text").extract()[-1]
                    data['deadlineDate'] = datetime.strptime(data['deadlineDate'], '%d %B %Y')
                except Exception:
                    pass
                
                data['dateScraped'] = datetime.now()

                data['minQualificationRequirements']=[]
                for exp in response.xpath("//li[contains(text(),'xperience')]"):
                    data['minQualificationRequirements'].extend([exp.xpath("text()").extract_first()])

                data['emails']=[]
                data['phones']=[]

                lc_body = ''.join(response.xpath("//body").extract()) if response.xpath("//body").extract() else None

                if lc_body:
                    lc_body = lc_body.lower()

                    lc_body = stripHTMLregex.sub("", lc_body)
                    lc_body = stripNonTelTags.sub(" ", lc_body)

                    emails = emailsregex.findall(lc_body)
                    mobiles = mobilesregex.findall(lc_body)

                    for email in emails: #fix errorneus email detection, its detecting strings such as blah@2x.jpg as emails
                                    if email.split(".")[-1] in ['jpg', 'jpeg', 'png', 'bmp']:
                                                    emails.remove(email)

                    # clean and dedupe
                    cleaned_emails = dedupeAndCleanList(emails);
                    cleaned_mobiles = dedupeAndCleanList(mobiles);
                    data['emails']=cleaned_emails
                    data['phones']=cleaned_mobiles
                    
                self.all_jobs_scraped_this_run[data['jobUrl']] = data

                yield self.all_jobs_scraped_this_run[data['jobUrl']]


	def spider_closed(self, spider):
		logging.info("Spider is closed.")
                
                headers = {
                    'Authorization': 'Splunk DB84F19F-B2F1-4B89-BB38-643DFB641B34',
                }

                response = requests.post('https://45.55.161.5:8088/services/collector/event', headers=headers, data=json.dumps(self.all_jobs_scraped_this_run), verify=False)
                
		logging.info(response.status_code)
		logging.info(response.text)
