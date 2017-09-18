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


def sendSplunk(dataToSend):
    headers = {
        'Authorization': 'Splunk DB84F19F-B2F1-4B89-BB38-643DFB641B34',
    }
    
    # convert datetime objects to STR so it could successfully eb serialized into JSON
    for key, value in dataToSend.items():
        if type(value) is datetime:
            dataToSend[key] = str(value)
        if type(value) is dict:
            for k,v in value.iteritems():
                    if type(dataToSend[key][k]) is datetime:
                            dataToSend[key][k] =  str(dataToSend[key][k])
        if type(value) is list:
            for k,v in enumerate(value):
                    if type(dataToSend[key][k]) is datetime:
                            dataToSend[key][k] =  str(dataToSend[key][k])

    response = requests.post('https://45.55.161.5:8088/services/collector/event', headers=headers, data=json.dumps({"event": {"data": dataToSend}}), verify=False)

    logging.info(response.status_code)
    logging.info(response.text)
    
    
    


class SchedularSpider(scrapy.Spider):
    
    name = "schedular"
    
    cursor = connectDB()

    scrapydUrl = "http://localhost:6800"

    def __init__(self, schedular_id=None,spider_name=None, *args, **kwargs):
            super(SchedularSpider, self).__init__(*args, **kwargs)
            self.spider_name = spider_name
            self.schedular_id = schedular_id
            
            
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

            if self.spider_name is not None:
                        data = [
                          ('project', 'default'),
                          ('spider', self.spider_name),
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
                data['source'] = self.name
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
                
                sendSplunk(self.all_jobs_scraped_this_run)
                
                
                                  
class BongthomSpider(scrapy.Spider):
        
	name = "bongthom"
        tbl_name = "jobs"
	
        cursor = connectDB()

        cookies = {
            'ARRAffinity': 'b331a5556c704b9a06966143e491e99dafa498fe4238e02426b3eadb3c410eb5',
            'WAWebSiteSID': 'ad200eb7c9af481b9c7a6a429ac2dfb0',
            'BIGipServerEL_Customer_HTTP2': '!SEs8mj22vgbJdYEDa/QqeFVfB8A6Z4XZwC8fdtrwnxtpP4me4MkhN0CmPOcX5vJxJcyjnN7yIahPmw==',
            'ASPSESSIONIDQCSCBDBQ': 'DNIAMEJDGPOCPOHMLAIKCHNM',
            'ci_session': 'qdcs76lrvgd0dnahuhvqus6osp35c1eu',
            '_ga': 'GA1.2.1400748127.1503995115',
            '_gid': 'GA1.2.430404378.1505393562',
            '_gat': '1',
        }

        headers = {
            'DNT': '1',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': 'https://bongthom.com/',
            'X-Requested-With': 'XMLHttpRequest',
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

        page = 1
        
        baseUrl = "https://bongthom.com"
        
	def __init__(self, *args, **kwargs):
                dispatcher.connect(self.spider_closed, signals.spider_closed)
        	super(BongthomSpider, self).__init__(*args, **kwargs)
                

	def start_requests(self):
		query = "SELECT jobUrl FROM `%s`" % (self.tbl_name)
		
		_execute_query(query, self.cursor)

		for row in self.cursor.fetchall():
			self.all_jobs_in_db[ row['jobUrl'] ] = ''

                yield Request('%s/job_list.html?key=0.9397814781626785&get_total_page=true&page=%s'%(self.baseUrl, str(self.page)), callback=self.parse_listing_page, headers=self.headers, cookies=self.cookies)


        def parse_listing_page(self, response):

            resp = json.loads(response.body)

            if resp['jobs']:
                for comp in resp['jobs']:

                    jobLink = re.sub(r'(<([^>]+)>)', '', comp['job_title'], flags=re.IGNORECASE)
                    jobLink = re.sub(r'[^a-zA-Z0-9]', '_', jobLink)
                    jobLink = re.sub(r'_{1,}', '_', jobLink)
                    jobLink = jobLink.lower()

                    jobLink = jobLink[0:39];
                    jobLink = re.sub(r'(^_)|(_$)', '', jobLink)
                    jobLink = "view_detail" if jobLink == "" else jobLink
                    jobLink = self.baseUrl + "/" + "job_detail/" + jobLink + "_" + str(comp['job_id']) + ".html"

                    if jobLink in self.all_jobs_in_db:
                        logging.info("%s already exists in DB. So skipping..."%(jobLink))
                    else:
                        logging.info("%s "%(jobLink))
                        
                        yield Request(url=jobLink, callback=self.parse_detail_page, headers=self.headers, meta={'data': comp})

                if int(self.page) == int(resp['num_pages']):
                    logging.info("%s was last page"%(response.url))  
                else:

                    self.page = self.page + 1 
                    logging.info("Going to next page: %s"%(str(self.page)))
                    yield Request('%s/job_list.html?key=0.9397814781626785&get_total_page=true&page=%s'%(self.baseUrl, str(self.page)), callback=self.parse_listing_page, headers=self.headers, cookies=self.cookies)


                        
	def parse_detail_page(self, response):
            data = {}
                
            data['jobID'] = str(response.meta['data']['job_id'])
            data['source'] = self.name
            data['jobUrl'] = response.url
            
            data['FullJobDescription'] = " ".join(response.xpath("//h2[contains(text(),'Announcement Description')]/following-sibling::p[1]//text()").extract())
            data['companyName'] = response.meta['data']['company_en']
            data['companyLogo'] = self.baseUrl+"/clients/"+str(response.meta['data']['company_id'])+"/images/"+response.meta['data']['company_en']
            data['companyURL'] = response.xpath("//*[@class='title']/following-sibling::a[1]/@href").extract_first()
            
            data['applyEmail'] = response.xpath("//a[starts-with(@href, 'mailto')]/text()").extract_first()
            
            data['jobDateUpdated'] = response.meta['data']['submit_date']
            data['deadlineDate'] = response.meta['data']['closing_date']
            data['dateScraped'] = datetime.now()
 
            data['emails']=[]
            data['phones']=[]
            
            for phone in response.xpath("//*[@class='fa fa-phone-square']/ancestor::div[@class='hidden-xs ellipsis-text']//text()").extract():
                data['phones'].extend([phone])

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
                data['emails']= data['emails'] + cleaned_emails
                data['phones']= data['phones'] + cleaned_mobiles
            
            
            for pos in response.css("div.job-detail-pos"):
                
                position = data.copy()
                position['positionTitle'] = " ".join(a.strip() for a in pos.css("h3 *::text").extract())
                
                position['CategoryTags'] = pos.css("em::text").extract_first()
                position['CategoryTags'] = position['CategoryTags'].split(",")
                position['CategoryTags'] = [a.strip() for a in position['CategoryTags']]

                position['jobID'] = data['jobID']+str(pos.css("h3 a::attr(id)").extract_first().split("pos-")[1])
                position['locationCity'] = pos.xpath("//span[contains(text(),'Location')]/following-sibling::span[1]//text()").extract_first()
                position['contractType'] = pos.xpath("//span[contains(text(),'Schedule')]/following-sibling::span[1]//text()").extract_first()
                position['salaryRange'] = pos.xpath("//span[contains(text(),'Salary')]/following-sibling::span[1]//text()").extract_first()
                    
                self.all_jobs_scraped_this_run[position['jobID']] = position

                yield self.all_jobs_scraped_this_run[position['jobID']]



	def spider_closed(self, spider):
		logging.info("Spider is closed.")
                
                sendSplunk(self.all_jobs_scraped_this_run)
                
                