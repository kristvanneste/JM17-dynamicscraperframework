# -*- coding: utf-8 -*-
import scrapy,logging,json,re,urllib,MySQLdb,urlparse,urllib2,requests,codecs
from scrapy.http.request import Request
from scrapy.http import FormRequest
from collections import OrderedDict

from scrapy.http import HtmlResponse

from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

from cambodiajobs.db_creds import DB_CREDS

from datetime import datetime

from elasticsearch import Elasticsearch

stripHTMLregex = re.compile(r'(<script\b[^>]*>([\s\S]*?)<\/script>)|(<style\b[^>]*>([\s\S]*?)<\/style>)')
stripNonTelTags = re.compile(r'(<(?![^>]+tel:)(.|\n)*?>)')

emailsregex = re.compile('[\w\.-]+@[\w-]+\.[\w\.-]+')
mobilesregex = re.compile(r"(\(?(?<!\d)\d{3}\)?-? *\d{3}-? *-?\d{3,4})(?!\d)|(?<!\d)(\+\d{11})(?!\d)")

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
        if item not in cleaned_list and item != "":
            cleaned_list.append(item)
    return cleaned_list


def sendSplunk(dataToSend):
    
    logging.info("sendSplunk() starts")
    headers = {
        'Authorization': 'Splunk DB84F19F-B2F1-4B89-BB38-643DFB641B34',
    }
    
    dataToSend = dataToSend.values()

    # convert datetime objects to STR so it could successfully eb serialized into JSON
    for key, value in enumerate(dataToSend):
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
    
    try:
        es = Elasticsearch(
            ['45.55.161.5'],
            port=9200,
            http_auth=('elastic', 'amsdkasdo21gsdP')
        )

        for i in dataToSend:
            res = es.index(index='jobs', doc_type='job', body=i)
            logging.info("Response from splunk")
            logging.info(res)
    except Exception,e:
        logging.error("There was some error sending data to Splunk")
        logging.error(e)
    
    logging.info("sendSplunk() ends")
    
    


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
        
        baseUrl = "https://www.everjobs.com.kh"
        searchUrl = "%s/en/jobs/?page=" % (baseUrl)
        
        minEducationRequirements = ["primary", "secondary", "high school", "associate", "bachelor", "master", "doctoral"]

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
                            
                            jobDateUpdated = str(datetime.strptime(jobDateUpdated, '%d %B %Y'))
                            
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
                data['jobID'] = response.url.split("/")[-1].split(".html")[0]
                data['active'] = 0
                
            else:

                data['active'] = 1
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

                data['minEducationRequirements'] = response.xpath("//dt[contains(text(),'Degree:')]/following-sibling::dd[1]//text()").extract()

                for p in response.css("p"):
                        p_text = " ".join(a.strip() for a in p.css("*::text").extract())
        
                        for word in self.minEducationRequirements:
                                if word.lower() in p_text.lower() and p_text not in data['minEducationRequirements']:
                                        data['minEducationRequirements'].extend([p_text])

                data['minEducationRequirements'] = ", ".join(data['minEducationRequirements'])

                data['minExperienceRequirements'] = " ".join(response.xpath("//dt[contains(text(),'Minimum years of experience:')]/following-sibling::dd[1]//text()").extract())
                data['jobLevel'] = response.xpath("//dt[contains(text(),'Career level:')]/following-sibling::dd[1]//text()").extract_first()
                data['ApplyURL'] = response.css(".apply-wrapper .btn-apply::attr(href)").extract_first()

                if data['ApplyURL']:
                        data['ApplyURL'] = self.baseUrl + data['ApplyURL']

                data['companyLogo'] = response.css("div.company-logo-wrapper img::attr(src)").extract_first()
                data['companyName'] = response.css("div.company-header-wrapper h4 a::text").extract_first()
                
                data['companyURL'] = response.xpath("//*[contains(@class, 'icon icon-home')]/ancestor::a/@href").extract_first()
                if data['companyURL'] is not None:
                    data['companyURL'] = self.baseUrl + data['companyURL']
                    
                try:
                    data['deadlineDate'] = response.css("p:contains('Application Deadline') strong::text").extract()[-1]
                    data['deadlineDate'] = datetime.strptime(data['deadlineDate'], '%d %B %Y')
                except Exception:
                    pass
                
                data['dateScraped'] = str(datetime.now())

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

                if 'companyURL' in data and data['companyURL'] is not None:
                    yield Request(url=data['companyURL'], callback=self.parse_company_page, headers=self.headers, meta={'data': data}, dont_filter=True)
                else:
                    self.all_jobs_scraped_this_run[data['jobUrl']] = data
                    yield self.all_jobs_scraped_this_run[data['jobUrl']]

        
        def parse_company_page(self, response):
            
                data = response.meta['data']
                data['companyWebsite'] = response.xpath("//dt[contains(text(),'Website')]/following-sibling::dd[1]/a/@href").extract_first()
                
                for email in response.css("a.__cf_email__"):
                    rawEmail = email.css("::attr(data-cfemail)").extract_first()
                    rawEmail = self.dcryptEmail(rawEmail)
                    if rawEmail not in data['emails']:
                        data['emails'].extend([rawEmail])
                
                lc_body = ''.join(response.xpath("//body").extract()) if response.xpath("//body").extract() else None

                if lc_body:
                        lc_body = lc_body.lower()

                        lc_body = stripHTMLregex.sub("", lc_body)
                        lc_body = stripNonTelTags.sub(" ", lc_body)

                        lc_body = re.sub(r"\s\s+", " ", lc_body)

                        emails = emailsregex.findall(lc_body)
                        phones = mobilesregex.findall(lc_body)
            
                        emails = emails + data['emails']
                        phones = phones + data['phones']

                        for email in emails: #fix errorneus email detection, its detecting strings such as blah@2x.jpg as emails
                                if email.split(".")[-1] in ['jpg', 'jpeg', 'png', 'bmp']:
                                        emails.remove(email)

                        # clean and dedupe
                        cleaned_emails = dedupeAndCleanList(emails);
                        cleaned_mobiles = dedupeAndCleanList(phones);
                        data['emails']=cleaned_emails
                        data['phones']=cleaned_mobiles

                self.all_jobs_scraped_this_run[data['jobUrl']] = data
                yield self.all_jobs_scraped_this_run[data['jobUrl']]


        def dcryptEmail(self, encryptedEmail):
            counter = 0;

            result = ''
            hexChar = int('0x' + encryptedEmail[counter:2], 16)

            counter = counter+2
            while counter < len(encryptedEmail):
                result = result + chr(int('0x' + encryptedEmail[counter:counter+2], 16) ^ hexChar)
                counter = counter+2

            return result


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
        
        hiddenWords = {}
        
	all_jobs_in_db = {}
	all_jobs_scraped_this_run = {}

        page = 1
        
        baseUrl = "https://bongthom.com"
        
        languageSkillsRequirements = ["language", " Mandarin "," Spanish "," English "," Hindi "," Arabic "," Portuguese "," Bengali "," Russian "," Japanese "," Punjabi "," German "," Javanese "," Wu "," Malay "," Telugu "," Vietnamese "," Korean "," French "," Marathi "," Tamil "," Urdu "," Turkish "," Italian "," Yue "," Thai "," Gujarati "," Jin "," Southern Min "," Persian "," Polish "," Pashto "," Kannada "," Xiang "," Malayalam "," Sundanese "," Hausa "," Odia "," Burmese "," Hakka "," Ukrainian "," Bhojpuri "," Tagalog "," Yoruba "," Maithili "," Uzbek "," Sindhi "," Amharic "," Fula "," Romanian "," Oromo "," Igbo "," Azerbaijani "," Awadhi "," Gan Chinese "," Cebuano "," Dutch "," Kurdish "," Serbo-Croatian "," Malagasy "," Saraiki "," Nepali "," Sinhalese "," Chittagonian "," Zhuang "," Khmer "," Turkmen "," Assamese "," Madurese "," Somali "," Marwari "," Magahi "," Haryanvi "," Hungarian "," Chhattisgarhi "," Greek "," Chewa "," Deccan "," Akan "," Kazakh "," Northern Min "," Sylheti "," Zulu "," Czech "," Kinyarwanda "," Dhundhari "," Haitian Creole "," Eastern Min "," Ilocano "," Quechua "," Kirundi "," Swedish "," Hmong "," Shona "," Uyghur "," Hiligaynon/Ilonggo "," Mossi "," Xhosa "," Belarusian "," Balochi "," Konkani "]
        minEducationRequirements = ["bachelor", "masters", "degree"]
        minExperienceRequirements = ["years of", "experience"]
        
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
            
            hiddenWordsCss = response.css('link[data-src="escape"]::attr("href")').extract_first()
            
            if hiddenWordsCss is not None:
         
                yield Request(url=hiddenWordsCss, callback=self.parse_hidden_wods_link, headers=self.headers, meta={'response_obj': response})
                
            else:
                logging.info("NO LINK hiddenWordsCss\n\n\n\n\n")
                self.parse_job_webpage(response)
        
        def parse_hidden_wods_link(self, response):
            
            hiddenWords = response.body
            hiddenWords = hiddenWords.split(".bte-")

            hiddenWords = hiddenWords[1:]

            hiddenWords = ".bte-" + ".bte-".join(hiddenWords)

            hiddenWords = hiddenWords.replace("::before{content:", "\": ").replace(".bte-", '"bte-').replace(";}",",").strip().strip(",")

            hiddenWords="{"+hiddenWords+"}"
            hiddenWords = json.loads(hiddenWords)
            
            response_obj = response.meta['response_obj']
            
            response_obj.meta['hiddenLinks'] = hiddenWords
            
            all_jobs_this_page = self.parse_job_webpage(response_obj)
            
            for job_id, pos in all_jobs_this_page.iteritems():
                
                # scrape company website if it is not on the job detail lpage
                if 'companyURL' in pos and ('companyWebsite' not in pos or pos['companyWebsite'] is None):
                    yield Request(url=pos['companyURL'], callback=self.parse_company_page, headers=self.headers, meta={'all_jobs_this_page': all_jobs_this_page}, dont_filter=True)
                    break # break out of here ... all jobs scraped from this job detail pge will be Yielded from parse_company_page function

                self.all_jobs_scraped_this_run[job_id] = pos
                yield self.all_jobs_scraped_this_run[job_id]

        
        def parse_company_page(self, response):
            
            all_jobs_this_page = response.meta['all_jobs_this_page']
            
            for job_id, pos in all_jobs_this_page.iteritems():
                pos['companyWebsite'] = response.css("div strong:contains('Website') a::attr(href)").extract_first()

                self.all_jobs_scraped_this_run[job_id] = pos
                yield self.all_jobs_scraped_this_run[job_id]
        
        def parse_job_webpage(self, response):
            
            response_body = "".join(a for a in response.css("body").extract()).replace(u"\u2018", "'").replace(u"\u2019", "'")
            
            logging.info("URL: "+response.url)
            if 'hiddenLinks' in response.meta:
                hiddenLinks = response.meta['hiddenLinks']

                for cssClass, relacementChar in hiddenLinks.iteritems():

                    sub_res = re.subn(u"(<span class=\"\w+\s*"+cssClass+"\">.*?</span>)", \
                                        " "+relacementChar+" ", \
                                                response_body)
                    if sub_res[1] > 0:
                        response_body = sub_res[0]
                        response_body = re.sub(r"\s\s+", ' ', response_body)
                
            response_body = HtmlResponse(url=response.url, body=response_body, encoding='utf-8')

            all_jobs_this_page = {}
            
            data = {}
            
            data['jobID'] = str(response.meta['data']['job_id'])
            data['source'] = self.name
            data['jobUrl'] = response.url

            data['companyDescription'] = " ".join(response_body.xpath("//h2[contains(text(),'Announcement Description')]/following-sibling::p[1]//text()").extract())
            data['companyName'] = response.meta['data']['company_en']

            if "company_logo" in response.meta['data'] and response.meta['data']['company_logo'] is not None:
                data['companyLogo'] = self.baseUrl+"/clients/"+str(response.meta['data']['company_id'])+"/images/"+response.meta['data']['company_logo']
            
            data['companyURL'] = response_body.xpath("//*[@class='title']/following-sibling::a[1]/@href").extract_first()
            data['companyWebsite'] = response_body.xpath("//strong[contains(text(),'Website')]/following-sibling::div[1]/a/@href").extract_first()
            
            data['applyEmail'] = response_body.xpath("//a[starts-with(@href, 'mailto')]/text()").extract_first()

            data['jobDateUpdated'] = response.meta['data']['submit_date']
            data['deadlineDate'] = response.meta['data']['closing_date']
            data['deadlineDate'] = datetime.strptime(data['deadlineDate'], '%Y-%m-%d %H:%M:%S')

            data['dateScraped'] = datetime.now()

            data['active'] = 1 if data['deadlineDate'] > data['dateScraped'] else 0

            data['emails']=[]
            data['phones']=[]

            for phone in response_body.xpath("//*[@class='fa fa-phone-square']/ancestor::div[@class='hidden-xs ellipsis-text']//text()").extract():
                phone = phone.strip()
                if phone != "" and phone not in data['phones']:
                    data['phones'].extend([phone])

            lc_body = ''.join(response_body.xpath("//body").extract()) if response_body.xpath("//body").extract() else None

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


            for pos in response_body.css("div.job-detail-pos"):

                position = data.copy()
                position['languageSkillsRequirements'] = []
                position['minEducationRequirements'] = []
                position['minExperienceRequirements'] = []

                for li in pos.css("ul.job-detail-req li"):
                    li_text =  " ".join(a.strip() for a in li.css("*").extract())
                    li_text = re.findall(r"(<li>.*?</li>)", li_text)[0]
                                
                    li_text = re.sub(u'<.*?>', '', li_text)

                    for word in self.languageSkillsRequirements:
                        if word.lower() in li_text.lower():
                            position['languageSkillsRequirements'].extend([li_text])

                    for word in self.minEducationRequirements:
                        if word.lower() in li_text.lower():
                            position['minEducationRequirements'].extend([li_text])

                    for word in self.minExperienceRequirements:
                        if word.lower() in li_text.lower():
                            position['minExperienceRequirements'].extend([li_text])

                position['languageSkillsRequirements'] = ",".join(position['languageSkillsRequirements'])
                position['minEducationRequirements'] = ",".join(position['minEducationRequirements'])
                position['minExperienceRequirements'] = ",".join(position['minExperienceRequirements'])

                position['positionTitle'] = " ".join(a.strip() for a in pos.css("h3 *::text").extract())

                position['CategoryTags'] = pos.css("em::text").extract_first()
                position['CategoryTags'] = position['CategoryTags'].split(",")
                position['CategoryTags'] = [a.strip() for a in position['CategoryTags']]

                position['fullJobDescription'] = ""
                duties = pos.xpath("h4[contains(text(),'Duties')]/following-sibling::ul").extract_first()
                if duties is not None and duties != "":
                        position['fullJobDescription'] += "Duties: "+duties
                
                req = pos.xpath("h4[contains(text(),'Requirements')]/following-sibling::ul").extract_first()
                if req is not None and req != "":
                        position['fullJobDescription'] += "Requirements: "+req

                position['jobID'] = data['jobID']+str(pos.css("h3 a::attr(id)").extract_first().split("pos-")[1])
                position['locationCity'] = pos.xpath("//span[contains(text(),'Location')]/following-sibling::span[1]//text()").extract_first()
                position['contractType'] = pos.xpath("//span[contains(text(),'Schedule')]/following-sibling::span[1]//text()").extract_first()
                position['salaryRange'] = pos.xpath("//span[contains(text(),'Salary')]/following-sibling::span[1]//text()").extract_first()
                
                all_jobs_this_page[position['jobID']] = position
            
            return all_jobs_this_page


	def spider_closed(self, spider):
            logging.info("Spider is closed.")

            sendSplunk(self.all_jobs_scraped_this_run)

            
                                  
class PelprekSpider(scrapy.Spider):
        
        name = "pelprek"
        tbl_name = "jobs"
        
        cursor = connectDB()

        cookies = {
            'laravel_session': 'eyJpdiI6ImpoRmxrcmI3blVrYWZXUlN6ZlJIY3c9PSIsInZhbHVlIjoibEtndnVIME1EazAzQ3hTU3VLWWVVTHVCWTZjUG43OStSaURpQm1WTEhJMnhQbGxpRE9IK0ZlWDhOa1VDMkpQbkI0eThhNThmRVRxUFdtUTBnMTFGUFE9PSIsIm1hYyI6ImE2YTc2MzIyOTEzZjY3NDAzOGNjODY4M2QyYjYyMWZiNjNkNjA4MmZmNzZiMjUxNzBjMDhkMWRjODcyYWZkZmYifQ%3D%3D',
            '__asc': '1e43d15615ef244b1dd5272bf8c',
            '__auc': '98e3ae7215ee6e3a716d3eefb73',
            '_ga': 'GA1.2.1730988070.1507112232',
            '_gid': 'GA1.2.90411606.1507303142',
        }

        headers = {
            'DNT': '1',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.8',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'http://www.pelprek.com/?page=2',
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
                        'cambodiajobs.pipelines.CambodiajobsPipeline': 200,
                }
        }
        
        hiddenWords = {}
        
        all_jobs_in_db = {}
        all_jobs_scraped_this_run = {}

        page = 1
        
        baseUrl = "http://www.pelprek.com"
        
        languageSkillsRequirements = ["language", " Mandarin "," Spanish "," English "," Hindi "," Arabic "," Portuguese "," Bengali "," Russian "," Japanese "," Punjabi "," German "," Javanese "," Wu "," Malay "," Telugu "," Vietnamese "," Korean "," French "," Marathi "," Tamil "," Urdu "," Turkish "," Italian "," Yue "," Thai "," Gujarati "," Jin "," Southern Min "," Persian "," Polish "," Pashto "," Kannada "," Xiang "," Malayalam "," Sundanese "," Hausa "," Odia "," Burmese "," Hakka "," Ukrainian "," Bhojpuri "," Tagalog "," Yoruba "," Maithili "," Uzbek "," Sindhi "," Amharic "," Fula "," Romanian "," Oromo "," Igbo "," Azerbaijani "," Awadhi "," Gan Chinese "," Cebuano "," Dutch "," Kurdish "," Serbo-Croatian "," Malagasy "," Saraiki "," Nepali "," Sinhalese "," Chittagonian "," Zhuang "," Khmer "," Turkmen "," Assamese "," Madurese "," Somali "," Marwari "," Magahi "," Haryanvi "," Hungarian "," Chhattisgarhi "," Greek "," Chewa "," Deccan "," Akan "," Kazakh "," Northern Min "," Sylheti "," Zulu "," Czech "," Kinyarwanda "," Dhundhari "," Haitian Creole "," Eastern Min "," Ilocano "," Quechua "," Kirundi "," Swedish "," Hmong "," Shona "," Uyghur "," Hiligaynon/Ilonggo "," Mossi "," Xhosa "," Belarusian "," Balochi "," Konkani "]
        minEducationRequirements = ["bachelor", "masters", "degree"]
        minExperienceRequirements = ["years of", "experience"]
        
        def __init__(self, *args, **kwargs):
                dispatcher.connect(self.spider_closed, signals.spider_closed)
                super(PelprekSpider, self).__init__(*args, **kwargs)
                

        def start_requests(self):
            
            query = "SELECT jobUrl FROM `%s`" % (self.tbl_name)

            _execute_query(query, self.cursor)

            for row in self.cursor.fetchall():
                    self.all_jobs_in_db[ row['jobUrl'] ] = ''

            yield Request('%s/?page=%s'%(self.baseUrl, str(self.page)), callback=self.parse_listing_page, headers=self.headers, cookies=self.cookies)


        def parse_listing_page(self, response):

                jobs_selector = "#list_jobs .job-list-container > div"

                if response.css(jobs_selector).extract_first() is not None:
                        for job in response.css(jobs_selector):

                            jobLink = job.css(".title_job::attr(href)").extract_first()

                            if jobLink in self.all_jobs_in_db:
                                logging.info("%s already exists in DB. So skipping..."%(jobLink))
                            else:
                                logging.info("%s "%(jobLink))

                                yield Request(url=jobLink, callback=self.parse_detail_page, headers=self.headers)
                            
                        self.page = self.page + 1 
                        url = '%s/?page=%s'%(self.baseUrl, str(self.page))
                        logging.info("\n\n\nGoing to next page: %s"%(url))
                        yield Request(url, callback=self.parse_listing_page, headers=self.headers, cookies=self.cookies)

                else:
                        logging.info("%s was last page"%(response.url))

                        
        def parse_detail_page(self, response):

                data = {}
            
                data['jobID'] = response.url.split("/")[-2]
                data['source'] = self.name
                data['jobUrl'] = response.url

                data['companyDescription'] = response.css('[name="description"]::attr(content)').extract_first()
                data['companyName'] = response.css("a:contains('More about')::text").extract_first()
                data['companyURL'] = response.css("a:contains('More about')::attr(href)").extract_first()

                if data['companyName'] is not None:
                        data['companyName'] = data['companyName'].strip().replace("> More about ", "")

                data['companyLogo'] = response.css('[property="og:image"]::attr(content)').extract_first()

                data['positionTitle'] = " ".join(a.strip() for a in response.css(".title_job_detaill *::text").extract())
                
                data['contractType'] = "".join(a.strip() for a in response.xpath("//b[contains(text(),'Job Type:')]/parent::div//text()").extract())
                data['contractType']=data['contractType'].replace("Job Type:","")

                data['numberofPositions'] = "".join(a.strip() for a in response.xpath("//b[contains(text(),'Hiring:')]/parent::div//text()").extract())
                data['numberofPositions']=data['numberofPositions'].replace("Hiring:","")

                data['locationCity'] = "".join(a.strip() for a in response.xpath("//b[contains(text(),'Job location:')]/parent::div//text()").extract())
                data['locationCity']=data['locationCity'].replace("Job location:","")
           
                data['salaryRange'] = "".join(a.strip() for a in response.xpath("//b[contains(text(),'Salary range:')]/parent::div//text()").extract())
                data['salaryRange']=data['salaryRange'].replace("Salary range:","")
    
                data['jobDateUpdated'] = "".join(a.strip() for a in response.xpath("//b[contains(text(),'Posting Date:')]/parent::div//text()").extract())
                data['jobDateUpdated']=data['jobDateUpdated'].strip().replace("Posting Date:","").strip()
                data['jobDateUpdated'] = datetime.strptime(data['jobDateUpdated'], '%d %b, %Y')

                data['dateExpire'] = "".join(a.strip() for a in response.xpath("//b[contains(text(),'Closing Date:')]/parent::div//text()").extract())
                data['dateExpire']=data['dateExpire'].strip().replace("Closing Date:","").strip()
                data['dateExpire'] = datetime.strptime(data['dateExpire'], '%d %b, %Y')

                data['dateScraped'] = datetime.now()
                data['active'] = 1 if data['dateExpire'] > data['dateScraped'] else 0

                data['skillsRequirements'] = " ".join(response.xpath("//h4[contains(text(),'Professional Skills')]/../text()[last()]").extract())
                data['languageSkillsRequirements'] = " ".join(response.xpath("//h4[contains(text(),'Language Skills')]/../text()[last()]").extract())

                data['languageSkillsRequirements'] = []
                data['minEducationRequirements'] = []
                data['minExperienceRequirements'] = []

                for li in response.css("li"):
                    li_text =  " ".join(a.strip() for a in li.css("*").extract())
                                
                    li_text = re.sub(u'<.*?>', '', li_text)

                    for word in self.languageSkillsRequirements:
                        if word.lower() in li_text.lower():
                            data['languageSkillsRequirements'].extend([li_text])

                    for word in self.minEducationRequirements:
                        if word.lower() in li_text.lower():
                            data['minEducationRequirements'].extend([li_text])

                    for word in self.minExperienceRequirements:
                        if word.lower() in li_text.lower():
                            data['minExperienceRequirements'].extend([li_text])

                data['languageSkillsRequirements'] = ",".join(data['languageSkillsRequirements'])
                data['minEducationRequirements'] = ",".join(data['minEducationRequirements'])
                data['minExperienceRequirements'] = ",".join(data['minExperienceRequirements'])

                data['ApplyURL'] = response.css(":contains('Apply Now')::attr(href)").extract_first()

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

               
                if 'companyURL' in data and data['companyURL'] is not None:
                    yield Request(url=data['companyURL'], callback=self.parse_company_page, headers=self.headers, meta={'data': data}, dont_filter=True)
                else:
                    self.all_jobs_scraped_this_run[data['jobUrl']] = data
                    yield self.all_jobs_scraped_this_run[data['jobUrl']]

        
        def parse_company_page(self, response):
            
                data = response.meta['data']
                data['companyWebsite'] = response.xpath("//h2[contains(text(),'Website')]/following-sibling::span/a/@href").extract_first()

                lc_body = ''.join(response.xpath("//body").extract()) if response.xpath("//body").extract() else None

                if lc_body:
                        lc_body = lc_body.lower()

                        lc_body = stripHTMLregex.sub("", lc_body)
                        lc_body = stripNonTelTags.sub(" ", lc_body)

                        lc_body = re.sub(r"\s\s+", " ", lc_body)

                        emails = emailsregex.findall(lc_body)
                        phones = mobilesregex.findall(lc_body)
            
                        emails = emails + data['emails']
                        phones = phones + data['phones']

                        for email in emails: #fix errorneus email detection, its detecting strings such as blah@2x.jpg as emails
                                if email.split(".")[-1] in ['jpg', 'jpeg', 'png', 'bmp']:
                                        emails.remove(email)

                        # clean and dedupe
                        cleaned_emails = dedupeAndCleanList(emails);
                        cleaned_mobiles = dedupeAndCleanList(phones);
                        data['emails']=cleaned_emails
                        data['phones']=cleaned_mobiles

                self.all_jobs_scraped_this_run[data['jobUrl']] = data
                yield self.all_jobs_scraped_this_run[data['jobUrl']]


        def spider_closed(self, spider):
            logging.info("Spider is closed.")

            sendSplunk(self.all_jobs_scraped_this_run)

