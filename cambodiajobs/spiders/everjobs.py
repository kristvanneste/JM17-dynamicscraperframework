# -*- coding: utf-8 -*-
from cambodiajobs.all_imports import *

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
		
		execute_query(query, self.cursor)
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
                
                
                                  
