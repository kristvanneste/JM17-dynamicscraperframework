# -*- coding: utf-8 -*-
from cambodiajobs.all_imports import *

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

            execute_query(query, self.cursor)

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

