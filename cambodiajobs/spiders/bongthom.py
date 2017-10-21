# -*- coding: utf-8 -*-
from cambodiajobs.all_imports import *

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

            execute_query(query, self.cursor)

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

            
                                  
