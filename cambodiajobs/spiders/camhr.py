# -*- coding: utf-8 -*-
from cambodiajobs.all_imports import *

class CamhrSpider(scrapy.Spider):
        
        name = "camhr"
        tbl_name = "jobs"
        
        cursor = connectDB()
        
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
        
        baseUrl = "http://camhr.com"
        
        all_jobs_in_db = {}
        all_jobs_scraped_this_run = {}
        
        cookies = {
            'USER_LANGUAGE': 'en',
            'UM_distinctid': '15f3eae95a41e-04c9973863cc4e-c303767-100200-15f3eae95a51ab',
            'JSESSIONID': 'abcE8gpbxQQS37gOn__8v',
            '__utmt': '1',
            'CNZZDATA1000045247': '1946452719-1508588705-null%7C1508588705',
            '__utma': '98086157.1179066918.1508585140.1508589429.1508589429.3',
            '__utmb': '98086157.4.10.1508590022',
            '__utmc': '98086157',
            '__utmz': '98086157.1508590022.3.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)',
        }

        headers = {
            'Origin': 'http://camhr.com',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.8',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Cache-Control': 'max-age=0',
            'Referer': 'http://camhr.com/pages/jobs/index.jsp',
            'Connection': 'keep-alive',
            'DNT': '1',
        }
        
        languageSkillsRequirements = ["language", " Mandarin "," Spanish "," English "," Hindi "," Arabic "," Portuguese "," Bengali "," Russian "," Japanese "," Punjabi "," German "," Javanese "," Wu "," Malay "," Telugu "," Vietnamese "," Korean "," French "," Marathi "," Tamil "," Urdu "," Turkish "," Italian "," Yue "," Thai "," Gujarati "," Jin "," Southern Min "," Persian "," Polish "," Pashto "," Kannada "," Xiang "," Malayalam "," Sundanese "," Hausa "," Odia "," Burmese "," Hakka "," Ukrainian "," Bhojpuri "," Tagalog "," Yoruba "," Maithili "," Uzbek "," Sindhi "," Amharic "," Fula "," Romanian "," Oromo "," Igbo "," Azerbaijani "," Awadhi "," Gan Chinese "," Cebuano "," Dutch "," Kurdish "," Serbo-Croatian "," Malagasy "," Saraiki "," Nepali "," Sinhalese "," Chittagonian "," Zhuang "," Khmer "," Turkmen "," Assamese "," Madurese "," Somali "," Marwari "," Magahi "," Haryanvi "," Hungarian "," Chhattisgarhi "," Greek "," Chewa "," Deccan "," Akan "," Kazakh "," Northern Min "," Sylheti "," Zulu "," Czech "," Kinyarwanda "," Dhundhari "," Haitian Creole "," Eastern Min "," Ilocano "," Quechua "," Kirundi "," Swedish "," Hmong "," Shona "," Uyghur "," Hiligaynon/Ilonggo "," Mossi "," Xhosa "," Belarusian "," Balochi "," Konkani "]
        minEducationRequirements = ["bachelor", "masters", "degree"]
        minExperienceRequirements = ["years of", "experience"]

        formdata = {
          'title': '',
          'industrialId': '',
          'employerId': '',
          'jobLevelId': '',
          'salaryId': '',
          'pubtimeId': '0',
          'termId': '',
          'workyears': '',
          'joblanguage': '',
          'urgent': '',
          'newJob': '',
          'categoryId': '',
          'locationId': '',
          'currentPage': '1',
          'x': '0',
          'y': '0',
        }

        def __init__(self, *args, **kwargs):
                dispatcher.connect(self.spider_closed, signals.spider_closed)
                super(CamhrSpider, self).__init__(*args, **kwargs)
                

        def start_requests(self):
            
            query = "SELECT jobID FROM `%s`" % (self.tbl_name)

            execute_query(query, self.cursor)

            for row in self.cursor.fetchall():
                    self.all_jobs_in_db[ row['jobID'] ] = ''

            yield FormRequest('%s%s'%(self.baseUrl, "/pages/jobs/"), method="post", callback=self.parse_listing_page, headers=self.headers, cookies=self.cookies, formdata=self.formdata)

        
        def parse_listing_page(self, response):
            
            for job in response.css("table.main-job-list-tab tr"):
                
                jobLink = job.css(".jobtitlelist a::attr(href)").extract_first()
                
                if jobLink is not None:
                    jobID = jobLink.split("?jobId=")[-1].split("&")[0]
                    jobLink = self.baseUrl + "/pages/jobs/" + jobLink
                    
                    if jobID not in self.all_jobs_in_db:

                        yield Request(url=jobLink, callback=self.parse_job_page, headers=self.headers)
                        
                    else:
                        logging.info("%s already exists in DB"%(jobLink))
            
            total_pages = response.xpath("//a[contains(text(),'Last Page')]/@onclick").extract_first()
            total_pages = int(filter(str.isdigit, str(total_pages)))
            
            current_page = response.css("#currentPage::attr(value)").extract_first()
            current_page = int(filter(str.isdigit, str(current_page)))
            
            if current_page <= total_pages:
                current_page = current_page + 1
                
                logging.info("Going to page %s"%(str(current_page)))
                
                self.formdata['currentPage'] = str(current_page)
                
                yield FormRequest('%s%s'%(self.baseUrl, "/pages/jobs/"), method="post", callback=self.parse_listing_page, headers=self.headers, cookies=self.cookies, formdata=self.formdata)
            
            else:
                logging.info("Scraped all pages, page %s was last page"%(str(current_page)))
                
                
        def parse_job_page(self, response):
            
            data = {}
            
            data['jobID'] = "camhr-"+response.css("#jobId::attr(value)").extract_first()
            data['source'] = self.name
            data['jobUrl'] = response.url

            data['positionTitle'] = response.css("h2.main-job-w-title::text").extract_first()

            data['companyDescription'] = "".join(a.strip() for a in response.css('.company-box.main-job.main-job-line .profile *::text').extract())
            data['companyName'] = response.css("#jobcompany::attr(value)").extract_first()
            data['companyURL'] = self.baseUrl+"/pages/employer/index.jsp?employerId="+response.css("#jobemployerId::attr(value)").extract_first()
            data['companyLogo'] = response.css("table.about-company img::attr(src)").extract_first()

            specs_tble = {}
            for specs in response.css("table.spec-tbl tr"):
                for th in specs.css("th"):
                    if th.css("p::text").extract_first() is not None:
                        specs_tble[th.css("p::text").extract_first().strip()] = "".join(a.strip() for a in th.xpath("following-sibling::td[1]//text()").extract())
            
            data['contractType'] = specs_tble['Term'] if 'Term' in specs_tble else ""
            data['numberofPositions'] = specs_tble['Hiring'] if 'Hiring' in specs_tble else ""
            data['locationCity'] = specs_tble['Location'] if 'Location' in specs_tble else ""
            data['salaryRange'] = specs_tble['Salary'] if 'Salary' in specs_tble else ""
            data['dateExpire'] = specs_tble['Closing Date'] if 'Closing Date' in specs_tble else ""
            data['dateExpire'] = datetime.strptime(data['dateExpire'], '%b-%d-%Y')

            data['dateScraped'] = datetime.now()
            data['active'] = 1 if data['dateExpire'] > data['dateScraped'] else 0

            data['languageSkillsRequirements'] = [specs_tble['Language'] if 'Language' in specs_tble else ""]
            data['minEducationRequirements'] = []
            data['minExperienceRequirements'] = []
            
            all_lines = []
            
            for table in response.xpath("//h3[contains(text(),'Job Requirements') or contains(text(),'Job Description')]/following-sibling::table[1]"):
                for td in table.css("td"):
                    all_html_in_td = td.css("*").extract_first()
                    all_lines = all_lines + all_html_in_td.split("<br>")
            for i,l in enumerate(all_lines):
                all_lines[i] = stripHTMLregex.sub('', all_lines[i])
                all_lines[i] = all_lines[i].strip().lstrip("-").strip()
            
            for li_text in all_lines:
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
                
                main_job_tab = OrderedDict()
                for specs in response.css("table.main-job-tab tr"):
                    for th in specs.css("th"):
                        if th.css("p::text").extract_first() is not None:
                            main_job_tab[th.css("p::text").extract_first().strip()] = "".join(a.strip() for a in th.xpath("following-sibling::td[1]//text()").extract())
                        
                data['companyWebsite'] = main_job_tab['Website'] if 'Website' in main_job_tab else main_job_tab['Website']

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

#            sendSplunk(self.all_jobs_scraped_this_run)

