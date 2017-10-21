# -*- coding: utf-8 -*-
from cambodiajobs.all_imports import *   

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

                execute_query(query, self.cursor)

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
                
    
    
