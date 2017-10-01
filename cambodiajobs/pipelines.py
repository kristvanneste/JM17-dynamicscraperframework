# -*- coding: utf-8 -*-
import re,logging,json

from cambodiajobs.spiders.scrapers import connectDB

class Format(object):
    def process_item(self, item, spider):
        regex = u"\s\s+|\\xa0"
        for key, value in item.items():
            if type(value) is str or type(value) is unicode:
                item[key] = value.strip()
                item[key] =  re.sub(regex,"",item[key])
            if type(value) is dict:
            	for k,v in value.iteritems():
            		if type(item[key][k]) is unicode or type(item[key][k]) is str:
	            		item[key][k] =  item[key][k].strip()
	            		item[key][k] =  re.sub(regex,"",item[key][k])
            if type(value) is list:
            	for k,v in enumerate(value):
            		if type(item[key][k]) is unicode or type(item[key][k]) is str:
	            		item[key][k] =  item[key][k].strip()
	            		item[key][k] =  re.sub(regex,"",item[key][k])
                               
        return item

class CambodiacompaniesPipeline(object):
        
	def process_item(self, item, spider):
            
                rawItem = item.copy()

                fieldsDumps = ["CompanyPhones", "CompanyFax", "CompanyTags"]

                for field in fieldsDumps:
                    if field in item:
                        item[field]=json.dumps(item[field])
            
		for column, value in item.iteritems():
			try:
				spider.cursor.execute("ALTER TABLE " +spider.tbl_name+ " ADD `"+column+"` varchar(255);")
			except Exception,e:
				if "Duplicate column name" not in str(e):
					raise e

		try:
			placeholders = ', '.join(['%s'] * len(item))
			columns = '`'+'`, `'.join(item.keys()).rstrip(' `') + '`'
			
			query = "REPLACE INTO %s ( %s ) VALUES ( %s )" % (spider.tbl_name, columns, placeholders)

			spider.cursor.execute(query, (item.values()))
		except Exception as e:
			if 'MySQL server has gone away' in str(e):
				cursor = connectDB()
				spider.cursor.execute(query, item.values())
			else:
				raise e

		return rawItem

class CambodiajobsPipeline(object):

	def process_item(self, item, spider):
            
                rawItem = item.copy()

                unique_column = "jobID"

                fieldsDumps = ["CategoryTags","minQualificationRequirements", "emails", "phones"]

                for field in fieldsDumps:
                    if field in item:
                        item[field]=json.dumps(item[field])

                # Create columns if they dont exists
		for column, value in item.iteritems():
			try:
				spider.cursor.execute("ALTER TABLE " +spider.tbl_name+ " ADD `"+column+"` varchar(255);")
			except Exception,e:
				if "Duplicate column name" not in str(e):
					raise e


		#check if record already exists
		spider.cursor.execute("SELECT * FROM " +spider.tbl_name+ " WHERE `"+unique_column+"` = %s", (item[unique_column],))
		existing = spider.cursor.fetchone()
                
                if spider.cursor.rowcount > 0:

			try:
				del item[unique_column]

				update_query = ', '.join(["`"+key+"` = %s " for key,value in item.iteritems()])

				update_query = "UPDATE " + spider.tbl_name + " SET " + update_query + " WHERE `"+unique_column+"` = %s"

				update_query_vals = list(item.values())
				update_query_vals.extend([ rawItem[unique_column] ])

				spider.cursor.execute(update_query, update_query_vals)


			except Exception as e:
				if 'MySQL server has gone away' in str(e):
					cursor = connectDB()
					spider.cursor.execute(update_query, update_query_vals)
				else:	
					raise e
		else:
                        placeholders = ', '.join(['%s'] * len(item))
                        columns = ', '.join(item.keys())
                        query = "INSERT INTO %s ( %s ) VALUES ( %s )" % (spider.tbl_name, columns, placeholders)
                        spider.cursor.execute(query, item.values())

		return rawItem