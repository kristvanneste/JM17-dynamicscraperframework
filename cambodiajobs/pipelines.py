# -*- coding: utf-8 -*-
import re,logging,json


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
				connectDB()
				spider.cursor.execute(query, item.values())
			else:
				raise e

		return rawItem

class CambodiajobsPipeline(object):
	def process_item(self, item, spider):
            
                rawItem = item.copy()

                fieldsDumps = ["CategoryTags","minQualificationRequirements", "emails", "phones"]

                for field in fieldsDumps:
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
				connectDB()
				spider.cursor.execute(query, item.values())
			else:
				raise e

		return rawItem