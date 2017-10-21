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

stripHTMLregex = re.compile(r'<.*?>|\(.*\)')
stripNonTelTags = re.compile(r'(<(?![^>]+tel:)(.|\n)*?>)')

emailsregex = re.compile('[\w\.-]+@[\w-]+\.[\w\.-]+')
mobilesregex = re.compile(r"(\(?(?<!\d)\d{3}\)?-? *\d{3}-? *-?\d{3,4})(?!\d)|(?<!\d)(\+\d{11})(?!\d)")

def connectDB():
	conn = MySQLdb.connect(user=DB_CREDS['user'], passwd=DB_CREDS['pass'], db=DB_CREDS['db'], host=DB_CREDS['host'], charset="utf8", use_unicode=True)
	cursor = MySQLdb.cursors.DictCursor(conn) 
	conn.autocommit(True)
	return cursor
    
def execute_query(query, cursor, data=[]):

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
    
    

