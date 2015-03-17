#!/usr/bin/python

import urllib2
from pyquery import PyQuery
import mysql.connector

class DB:
    def __init__(self, host = 'localhost', user = 'air', password = 'air', database = 'air'):
        self.conn = mysql.connector.connect(host = host, user = user, password = password, database = database)
    
    # sql_tuple_list: a list of tuple which has the format (sql-template, sql-parameters)
    #               which is the parameter of cursor.execute(.)
    # The method will execute the sql statements that come from the list of (sql-template, sql-parameters)
    def execute(self, sql_tuple_list):
        cursor = self.conn.cursor()
        map(lambda tup: cursor.execute(*tup), sql_tuple_list)
        self.conn.commit()
        cursor.close()
        return self;

    def close(self):
        self.conn.close()


class Extractor:
    def __init__(self, checkpoints_n = 11): # number of checkpoints
        self.html2attr= {'pjadt_location': 'checkpoint', 'pjadt_aqi': 'aqi', 'pjadt_pm25': 'pm2_5', 'pjadt_pm10': 'pm10'}
        self.checkpoints_n = checkpoints_n

    def getAttrs(self):
        return self.html2attr.values()

    def extract_elem(self, elem):
        return {value: elem.find_class(key)[0].text_content().strip().split(' ')[0] 
                for key, value in self.html2attr.iteritems()}
    
    def extract_elems(self, elems):
        return [self.extract_elem(elem) for elem in elems]
    
    def extract(self, content):
        return self.extract_elems(
                PyQuery(content)('ul.pj_area_data_details li.pj_area_data_item')[:self.checkpoints_n])


def write(data):
    attrs = Extractor().getAttrs()
    add_record = ("insert into air_quality(" + (','.join(attrs)) + ", update_time) " + 
                  "values(" + (','.join(['%s'] * len(attrs))) + ", now())");
    DB().execute([(add_record, tuple(dic[key] for key in attrs)) for dic in data]).close()
    return data 

def extract(content):
    return Extractor().extract(content)

def request(url):
    return urllib2.urlopen(url).read()

def main(url):
    print write(extract(request(url)))

if __name__ == "__main__":
    main("http://www.pm25.com/city/hangzhou.html")
