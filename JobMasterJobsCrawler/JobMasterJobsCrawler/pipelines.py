# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# JobMasterJob
import pymysql
import os
import sys
import locale
import xlwt
import codecs
import pymysql
import shutil
import pandas as pd
from xlutils.copy import copy
from xlrd import open_workbook
from twisted.enterprise import adbapi
pymysql.install_as_MySQLdb()
from JobMasterJobsCrawler import  settings
from scrapy.exceptions import DropItem


excel_file_path = "../../site_data.xls"
# excel_file_path = "../site_data.xls"


class JobmasterjobscrawlerPipeline(object):

    def __init__(self):
        self.ids_seen = set()

        self.sheet_name = 'JobMaster'  # name of the sheet for current website
        self.unsorted_temp_site_data_xls = 'unsorted_site_data.xls'  # temporary xls file which contain scraped item
        sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout)
        reload(sys)
        sys.setdefaultencoding('utf-8')

        if os.path.isfile(excel_file_path):
            """ check if site_data.xls exists we will copy old xls and append to last row"""
            self.file_exists = True
            self.read_old_book = open_workbook(excel_file_path)
            self.clone_old_book = copy(self.read_old_book)
            try:
                """ if  the self.sheet_name for the site exists in site_data.xls exists"""
                self.sheet_index = self.read_old_book.sheet_names().index(self.sheet_name)
                self.clone_old_ws = self.clone_old_book.get_sheet(self.sheet_index)
                self.next_row = self.clone_old_ws.last_used_row
                self.sheet = self.clone_old_ws
            except:
                """ the self.sheet_name  for the site not exists in site_data.xls exists we will create the sheet"""
                self.sheet = self.clone_old_book.add_sheet(self.sheet_name)
                self.sheet.write(0, 0, 'Site')
                self.sheet.write(0, 1, 'Company')
                self.sheet.write(0, 2, 'Company_jobs')
                self.sheet.write(0, 3, 'Job_id')
                self.sheet.write(0, 4, 'Job_title')
                self.sheet.write(0, 5, 'Job_Description')
                self.sheet.write(0, 6, 'Job_Post_Date')
                self.sheet.write(0, 7, 'Job_URL')
                self.sheet.write(0, 8, 'Country_Areas')
                self.sheet.write(0, 9, 'Job_categories')
                self.sheet.write(0, 10, 'AllJobs_Job_class')
                self.sheet.write(0, 11, 'unique_id')

                self.next_row = self.sheet.last_used_row

            self.book = self.clone_old_book


        else:
            """ check if site_data.xls exists we will create the excel file and add self.sheet_name of the website"""
            self.file_exists = False
            self.book = xlwt.Workbook(encoding='utf-8')
            self.sheet = self.book.add_sheet(self.sheet_name)
            self.sheet.write(0, 0, 'Site')
            self.sheet.write(0, 1, 'Company')
            self.sheet.write(0, 2, 'Company_jobs')
            self.sheet.write(0, 3, 'Job_id')
            self.sheet.write(0, 4, 'Job_title')
            self.sheet.write(0, 5, 'Job_Description')
            self.sheet.write(0, 6, 'Job_Post_Date')
            self.sheet.write(0, 7, 'Job_URL')
            self.sheet.write(0, 8, 'Country_Areas')
            self.sheet.write(0, 9, 'Job_categories')
            self.sheet.write(0, 10, 'AllJobs_Job_class')
            self.sheet.write(0, 11, 'unique_id')

            self.next_row = self.sheet.last_used_row

    def close_spider(self, spider):

        """ if site_data.xls exists"""
        if self.file_exists:
            try:
                """ Will remove old excel_file_path and
                save new file which is sorted version of unsorted_temp_site_data_xls"""

                os.remove(excel_file_path)
                unsorted_xls = open_workbook(self.unsorted_temp_site_data_xls, on_demand=True)
                sheet_name_list = unsorted_xls.sheet_names()
                writer = pd.ExcelWriter(excel_file_path)
                # writer = pd.ExcelWriter()
                for sheet_name in sheet_name_list:
                    unsorted_xls_df = pd.read_excel(self.unsorted_temp_site_data_xls, sheetname=sheet_name)
                    sorted_xls = unsorted_xls_df.sort_values(by='Company')
                    sorted_xls = sorted_xls.drop_duplicates() # remove duplicates
                    sorted_xls.to_excel(writer, sheet_name=sheet_name, index=False)
                writer.save()
                os.remove(self.unsorted_temp_site_data_xls)
            except:
                """ For any reason the pd.ExcelWriter cannot write and save the file
                    We will just move the unsorted_tem_site_data_xls to excel_file_path"""

                if not os.path.isfile(excel_file_path):
                    shutil.move(self.unsorted_temp_site_data_xls, excel_file_path)

        else:
            """ if site_data.xls doesnot exists"""
            try:
                unsorted_xls_df = pd.read_excel(self.unsorted_temp_site_data_xls)
                sorted_xls = unsorted_xls_df.sort_values(by='Company')
                sorted_xls = sorted_xls.drop_duplicates()
                sorted_xls.to_excel(excel_file_path, index=False, sheet_name=self.sheet_name)
                os.remove(self.unsorted_temp_site_data_xls)
            except:
                pass

    def process_item(self, item, spider):
        if item['JobMasterJob']['Job_id'] in self.ids_seen:
            raise DropItem("*"*100+"\n"+"Duplicate item found: %s" % item + "\n"+"*"*100)

        else:
            self.ids_seen.add(item['JobMasterJob']['Job_id'])

            self.next_row += 1
            self.sheet.write(self.next_row, 0, item['JobMasterJob']['Site'])
            self.sheet.write(self.next_row, 1, item['JobMasterJob']['Company'])
            self.sheet.write(self.next_row, 2, item['JobMasterJob']['Company_jobs'])
            self.sheet.write(self.next_row, 3, item['JobMasterJob']['Job_id'])
            self.sheet.write(self.next_row, 4, item['JobMasterJob']['Job_title'])
            self.sheet.write(self.next_row, 5, item['JobMasterJob']['Job_Description'])
            self.sheet.write(self.next_row, 6, item['JobMasterJob']['Job_Post_Date'])
            self.sheet.write(self.next_row, 7, item['JobMasterJob']['Job_URL'])
            self.sheet.write(self.next_row, 8, item['JobMasterJob']['Country_Areas'])
            self.sheet.write(self.next_row, 9, item['JobMasterJob']['Job_categories'])
            self.sheet.write(self.next_row, 10, item['JobMasterJob']['AllJobs_Job_class'])
            self.sheet.write(self.next_row, 11, item['JobMasterJob']['unique_id'])

            self.book.save(self.unsorted_temp_site_data_xls)

            return item


class MySQLPipeline(object):

    def __init__(self, dbpool):
        self.dbpool = dbpool
        self.ids_seen = set()

    @classmethod
    def from_settings(cls,settings):
        dbargs = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset='utf8',
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        return cls(dbpool)

    def process_item(self, item, spider):
        if item['JobMasterJob']['Job_id'] in self.ids_seen:

            raise DropItem("+"*100+"\n"+"Duplicate item found: %s" % item + "\n"+"+"*100)
        else:
            self.ids_seen.add(item['JobMasterJob']['Job_id'])
            dbpool = self.dbpool.runInteraction(self.insert, item, spider)
            dbpool.addErrback(self.handle_error, item, spider)
            dbpool.addBoth(lambda _: item)
            return dbpool

    def insert(self, conn, item, spider):

        conn.execute("""
            INSERT INTO sites_datas (
            Site,
            Company,
            Company_jobs,
            Job_id,
            Job_title,
            Job_Description,
            Job_Post_Date,
            Job_URL,
            Country_Areas,
            Job_categories,
            AllJobs_Job_class,
            unique_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
        """, (
            item['JobMasterJob']['Site'],
            item['JobMasterJob']['Company'],
            item['JobMasterJob']['Company_jobs'],
            item['JobMasterJob']['Job_id'],
            item['JobMasterJob']['Job_title'],
            item['JobMasterJob']['Job_Description'],
            item['JobMasterJob']['Job_Post_Date'],
            item['JobMasterJob']['Job_URL'],
            item['JobMasterJob']['Country_Areas'],
            item['JobMasterJob']['Job_categories'],
            item['JobMasterJob']['AllJobs_Job_class'],
            item['JobMasterJob']['unique_id']

        ))
        spider.log("Item stored in dbSchema: %s %r" % (item['JobMasterJob']['Job_id'], item))

    def handle_error(self, failure, item, spider):
        """Handle occurred on dbSchema interaction."""
        self.logger.info("DB Schema Handled")



