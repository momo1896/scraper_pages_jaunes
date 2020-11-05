# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import scrap_pagesjaunes.db_credentials as db_cred
from inex_tools.mongo.entreprise_class import EntrepriseConnection
import scrap_pagesjaunes.config as cfg
import pandas as pd


class ScrapPagesJaunesPipeline(object):
    def process_item(self, item, spider):
        """Create the company if not exist and append the data in sources"""
        try:
            # self._mongo.create_company(item['identifiant'])
            self._mongo.append_source_to_company(item['identifiant'],
                                                 cfg.SOURCE_PJ_INFO,
                                                 item['data'])
        except KeyError:
            pass

    def open_spider(self, spider):
        """Initiate db connection when spider starts running"""
        self._mongo = EntrepriseConnection()
        self._mongo.connect_to_db(
            host=db_cred.mongodb['host'],
            port=db_cred.mongodb['port'],
            db=db_cred.mongodb['database'],
            collection=db_cred.mongodb['collection'])

    def close_spider(self, spider):
        """Close db connection when spider stops running"""
        self._mongo.close_connection()
