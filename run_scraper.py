from scrapy.crawler import CrawlerProcess
# from scrapy.utils.project import get_project_settings
from scrapy.settings import Settings
import os
from scrap_pagesjaunes.scrap_pagesjaunes.spiders.scrap import PagesJaunesSpider
import scrap_pagesjaunes.config as cfg
import pandas as pd
import numpy as np

from inex_tools.mongo.entreprise_class import EntrepriseConnection
import scrap_pagesjaunes.db_credentials as db_cred


class RunPagesJaunes():

    def __init__(self,
                 project,
                 nace_list=None,
                 force_scrap=False):
        self._project = project
        self._nace_list = nace_list
        self._force_scrap = force_scrap

    def _connect_to_db(self):
        self._mongo = EntrepriseConnection()
        self._mongo.connect_to_db(
            host=db_cred.mongodb['host'],
            port=db_cred.mongodb['port'],
            db=db_cred.mongodb['database'],
            collection=db_cred.mongodb['collection'])

    def _extract_firms(self):
        """Extract id list from mongodb"""

        if self._force_scrap:
            self.firms = self._mongo.get_companies_by_project(
                self._project,
                self._nace_list,
                df=True)
        else:
            self.firms = self._mongo.get_companies_by_project(
                self._project,
                self._nace_list,
                exclude_sources=[cfg.SOURCE_PJ_INFO],
                df=True)

        print(self.firms)
        datas = []
        to_scrap = self._mongo.get_companies_by_id(
            self.firms['identifiant'].tolist())
        for company in to_scrap:
            try:
                data_temp = self._mongo.get_source(
                    cfg.SOURCE_PJ_LINK, company)['data']
            except TypeError:
                continue
            datas += [data_temp]
        df_to_scrap = pd.DataFrame(datas)
        print(df_to_scrap)
        self._url_list = list(df_to_scrap['link_first_result'].unique())
        self._url_list.remove(np.nan)

    def _run_scraper(self):
        # settings = get_project_settings()
        settings = Settings()
        os.environ['SCRAPY_SETTINGS_MODULE'] = \
            'scrap_pagesjaunes.scrap_pagesjaunes.settings'
        settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
        settings.setmodule(settings_module_path, priority='project')
        crawler = CrawlerProcess(settings)
        crawler.crawl(PagesJaunesSpider, input='inputargument',
                      url_list=self._url_list)
        crawler.start()

    def run_script(self):
        self._connect_to_db()
        self._extract_firms()
        self._run_scraper()
        self._mongo.close_connection()
