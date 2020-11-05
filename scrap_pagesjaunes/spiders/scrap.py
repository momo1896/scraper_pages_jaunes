from scrapy import Spider
import scrapy_splash
# import config as cfg
import datetime
# from postal.parser import parse_address
from time import sleep


class PagesJaunesSpider(Spider):
    name = 'scrap_pagesjaunes'

    def __init__(self, *args, **kwargs):
        super(PagesJaunesSpider, self).__init__(*args, **kwargs)
        self.allowed_domains = ['pagesjaunes.fr']
        self.start_urls = kwargs.get('url_list')

    def start_requests(self):
        for i in range(len(self.start_urls)):
            yield scrapy_splash.SplashRequest(
                url=self.start_urls[i],
                callback=self.collect_data,
                dont_filter=True,
                endpoint='render.html',
                args={'wait': 4},
                meta={'url': self.start_urls[i]})

    def collect_data(self, response):
        """Collect the data"""
        url = response.url
        res = dict()
        res['url'] = url
        # res['code_ape'] = self.nace
        res['scrap_date'] = datetime.datetime.now()
        # res['id_source'] = "pages_jaunes"

        data = dict()
        data['url'] = url
        data['nom_etablissement'] = response.xpath('//*[@id="teaser-header"]'
                                                   '/div[1]/div[1]/div/div[1]'
                                                   '/h1/text()').get()
        data['activite'] = response.xpath('//*[@id="teaser-header"]/div[1]'
                                          '/div[1]/div/div[2]/div'
                                          '/span/text()').get()
        tel = response.xpath('//*[@id="teaser-footer"]/div/div'
                             '/div[1]/div/span/span[2]'
                             '/text()').get()
        if tel != '\n':
            data['telephone'] = tel

        data['adresse_complete'] = ""
        bloc_adresse = response.xpath('//*[@id="teaser-footer"]/div/div'
                                      '/div[2]/a[1]/span')
        if len(bloc_adresse) == 3:
            adresse_num = bloc_adresse[0].xpath('text()').get()
            if adresse_num is not None:
                data['adresse_num'] = adresse_num
            adresse_voie = bloc_adresse[0].xpath('span/text()').get()
            data['adresse_voie'] = adresse_voie
            adresse_cp = bloc_adresse[1].xpath('text()').get().replace(',', '')
            data['adresse_cp'] = adresse_cp
            adresse_ville = bloc_adresse[2].xpath('text()').get()
            data['adresse_ville'] = adresse_ville
            try:
                data['adresse_complete'] = adresse_num + adresse_voie
                + adresse_cp + adresse_ville
            except TypeError:
                if adresse_num is None:
                    data['adresse_complete'] = adresse_voie + adresse_cp\
                        + adresse_ville
                else:
                    pass

        print(data['adresse_complete'])

        prestation_bloc = response.xpath('//*[@id="zone-info"]'
                                         '/div[@class="zone-produits-presta'
                                         '-services-marques fd-bloc"]'
                                         '/div[1]/ul/li')
        if(len(prestation_bloc) == 0):
            prestation_bloc = response.xpath('//*[@id="zone-info"]'
                                             '/div[@class="zone-produits-'
                                             'presta-services-marques fd-bloc"'
                                             ']/div/div[1]/ul/li')
        prestations = []
        if len(prestation_bloc) != 0:
            for bloc in prestation_bloc:
                prestations.append(bloc.xpath('span/text()').get())
        data['prestations'] = prestations

        activite_bloc = response.xpath('//*[@id="zoneMultiactivite"]'
                                       '/div/ul/li')
        activites = []
        if len(activite_bloc) != 0:
            for bloc in activite_bloc:
                info = bloc.xpath('span/text()').get()
                if info is None:
                    info = bloc.xpath('a/span/text()').get()
                activites.append(info)
        data['activites'] = activites

        try:
            web = response.xpath('//*[@id="teaser-footer"]/div/div'
                                 '/div[4]/a/span[2]/text()').get()\
                                             .strip()
            if 'http' not in web:
                data['web'] = 'http://' + web
            else:
                data['web'] = web
        except AttributeError:
            pass

        boutton_insee = response.xpath('//*[@id="ancre-nav"]'
                                       '/button[@title="Aller à la partie '
                                       'Infos INSEE"]')
        boutton_insee_exist = False
        if(len(boutton_insee) == 1):
            boutton_insee_exist = True

        if boutton_insee_exist is True:
            data['siret'] = response.css('li.row.siret').xpath('span/text()')\
                .get().strip()
            res['identifiant'] = data['siret']

            employe_str = response.css('li.row.effectif_entreprise')\
                .xpath('span/text()').get()
            if r'à' in employe_str:
                data['tranche_employe_basse'] = employe_str.split()[0]
                data['tranche_employe_haute'] = employe_str.split()[2]
                data['employes'] = round(
                                        (float(data['tranche_employe_basse']) +
                                         float(data['tranche_employe_haute'])
                                         ) / 2)
            elif r'salarié' in employe_str:
                data['employes'] = int(employe_str.split()[0].strip())

            data['type_etablissement'] = response.css('li.row.siege')\
                .xpath('span/text()').get()
            try:
                data['adresse_siege'] = response.css('li.row.adresse_siege')\
                    .xpath('span/text()').get()
            except AttributeError:
                pass

            forme_juridique = response.css('li.row.forme_juridique')\
                .xpath('span/text()').get()
            if forme_juridique is not None:
                data['forme_juridique'] = forme_juridique

            data['dirigeants'] = response.css('li.row.dirigeants')\
                .xpath('div/span/text()').extract()

            ca = response.css('li.row.chiffre_affaire')\
                .xpath('span/text()').get()
            if ca is not None:
                data['ca'] = response.css('li.row.chiffre_affaire')\
                    .xpath('span/text()').get()
                if r'à' in data['ca']:
                    data['tranche_ca_basse'] = \
                        data['ca'].split()[0].replace(',', '.')\
                                             .replace(' .', '.')
                    data['tranche_ca_haute'] = \
                        data['ca'].split()[2].replace(',', '.')

                    data['ca'] = round((float(data['tranche_ca_basse']) +
                                        float(data['tranche_ca_haute'])
                                        ) / 2)

        # we suppose that if the title is missing it's
        # because page jaunes blocked us
        if data['nom_etablissement'] is None:
            sleep(15)
            print('nom_eta_none')

        else:
            for key, value in data.items():
                print(key, value)
                if isinstance(value, str):
                    data[key] = value.strip().replace('\n', '')
            res['data'] = data

        yield res
