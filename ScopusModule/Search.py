# -*- coding: utf-8 -*-
"""
Created on Mon May 17 2021

@author: Nicolas
"""

from urllib.parse import quote_plus as url_encode
from .Utils import encodeFacets

class Search():
    # variables estáticas
    _url_base = "https://api.elsevier.com/content/search/"


    def __init__(self, url=None, query=None, facets=None, view=None , field=None, searchType=None):
        self.facets = facets
        if url and not query:
            self.url = url
        elif query and not url:
            if not searchType:
                raise ValueError('No se ha especificado el tipo de búsqueda.')
            self.url = self._url_base + searchType +'?query=' + url_encode(query)
            if view:
                self.url = self.url + '&view=' + view
            if field:
                self.url = self.url + '&field=' + field
            if facets:
                self.url = self.url + '&facets=' + facets 
        elif not url and not query:
             raise ValueError('No se ha especificado ningún URL o query.')
        else:
            raise ValueError(
                'Se ha especificado tanto el URL como la query. Solo se necesita uno.')

    def execute(self, client = None, get_all = False):
        api_response = client.exec_request(self.url)
        self.tot_num_res = int(api_response['search-results']['opensearch:totalResults'])
        print("Resultados totales:", self.tot_num_res)
        self.results = api_response['search-results']['entry']
        self.num_res = len(self.results)
        print('Resultados actuales: ', self.num_res)
        if get_all is True:
            while (self.num_res < self.tot_num_res):
                for e in api_response['search-results']['link']:
                    if e['@ref'] == 'next':
                        next_url = e['@href']
                api_response = client.exec_request(encodeFacets(next_url, self.facets))
                self.results += api_response['search-results']['entry']
                self.num_res = len(self.results)
                print('Resultados actuales: ', self.num_res)
         
