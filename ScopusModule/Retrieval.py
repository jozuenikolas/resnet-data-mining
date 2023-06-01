# -*- coding: utf-8 -*-
"""
Created on Mon May 18 2021

@author: Nicolas
"""

import requests
import json
import urllib

class AffilRetrieval():
    """Una affilliation en Scopus."""

    # variables estáticas
    _url_base = u'https://api.elsevier.com/content/affiliation/affiliation_id/'

    def __init__(self, url=None, affil_id=None, view=None, field=None):
        """Inicializa una affiliation dado la URL 
        de la affiliation o affiliation ID."""
        
        if url and not affil_id:
            self.url = url
        elif affil_id and not url:
            self.url = self._url_base + affil_id
            if view:
                self.url = self.url + '&view=' + view
            if field:
                self.url = self.url + '&field=' + field
        elif not url and not affil_id:
            raise ValueError('No se ha especificado ningún URL e ID.')
        else:
            raise ValueError(
                'Se ha especificado tanto el URL como el ID. Solo se necesita uno.')

    def execute(self, client = None):
        api_response = client.exec_request(self.url)
        self.result = api_response['affiliation-retrieval-response']
        #TODO filtrar respuesta

class AuthorRetrieval():
    """Una author en Scopus."""

    # variables estáticas
    _url_base = u'https://api.elsevier.com/content/author/author_id/'

    def __init__(self, url=None, response_list=False, author_id=None, view=None, field=None):
        """Inicializa un author dado la URL 
        del author o author ID."""
        self.response_list = response_list

        if url and not author_id:
            self.url = url
        elif author_id and not url:
            self.url = self._url_base + author_id
            if view:
                self.url = self.url + '&view=' + view
            if field:
                self.url = self.url + '&field=' + field
        elif not url and not author_id:
            raise ValueError('No se ha especificado ningún URL e ID.')
        else:
            raise ValueError(
                'Se ha especificado tanto el URL como el ID. Solo se necesita uno.')

    def execute(self, client = None):
        api_response = client.exec_request(self.url)
        if self.response_list:
            self.result = api_response['author-retrieval-response-list']['author-retrieval-response']
        else:
            self.result = api_response['author-retrieval-response']
        #TODO filtrar respuesta


class ArticleRetrieval():
    """Un Article en Scopus."""

    # variables estáticas
    _url_base = u'https://api.elsevier.com/content/abstract/scopus_id/'

    def __init__(self, url=None, scopus_id=None, view=None, field=None):
        """Inicializa un article dado la URL 
        del article o Scopus ID."""
        
        if url and not scopus_id:
            self.url = url
        elif scopus_id and not url:
            self.url = self._url_base + scopus_id
            if view:
                self.url = self.url + '&view=' + view
            if field:
                self.url = self.url + '&field=' + field
        elif not url and not scopus_id:
            raise ValueError('No se ha especificado ningún URL e ID.')
        else:
            raise ValueError(
                'Se ha especificado tanto el URL como el ID. Solo se necesita uno.')

    def execute(self, client = None):
        api_response = client.exec_request(self.url)
        self.result = api_response['abstracts-retrieval-response']
        #TODO filtrar respuesta
