
import os
import re
import pdb
import json
from typing import Dict, List, Union

import requests

import scidd.core
import scidd.core.exc
from scidd.core.cache import LocalAPICache
from scidd.core.logger import scidd_logger as logger

from .dataset.galex import GALEXResolver
from .dataset.wise import WISEResolver
from .dataset.twomass import TwoMASSResolver
from .dataset.sdss import SDSSResolver

class SciDDAstroResolver(scidd.core.Resolver):
	'''
	This resolver can translate SciDDs of the "scidd:astro" domain into URLs that point to the specific resource.
	
	This object encapsulates an external service that does the actual translation. By default the service used
	is ``api.trillianverse.org``, but any service that communicates in the same way can be used. The default is sufficient
	most of the time. An example of using an alternative is when a large number of files are locally available where
	it is preferred to use over downloading new external copies.
	
	:param scheme: scheme where the resolver service uses, e.g. "http", "https"
	:param host: the hostname of the resolver service
	:param port: the port number the resolver service is listening on
	'''
	
	def __init__(self, scheme:str="https", host:str=None, port:int=None):
		super().__init__(scheme=scheme, host=host, port=port)
		self.useCache = True
	
	@classmethod
	def defaultResolver(cls):
		'''
		This factory method returns a resolver that is pre-preconfigured with default values designed to be used out of the box (batteries included).
		
		The default service is ``https://api.trillianverse.org``.
		
		The same object will always be returned from this method (pseudo-singleton).
		'''
		if cls._default_instance is None:
			if "SCIDD_ASTRO_RESOLVER_HOST" in os.environ:
				host = os.environ["SCIDD_ASTRO_RESOLVER_HOST"]
			else:
				host = "api.trillianverse.org"
			
			if "SCIDD_ASTRO_RESOLVER_PORT" in os.environ:
				port = os.environ["SCIDD_ASTRO_RESOLVER_PORT"]
			else:
				port = 443
			cls._default_instance = cls(host=host, port=port)
		return cls._default_instance
	
	def get(self, path:str=None, params:dict={}, data:dict={}, headers:Dict[str,str]=None) -> Union[Dict,List]:
		'''
		Make a GET call on the Trillian API with the given path and parameters.
		
		:param path: the path of the API to call
		:param params: a dictionary of the parameters to pass to the API
		:param headers: any additional headers to pass to the API
		:returns: JSON response
		:raises: see: https://2.python-requests.org/en/master/api/#exceptions
		'''
		if path is None:
			raise ValueError("A path must be provided to make an API call.")
		
		with requests.Session() as http_session:
			response = http_session.get(self.base_url + path, params=params)
			logger.debug(f"params={params}")
			logger.debug(f"API request URL: '{response.url}'")
			
			try:
				response.raise_for_status()
			except requests.HTTPError as e:
				try:
					err_msg = response.json()["errors"][0]["message"]
				except:
					err_msg = response.json()
				raise scidd.core.exc.TrillianAPIException(f"An error occurred accessing the Trillian API: {err_msg}")
			
			return response.json()
	
	def urlForSciDD(self, sci_dd:scidd.core.SciDD, verify_resource=False) -> str:
		'''
		This method resolves a SciDD into a URL that can be used to retrieve the resource.
		
		:param sci_dd: a `scidd.core.SciDD` object
		:param verify_resource: verify that the resource exists at the location returned, raises `scidd.core.exc.		ResourceUnavailableWhereResolverExpected` exception if not found
		'''

		# match = re.search("^astro:/(data|file)/([^/]+)/(.+)", id_)
		# if match:
		# 	top_level = match.group(1)
		# 	dataset = match.group(2)
		# 	segment = match.group(3)
		
		if isinstance(sci_dd, scidd.astro.SciDDAstroData):
			raise NotImplementedError()
		elif isinstance(sci_dd, scidd.astro.SciDDAstroFile):
			#print(f"dataset = {sci_dd.dataset}")
			dataset = sci_dd.dataset.split(".")[0]
			if dataset == "galex":
				url = GALEXResolver().resolveURLFromSciDD(sci_dd)
			elif dataset == "wise":
				url = WISEResolver().resolveURLFromSciDD(sci_dd)
			elif dataset == "2mass":
				url = TwoMASSResolver().resolveURLFromSciDD(sci_dd)
			elif dataset == "sdss":
				url = SDSSResolver().resolveURLFromSciDD(sci_dd)
			else:
				raise NotImplementedError(f"The dataset '{sci_dd.dataset}' does not currently have a resolver associated with it.")
		else:
			raise NotImplementedError(f"Class {type(sci_dd)} not handled in {self.__class__}.")
				
		
		# if sci_dd.scidd.startswith("scidd:/astro/data/"):
		# 	url = self._url_for_astrodata(sci_dd)
		# elif sci_dd.scidd.startswith("scidd:/astro/file/"):
		# 	url = self._url_for_astrofile(sci_dd)
		
		#print(f"id={sci_dd}")
		#print(f"url={url}")
		
		if verify_resource:
			r = requests.head(url)
			if r.status_code != requests.codes.ok:
				raise scidd.core.exc.ResourceUnavailableWhereResolverExpected("The resolver returned a URL, but the resource was not found at that location.")
		return url

	def _url_for_astrofile(self, sci_dd) -> str:
		'''
		'''
		# strip leading identifier text:
		resource_path = sci_dd.scidd[len("scidd:/astro/file"):]

		raise NotImplementedError()
	
	def _url_for_astrodata(self, sci_dd) -> str:
		'''
		'''
		# strip leading identifier text:
		resource_path = sci_dd.scidd[len("scidd:/astro/data"):]
		
		raise NotImplementedError()

	def resourceForID(self, sci_dd):
		'''
		Resolve the provided "scidd:" identifier and retrieve the resource it points to.
		'''
		url = self.urlForSciDD(sci_dd)
		
		# todo: fetch data/file for URL
		
		raise NotImplementedError()

	def genericFilenameResolver(self, dataset:str=None, release:str=None, filename:str=None, uniqueid:str=None) -> List[dict]:
		'''
		This method calls the Trillian API to search for a given filename; dataset and release names are optional.
		
		:param dataset: the short name of the dataset
		:param release: the short name of the release
		:param filename: the file name
		:param uniqueid: if filenames are not unique in the dataset, this is an identifier that disambiguates the records for the filename
		'''
		if uniqueid:
			CACHE_KEY = "/".join(["astro:file", str(dataset), str(release), str(filename), str(uniqueid)])
		else:
			CACHE_KEY = "/".join(["astro:file", str(dataset), str(release), str(filename)])

		if self.useCache:
			try:
				results = json.loads(LocalAPICache.defaultCache()[CACHE_KEY])
				logger.debug("API cache hit")
				return results
			except KeyError:
				pass
		
		if ";" in filename:
			pdb.set_trace()
		
		query_parameters = { "filename" : filename }
		if dataset:
			query_parameters["dataset"] = dataset
		if release:
			query_parameters["release"] = release
		if uniqueid:
			query_parameters["uniqueid"] = uniqueid
			logger.debug(f"uniqueid={uniqueid}")
		results = self.get("/astro/data/filename-search", params=query_parameters)
		
		if self.useCache:
			try:
				LocalAPICache.defaultCache()[CACHE_KEY] = json.dumps(results)
			except Exception as e:
				raise e # remove after debugging
				logger.debug(f"Note: exception in trying to save API response to cache: {e}")
				pass
		
		return results





