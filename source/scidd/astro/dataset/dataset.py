
import re
import pdb
import json
from abc import ABC, ABCMeta, abstractmethod, abstractproperty

import scidd.core.exc
from scidd.core import SciDD
from scidd.core.logger import scidd_logger as logger

# class DatasetResolverBaseMeta(type):
# 	@staticmethod
# 	def resolve_filename(sci_dd) -> str:
# 		return DatasetResolverBase.resolveFilenameFromRelease(sci_dd=sci_dd, dataset=__class__._dataset, releases=__class__._releases)


class DatasetResolverBase(metaclass=ABCMeta):
	'''
	This is the base class for resolvers
	'''
	@abstractproperty
	def dataset(self):
		return self._dataset

	@abstractproperty
	def releases(self):
		return NotImplementedError("")

	def resolveURLFromSciDD(self, sci_dd:SciDD) -> str:
		#return self.resolveURLFromRelease(sci_dd=sci_dd, dataset=self.dataset, releases=self.releases)

	#def resolveURLFromRelease(self, sci_dd:SciDD=None, dataset:str=None, releases:List[str]=None) -> str:
		'''
		Given a SciDD pointing to a file, return a URL that locates the resource.

		:param sci_dd: a SciDD object
		'''
		#:param dataset: the short name of the dataset
		#:param releases: list of releases to search for the file under, or ``None`` to search across all
		#'''

		try:
			dataset, release = sci_dd.datasetRelease.split(".")
		except ValueError:
			dataset = sci_dd.datasetRelease
			release = None

		records = sci_dd.resolver.genericFilenameResolver(dataset=dataset,
														  release=release,
														  filename=sci_dd.filename,
														  uniqueid=sci_dd.filenameUniqueIdentifier)

		logger.debug(f"response: {json.dumps(records, indent=4)}\n")
		if len(records) == 1:
			url = records[0]["url"] # don't set sci_dd.url here or will infinitely recurse
			if sci_dd._url is None:
				sci_dd._url = url
			return url
		elif len(records) == 0:
			raise scidd.core.exc.UnableToResolveSciDDToURL(f"The SciDD could not be resolved to a URL (no records found): '{sci_dd}'.")
		else:
			raise scidd.core.exc.UnableToResolveSciDDToURL(f"The SciDD could not be resolved to a single URL ({len(records)} records found): '{sci_dd}'.")
