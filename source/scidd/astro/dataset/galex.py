
import logging

import scidd
from scidd.core.utilities.designpatterns import singleton
from scidd.core.logger import scidd_logger as logger
#from ... import exc

from .dataset import DatasetResolverBase

logger = logging.getLogger("scidd.astro")

@singleton
class GALEXResolver(DatasetResolverBase):

	@property
	def dataset(self):
		return "galex"

	@property
	def releases(self):
		return ["gr6", "gr7"]
