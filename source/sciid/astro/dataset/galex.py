
import logging

import scidd
from ...utilities.designpatterns import singleton
from ...logger import scidd_logger as logger
#from ... import exc

from .dataset import DatasetResolverBase

logger = logging.getLogger("scidd")

@singleton
class GALEXResolver(DatasetResolverBase):
	
	@property
	def dataset(self):
		return "galex"
	
	@property
	def releases(self):
		return ["gr6", "gr7"]
