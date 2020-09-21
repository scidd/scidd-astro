
import logging

import scidd
from ...utilities.designpatterns import singleton
from ...logger import scidd_logger as logger
#from ... import exc

from .dataset import DatasetResolverBase

logger = logging.getLogger("scidd")

@singleton
class WISEResolver(DatasetResolverBase):
	
	@property
	def dataset(self):
		return "wise"
	
	@property
	def releases(self):
		return ["allsky"]
	
