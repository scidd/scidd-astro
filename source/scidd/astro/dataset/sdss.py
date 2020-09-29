import logging

import scidd
from scidd.core.utilities.designpatterns import singleton
from scidd.core.logger import scidd_logger as logger

from .dataset import DatasetResolverBase

logger = logging.getLogger("scidd")

@singleton
class SDSSResolver(DatasetResolverBase):
	
	@property
	def dataset(self):
		return "sdss"
	
	@property
	def releases(self):
		return ["dr16"]
