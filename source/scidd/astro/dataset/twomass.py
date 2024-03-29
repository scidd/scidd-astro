
#from .. import SciDDAstro
#from ... import exc
#from ... import SciDDFileResource

import logging

import scidd
from astropy.coordinates import SkyCoord

from scidd.core.utilities.designpatterns import singleton
from scidd.core.logger import scidd_logger as logger

from .dataset import DatasetResolverBase

logger = logging.getLogger("scidd.astro")

@singleton
class TwoMASSResolver(DatasetResolverBase):

	@property
	def dataset(self):
		return "2mass"

	@property
	def releases(self):
		return ["allsky"]

	@property
	def cachePathUniqueIdentifier(self) -> str:
		'''
		A unique identifier to include in a cache path to disambiguate files since 2MASS filenames are not unique within the data release.
		'''

		raise NotImplementedError("This code seems wrong and/or out of date.")
		# path = None
		# match = re.search("^.+;([^#]+)", self.path)
		# if match:
		# 	for key, value in [pair.split("=") for pair in match.group(1).split("?")]:
		# 		if key == "uniqueid":
		# 			path = os.path.join(ipix_path_within_cache, key)
		# 			break
		# 	assert path is not None, f"Expected to find a unique identifier for a filename, but one was not found: {sci_dd}".
		# else:
		# 	raise scidd.core.exc.UnexpectedSciDDFormatException("Format of 2MASS SciDD not as expected.")

	def uniqueIdentifierForFilename(self) -> str:
		'''
		Returns a string that can be used as a unique identifier to disambiguate files within the dataset that have the same name.

		The default returns an empty string as it is assumed filenames within a dataset are unique;
		override this method to return an identifier when this is not the case.
		'''
		# The filename should always be the last part of the URI if this is a filename, excluding any fragment.
		uri = self.scidd.split("#")[0]  # strip fragment identifier (if present)
		filename = uri.split("/")[-1]   # filename will always be the last element
		unique_id = filename.split(";")[1] # remove any unique identifier that might be present at the end of the filename

		return unique_id

		#match = re.search(";([^#].+)", self.path)
		#if match:
		##	# use this if more terms are added to the ";..." segment
		##	for key, value in [pair.split("=") for pair in match.group(1).split("?")]:
		##		if key == "uniqueid":
		##			break
		#	_,identifier = match.group(1).split("=")
		#	return identifier
		#else:
		#	raise scidd.core.exc.UnexpectedSciDDFormatException(f"Expected to find a unique identifier for a filename, but one was not found: '{sci_dd}'.")

	@property
	def position(self) -> SkyCoord:
		'''
		Returns a representative sky position for this file; this value should not be used for science.

		A file could contain data that points one or more (even hundreds of thousands) locations on the sky.
		This method effectively returns the first location found, e.g. the sky location of the reference pixel
		from the first image HDU, reading the first WCS from the file, reading known keywords, etc.
		It is intended to be used as an identifier to place the file *somewhere* on the sky (e.g. for the purposes
		of caching), but it is not intended to be exhaustive. Use traditional methods to get positions for analysis.
		Whenever possible (but not guaranteed), the value returned is in J2000 IRCS.
		'''

		if self._position is None:
			# note that the API automatically discards file compression extensions
			parameters = {
				"filename" : self.filename,
				"dataset" : self.dataset
			}

			#records = scidd.API().get(path="/astro/data/filename-search", params=parameters)
			record = scidd.API().newFilenameRequest(filename=self.filename,
													uniqueid=self.uniqueIdentifierForFilename,
													expect_one=True)

			# Expect to get back all records that match this filename (there can be many).
			# Find the matching one.
			#uniqueid = self.uniqueIdentifierForFilename
			#for rec in records:
			if True:
				sci_dd = record["scidd"]
				if uniqueid in sci_dd:
					logger.debug(record)
					pos = record["position"] # array or two points
					self._position = SkyCoord(ra=pos[0]*u.deg, dec=pos[1]*u.deg)

					# while we're here...
					if self._url is None:
						self._url = record["url"]
						self._uncompressed_file_size = record["file_size"]
					#break

			logger.debug(f"Could not find a position for '{self}'!")

		return self.position

