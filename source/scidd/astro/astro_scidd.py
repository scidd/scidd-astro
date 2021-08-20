
import io
import os
import re
import pdb
import json
import pathlib
from typing import Union, List

import scidd.core
import scidd.core.exc
from scidd.core import LocalAPICache
from scidd.core.logger import scidd_logger as logger
from scidd.core import SciDD, SciDDFileResource, Resolver
import astropy.units as u
from astropy.coordinates import SkyCoord

from . import SciDDAstroResolver

compression_extensions = [".zip", ".tgz", ".gz", "bz2"]

class SciDDAstro(SciDD):
	'''
	This class is wrapper around SciDD identifiers in the 'astro' namespace ("scidd:/astro").

	:param scidd: a SciDD identifier or one assumed to have "scidd:/astro" prepended to it
	:param resolver: an object that can resolve the identifier to a URL; for most cases using the default resolver by passing 'None' is the right choice
	'''
	def __init__(self, sci_dd:str=None, resolver:Resolver=None):
		if isinstance(sci_dd, SciDD):
			sci_dd = str(sci_dd)
		if not sci_dd.startswith("scidd:/astro/") and sci_dd.startswith("/"):
			# allow abbreviated form, e.g. "/data/galex/..." becomes "scidd:/astro/galex/...";
			sci_dd = "scidd:/astro" + sci_dd
		if resolver is None:
			resolver = SciDDAstroResolver.defaultResolver()

		super().__init__(sci_dd=sci_dd, resolver=resolver)

		self._datasetRelease = None # cache value -> this is in the form "dataset.release", e.g. "sdss.dr13"

	def __new__(cls, sci_dd:str=None, resolver:Resolver=None):
		'''
		If the SciDD passed into the constructor can be identified as being handled by a specialized subclass, that subclass is instantiated.
		'''
		if cls is SciDDAstro:
			# note: the walrus operation (3.8+) will be useful here

			# dataset specific classes
			match = re.search("scidd:/astro/(?P<type>data|file)/2mass/.+", str(sci_dd))
			if match:
				if match.group("type") == "data":
					return super().__new__(scidd.astro.SciDDAstroData)
				else: # type == "file"
					return super().__new__(scidd.astro.dataset.twomass.SciDDAstro2MassFile)

			# anything else that can be handled by the "generic" classes
			if str(sci_dd).startswith("scidd:/astro/data/"):
				return super().__new__(scidd.astro.SciDDAstroData)
			elif str(sci_dd).startswith("scidd:/astro/file/"):
				return super().__new__(scidd.astro.SciDDAstroFile)

		return super().__new__(cls)

	def isValid(self) -> bool:
		'''
		Performs (very!) basic validation of the syntax of the identifier.
		'''
		return self._scidd.startswith("scidd:/astro")

	@property
	def datasetRelease(self) -> str:
		'''
		Returns the short label of the dataset and the release separated by a '.', e.g. ``sdss.dr16``

		In the context of astro data SciDDs, the dataset is the first collection and the release is the first
		subcollection/path element that follows.

		The short label can be used to get the dataset object, e.g. "galex" -> Dataset.from_short_name("galex")
		'''
		if self._datasetRelease is None:
			match = re.search("^scidd:/astro/(data|file)/([^/]+)/([^/^.]+)", self.scidd)
			if match:
				self._datasetRelease = ".".join([match.group(2), match.group(3)])
		#		match = re.search("^scidd:/astro/(data|file)/([^/]+)", self.scidd)
		#		if match:
		#			self._dataset = match.group(2)
		return self._datasetRelease

	@property
	def dataset(self) -> str:
		'''
		Returns the short label of the dataset indicated in the SciDD.
		'''
		# This doesn't feel strongly robust; also maybe should not
		# name it "dataset" as it doesn't return an object as
		# it would in Trillian.
		return self.datasetRelease.split(".")[0]

	# @property
	# def release(self) -> str:
	# 	'''
	# 	The release on a `scidd:/astro/(file|data)/<dataset>/`-style id is the subcollection/path immediately following the dataset.
	# 	'''
	# 	if self._release is None:
	# 		match = re.search(
	#
	# 	return self._release


class SciDDAstroData(SciDDAstro):
	'''
	An identifier pointing to data in the astronomy namespace ("scidd:/astro/data/").
	'''
	def __init__(self, sci_dd:str=None, resolver:Resolver=None):
		if sci_dd.startswith("scidd:") and not sci_dd.startswith("scidd:/astro/data/"):
			raise scidd.core.exc.SciDDClassMismatch(f"Attempting to create {self.__class__} object with a SciDD that does not begin with 'scidd:/astro/data/'; try using the 'SciDD(sci_dd)' factory constructor instead.")
		super().__init__(sci_dd=sci_dd, resolver=resolver)

	def isFile(self) -> bool:
		''' Returns 'True' if this identifier points to a file. '''
		return False

	def isValid(self) -> bool:
		'''
		Performs basic validation of the syntax of the identifier; a returned value of 'True' does not guarantee a resource will be found.
		'''
		return self._scidd.startswith("scidd:/astro/data/")

class SciDDAstroFile(SciDDAstro, SciDDFileResource):
	'''
	An identifier pointing to a file in the astronomy namespace ("scidd:/astro/file/").

	:param sci_dd: the SciDD identifier
	:param resolver: an object that knows how to translate a SciDD into a URL that points to the specific resource
	'''

	def __init__(self, sci_dd:str=None, resolver:Resolver=None):
		SciDDAstro.__init__(self, sci_dd=sci_dd, resolver=resolver)
		SciDDFileResource.__init__(self)
		self._position = None

	# @property
	# def path_within_cache(self):
	# 	'''
	# 	The directory path within the top level SciDD cache where the file would be written when downloaded, not including filename.
	# 	'''
	# 	if self._cache_path is None:
	# 		match = re.search("^/astro/file/(.+)#?", str(pathlib.Path(self.path)))
	# 		if match:
	# 			path_components = pathlib.Path(match.group(1)).parts[0:-1] # omit last part (filename)
	# 			self._cache_path = os.path.join("astro", *path_components)
	# 		else:
	# 			raise NotImplementedError()
	# 		logger.debug(f" --> {self._cache_path}")
	# 	return self._cache_path

	def isFile(self) -> bool:
		''' Returns 'True' if this identifier points to a file. '''
		# this overrides the superclass implementation; is always True
		return True

	def isValid(self) -> bool:
		'''
		Performs basic validation of the syntax of the identifier; a returned value of 'True' does not guarantee a resource will be found.
		'''
		return self._scidd.startswith("scidd:/astro/file/")

	@property
	def filenamesUniqueInDataset(self) -> bool:
		'''
		Returns true if all filenames within the dataset this identifier belongs to are unique.

		This is generally true so 'True' is the default. It is expected that subclasses
		override this method since handling of non-unique filenames within a dataset
		will require special handling anyway.
		'''
		return True

	@property
	def filenameUniqueIdentifier(self) -> str:
		'''
		Returns a string that can be used as a unique identifier to disambiguate files that have the same name within a dataset .

		The default returns an empty string as it is assumed filenames within a dataset are unique;
		override this method to return an identifier when this is not the case.
		'''

		# example of SciDD with a filename identifier:
		# scidd:/astro/file/2mass/allsky/ji0270198.fits;uniqueid=20001017.s.27#1

		if self._filename_unique_identifier is None:
			match = re.search("^.+;([^#]+)", str(self))
			if match:
				extended_file_descriptor = match.group(1)
				d = dict([x.split("=") for x in extended_file_descriptor.split("?")])
				if "uniqueid" in d:
					self._filename_unique_identifier = d["uniqueid"]
			else:
				self._filename_unique_identifier = None


			# match = re.search("^.+;uniqueid=([^#]+)", str(self))
			# if match:
			# 	self._filename_unique_identifier = match.group(1)
			# else:
			# 	self._filename_unique_identifier = None
		return self._filename_unique_identifier

	@property
	def filename(self, without_compressed_extension:bool=True) -> str:
		'''
		If this identifier points to a file, return the filename, "None" otherwise.
		:param without_compressed_extension: if True, removes extensions indicating compression (e.g. ".zip", ".tgz", etc.)
		'''
		# The filename should always be the last part of the URI if this is a filename, excluding any fragment.
		uri = self.scidd.split("#")[0]  # strip fragment identifier (if present)
		filename = uri.split("/")[-1]   # filename will always be the last element
		filename = filename.split(";")[0] # remove any unique identifier that might be present at the end of the filename

		if without_compressed_extension:
			fname, ext = os.path.splitext(filename)
			if ext in compression_extensions:
				filename = fname
		return filename

	@property
	def url(self) -> str:
		'''
		Returns a specific location (URL) that points to the resource described by this SciDD using the :py:class:`scidd.Resolver` assigned to this object.
		'''
		if self._url is None:
			if self.resolver is None:
				raise scidd.core.exc.NoResolverAssignedException("Attempting to resolve a SciDD without having first set a resolver object.")

			self._url = self.resolver.urlForSciDD(self)
		return self._url

	@classmethod
	def fromFilename(cls, filename:str, allow_multiple_results=False) -> Union[SciDD,List[SciDD]]:
		'''
		A factory method that attempts to return a SciDD identifier from a filename alone; depends on domain-specific resolvers.

		:param filename: the filename to create a SciDD identifier from
		:param domain: the top level domain of the resource, e.g. `astro`
		:param allow_multiple_results: when True will raise an exception if the filename is not unique; if False will always return an array of matching SciDDs.
		'''
		CACHE_KEY = f"astro/filename:{filename}"

		try:
			# fetch from cache
			list_of_results = json.loads(LocalAPICache.defaultCache()[CACHE_KEY])
			logger.debug("API cache hit")
		except KeyError:
			# Use the generic filename resolver which assumes the filename is unique across all curated data.
			# If this is not the case, override this method in a subclass (e.g. see the twomass.py file).
			list_of_results = SciDDAstroResolver.defaultResolver().genericFilenameResolver(filename=filename)

			# save to cache
			try:
				LocalAPICache.defaultCache()[CACHE_KEY] = json.dumps(list_of_results)
			except Exception as e:
				raise e # remove after debugging
				logger.debug(f"Note: exception in trying to save API response to cache: {e}")
				pass

		logger.debug(f"{list_of_results=}")

		if allow_multiple_results:
			for record in list_of_results:
				s = SciDD(rec["scidd"])
				s.url = rec["url"] # since we have it here anyway
				s._uncompressed_file_size = rec["file_size"]
				s._datasetRelease = ".".join([rec["dataset"], rec["release"]]) # note there isn't a public interface for this
			return [SciDD(rec["scidd"]) for rec in list_of_results]
		else:
			if len(list_of_results) == 1:
				return SciDD(list_of_results[0]["scidd"])
			elif len(list_of_results) > 1:
				raise scidd.core.exc.UnableToResolveFilenameToSciDD(f"Multiple SciDDs were found for the filename '{filename}'. Set the flag 'allow_multiple_results' to True to return all in a list.")
			else:
				raise scidd.core.exc.UnableToResolveFilenameToSciDD(f"Could not find the filename '{filename}' in known datasets. Is the dataset one of those currently implemented?")
				# TODO: create API call to list currently implemented datasets

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
				"dataset"  : self.datasetRelease
			}

			# handle cases where filenames are not unique
			if self == "2mass":
				raise NotImplementedError("TODO: handle 'uniqueid' or whatever we land on to disambiguate filenames.")
				parameters[""] = None

			records = scidd.core.API().get(path="/astro/data/filename-search", params=parameters)

			logger.debug(records)
			if not len(records) == 1:
				raise Exception(f"Expected to find a single matching record; {len(records)} found.")

			pos = records[0]["position"] # array of two points
			self._position = SkyCoord(ra=pos[0]*u.deg, dec=pos[1]*u.deg)

			# while we have the info...
			if self._url is None:
				self._url = records[0]["url"]
				self._uncompressed_file_size = records[0]["file_size"]

		return self._position
