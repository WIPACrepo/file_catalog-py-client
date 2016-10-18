
import requests
import json
import os

class ClientError(Exception):
    """
    Errors that occur at client side.
    """
    pass

class Error(Exception):
    """
    Errors that occur at server side.
    """
    def __init__(self, message, code, *args):
        self.message = message
        self.code = code

        # Try to decode message if it is a json string:
        try:
            self.message = json.loads(self.message)['message']
        except:
            # If it fails, just let the message as is
            pass

        super(Error, self).__init__(self.message, code, *args)

class BadRequestError(Error):
    def __init__(self, message, *args):
        super(BadRequestError, self).__init__(message, 400, *args) 

class TooManyRequestsError(Error):
    def __init__(self, message, *args):
        super(TooManyRequestsError, self).__init__(message, 429, *args) 

class UnspecificServerError(Error):
    def __init__(self, message, *args):
        super(UnspecificServerError, self).__init__(message, 500, *args) 

class ServiceUnavailableError(Error):
    def __init__(self, message, *args):
        super(ServiceUnavailableError, self).__init__(message, 503, *args) 

class ConflictError(Error):
    def __init__(self, message, *args):
        super(ConflictError, self).__init__(message, 409, *args) 

class NotFoundError(Error):
    def __init__(self, message, *args):
        super(NotFoundError, self).__init__(message, 404, *args) 

def error_factory(code, message):
    """
    Tries to find the correct `Error` class. If no class is found that corresponds to the `code`,
    it will utilize the `Error` class it self.
    """
    if 'cls' not in error_factory.__dict__ or 'codes' not in error_factory.__dict__:
        error_factory.__dict__['cls'] = Error.__subclasses__()
        error_factory.__dict__['codes'] = [c('').code for c in error_factory.__dict__['cls']]

    try:
        # `index()` throws an `ValueError` if the value isn't found
        i = error_factory.__dict__['codes'].index(code)
        return error_factory.__dict__['cls'][i](message)
    except ValueError as e:
        return Error(message, code)

class Cache:
    """
    Manages the caching for the file catalog client.

    For instance, the mongo_id/uid and the etags are cached.
    """
    def __init__(self):
        self._mongo_id = {}
        self._etag = {}

    def set_mongo_id(self, uid, mongo_id):
        self._mongo_id[uid] = mongo_id

    def get_mongo_id(self, uid):
        return self._mongo_id[uid]

    def has_mongo_id(self, uid):
        return uid in self._mongo_id

    def delete_mongo_id(self, mongo_id):
        """
        Deletes the uid/mongo_id mapping by mongo_id. If the mongo_id was not mapped, nothing happens.
        """
        uid = None
        for u, mid in self._mongo_id.iteritems():
            if mid == mongo_id:
                uid = u

        if uid is not None:
            del self._mongo_id[uid]

    def set_etag(self, mongo_id, etag):
        self._etag[mongo_id] = etag

    def get_etag(self, mongo_id):
        return self._etag[mongo_id]

    def has_etag(self, mongo_id):
        return mongo_id in self._etag

    def delete_etag(self, mongo_id):
        del self._etag[mongo_id]

    def clear_cache_by_mongo_id(self, mongo_id):
        """
        Removes everything that is connected to this mongo_id from the cache.
        If the mongo_id was not found in the cache, nothing happens.
        """
        if self.has_etag(mongo_id):
            self.delete_etag(mongo_id)

        self.delete_mongo_id(mongo_id)

class FileCatalogPyClient:
    def __init__(self, url, port = None):
        """
        Initializes the client.

        If a port is specified, it is added to the `url`, e.g. `https://example.com:8080`.
        """
        self._url = url
        self._cache = Cache()

        if port is not None:
            self._url = self._url + ':' + str(port)

        # add base api path:
        self._url = os.path.join(self._url, 'api')

    def get_file_list(self, query = {}, start = None, limit = None):
        """
        Queries the file list from the file catalog.

        This method caches the uid/mongo_id mapping in order to be able
        querying files by uid faster.
        """
        payload = {}

        if start is not None:
            payload['start'] = int(start)

        if limit is not None:
            payload['limit'] = int(limit)

        if not isinstance(query, dict):
            raise ClientError('Argument `query` must be a dict.')

        if query:
            payload['query'] = json.dumps(query)

        r = requests.get(os.path.join(self._url, 'files'), params = payload)

        if r.status_code == requests.codes.OK:
            rdict = r.json()

            for f in rdict['_embedded']['files']:
                if 'uid' in f and 'mongo_id' in f:
                    self._cache.set_mongo_id(f['uid'], f['mongo_id'])

            return rdict
        else:
            raise error_factory(r.status_code, r.text)

    def get_file(self, mongo_id = None, uid = None):
        """
        Queries meta information for a specific file. The file can be queried by `mongo_id` or by `uid`.
        
        *Note*: Since the file catalog is mongo_id based, it might need an additional query to find the uid -> mongo_id mapping.
        However, get_file_list() caches automatically all mappings that it received.
        """
        mongo_id = self._get_mongo_id(mongo_id, uid)

        # Query the data 
        r = requests.get(os.path.join(self._url, 'files', mongo_id))

        if r.status_code == requests.codes.OK:
            # Cache etag
            if 'etag' in r.headers:
                self._cache.set_etag(mongo_id, r.headers['etag'])
            else:
                raise Error('The server responded without an etag', -1)

            return r.json()
        else:
            raise error_factory(r.status_code, r.text)

    def create_file(self, metadata):
        """
        Tries to create a file in the file catalog.

        `metadata` must be a dictionary and needs to contain at least all mandatory fields.

        *Note*: The client does not check the metadata. Checks are entirely done by the server.
        *Note*: If the file has been created successfully, the new `uid`/`mongo_id` pair will be cached automatically.
        """
        r = requests.post(os.path.join(self._url, 'files'), json.dumps(metadata))

        if r.status_code == requests.codes.CREATED:
            # Add uid/mongo_id to cache
            rdict = r.json()
            self._cache.set_mongo_id(metadata['uid'], os.path.basename(rdict['file']))
            return rdict
        elif r.status_code == requests.codes.OK:
            # Replica added
            return r.json()
        else:
            raise error_factory(r.status_code, r.text)

    def update_file(self, mongo_id = None, uid = None, metadata = {}, clear_cache = False):
        """
        Updates/patches a metadata of a file.

        `clear_cache`: If set to `True` (`False` is default), it will not use the etag that is in the cache. It will query first the
        etag and will use this instead.
        """
        return self._update_or_replace_file(mongo_id = mongo_id, uid = uid, metadata = metadata, clear_cache = clear_cache, method = requests.patch)

    def _update_or_replace_file(self, mongo_id = None, uid = None, metadata = {}, clear_cache = False, method = None):
        """
        Since `patch` and `put` have the same interface but do different things, we only need one method with a switch... :)
        """
        mongo_id = self._get_mongo_id(mongo_id, uid)

        if not metadata:
            raise ClientError('No metadata has been passed to update file metadata')

        # Find etag
        # Check if the cache should be cleared:
        if clear_cache:
            self._cache.delete_etag(mongo_id)

        etag = None
        if self._cache.has_etag(mongo_id):
            etag = self._cache.get_etag(mongo_id)
        else:
            # Query etag: utilize get_file() since it caches the etag automatically
            self.get_file(mongo_id = mongo_id)
            if self._cache.has_etag(mongo_id):
                etag = self._cache.get_etag(mongo_id)
            else:
                # OK, no error has been raised (e.g. the file does not exist) but we still
                # do not know the etag. Thats odd. Abort.
                raise ClientError("Could not update file with `mongo_id` = %s because we could not find the etag" % mongo_id)

        r = method(os.path.join(self._url, 'files', mongo_id),
                           data = json.dumps(metadata),
                           headers = {'If-None-Match': etag})

        if r.status_code == requests.codes.OK:
            # Cache etag
            if 'etag' in r.headers:
                self._cache.set_etag(mongo_id, r.headers['etag'])
            else:
                raise Error('The server responded without an etag', -1)

            return r.json()
        else:
            raise error_factory(r.status_code, r.text)

    def replace_file(self, mongo_id = None, uid = None, metadata = {}, clear_cache = False):
        """
        Replaces the metadata of a file except for `mongo_id` and `uid`.
        """
        return self._update_or_replace_file(mongo_id = mongo_id, uid = uid, metadata = metadata, clear_cache = clear_cache, method = requests.put)

    def delete_file(self, mongo_id = None, uid = None):
        """
        Deletes the metadata of a file.
        """
        mongo_id = self._get_mongo_id(mongo_id, uid)

        r = requests.delete(os.path.join(self._url, 'files', mongo_id))

        if r.status_code == requests.codes.NO_CONTENT:
            self._cache.clear_cache_by_mongo_id(mongo_id)
        else:
            raise error_factory(r.status_code, r.text)

    def _get_mongo_id(self, mongo_id, uid):
        """
        Helper to support the interface of using `mongo_id` or `uid` to query/store data.
        """
        if mongo_id is None and uid is None:
            raise ClientError('You need to specify either `mongo_id` or `uid`')

        if mongo_id is not None and uid is not None:
            raise ClientError('The query is ambiguous. Do not specify `uid` and `mongo_id` at the same time.')

        # If mongo_id is given, we have anything we need to do the query

        if uid is not None:
            mongo_id = self._get_mongo_id_by_uid(uid)

        return mongo_id

    def _get_mongo_id_by_uid(self, uid):
        """
        Tries to find the `mongo_id` by only knowing the `uid`.
        """
        # Check cache first if we already know the mongo_id
        if self._cache.has_mongo_id(uid):
            # We have the uid/mongo_id pair cached. Yay!
            return self._cache.get_mongo_id(uid)
        else:
            # OK, we need to query the mongo_id
            self.get_file_list(query = {'uid': uid})

            # Since get_file_list() caches the result, we don't need to read explicitely the result
            # Check again if we know now the `uid`
            if self._cache.has_mongo_id(uid):
                # Yay!
                return self._cache.get_mongo_id(uid)
            else:
                # :'( The uid has not been found in the file catalog
                raise ClientError("The uid `%s` is not present in the file catalog" % uid)

