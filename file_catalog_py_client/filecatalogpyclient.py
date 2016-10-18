
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

        print "Init error_factory executed"

    try:
        # `index()` throws an `ValueError` if the value isn't found
        i = error_factory.__dict__['codes'].index(code)
        return error_factory.__dict__['cls'][i](message)
    except ValueError as e:
        return Error(message, code)

class FileCatalogPyClient:
    def __init__(self, url, port = None):
        """
        Initializes the client.

        If a port is specified, it is added to the `url`, e.g. `https://example.com:8080`.
        """
        self._url = url
        self._cache = {}

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
            rdict = json.loads(r.text)

            for f in rdict['_embedded']['files']:
                if 'uid' in f and 'mongo_id' in f:
                    self._cache[f['uid']] = f['mongo_id']

            return rdict
        else:
            raise error_factory(r.status_code, r.text)

    def get_file(self, mongo_id = None, uid = None):
        """
        Queries meta information for a specific file. The file can be queried by `mongo_id` or by `uid`.
        
        *Note*: Since the file catalog is mongo_id based, it might need an additional query to find the uid -> mongo_id mapping.
        However, get_file_list() caches automatically all mappings that it received.
        """
        if mongo_id is None and uid is None:
            raise ClientError('You need to specify either `mongo_id` or `uid`')

        if mongo_id is not None and uid is not None:
            raise ClientError('The query is ambiguous. Do not specify `uid` and `mongo_id` at the same time.')

        # If mongo_id is given, we have anything we need to do the query

        if uid is not None:
            # OK, uid is given. Check cache first if we already know the mongo_id
            if uid in self._cache:
                # We have the uid/mongo_id pair cached. Yay!
                mongo_id = self._cache[uid]
            else:
                # OK, we need to query the mongo_id
                self.get_file_list(query = {'uid': uid})

                # Since get_file_list() caches the result, we don't need to read explicitely the result
                # Check again if we know now the `uid`
                if uid in self._cache:
                    # Yay!
                    mongo_id = self._cache[uid]
                else:
                    # :'( The uid has not been found in the file catalog
                    raise ClientError("The uid `%s` is not present in the file catalog" % uid)
        
        # Query the data 
        r = requests.get(os.path.join(self._url, 'files', mongo_id))

        if r.status_code == requests.codes.OK:
            return json.loads(r.text)
        else:
            raise error_factory(r.status_code, r.text)

    def create_file(self, metadata):
        """
        Tries to create a file in the file catalog.

        `metadata` must be a dictionary and needs to contain at least all mandatory fields.

        *Note*: The client does not check the metadata. Checks are entirely done by the server.
        """
        r = requests.post(os.path.join(self._url, 'files'), json.dumps(metadata))

        if r.status_code == requests.codes.CREATED:
            # Add uid/mongo_id to cache
            rdict = json.loads(r.text)
            self._cache[metadata['uid']] = os.path.basename(rdict['file'])
        else:
            raise error_factory(r.status_code, r.text)

    def update_file(self, mongo_id = None, uid = None):
        pass

    def replace_file(self, mongo_id = None, uid = None):
        pass

    def delete_file(self, mongo_id = None, uid = None):
        pass

