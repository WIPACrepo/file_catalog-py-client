
import requests
import json
import os

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
            raise Exception('Argument `query` must be a dict.')

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
            raise Exception(r.text)

    def get_file(self, mongo_id = None, uid = None):
        """
        Queries meta information for a specific file. The file can be queried by `mongo_id` or by `uid`.
        
        *Note*: Since the file catalog is mongo_id based, it might need an additional query to find the uid -> mongo_id mapping.
        However, get_file_list() caches automatically all mappings that it received.
        """
        if mongo_id is None and uid is None:
            raise Exception('You need to specify either `mongo_id` or `uid`')

        if mongo_id is not None and uid is not None:
            raise Exception('The query is ambiguous. Do not specify `uid` and `mongo_id` at the same time.')

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
                    raise Exception("The uid `%s` is not present in the file catalog" % uid)
        
        # Query the data 
        r = requests.get(os.path.join(self._url, 'files', mongo_id))
        return json.loads(r.text)

    def update_file(self):
        pass

    def replace_file(self):
        pass

    def delete_file(self):
        pass

