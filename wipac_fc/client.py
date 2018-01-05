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
    except ValueError:
        return Error(message, code)


class WFCClient:
    def __init__(self, url, port=None, use_session=False):
        """
        Initializes the client.

        If a port is specified, it is added to the `url`, e.g. `https://example.com:8080`.
        """
        self._url = url

        if port is not None:
            self._url = self._url + ':' + str(port)

        # add base api path:
        self._url = os.path.join(self._url, 'api')

        # use session?
        if use_session:
            self._r = requests.Session()
        else:
            self._r = requests

    def get_list(self, query={}, start=None, limit=None):
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

        r = self._r.get(os.path.join(self._url, 'files'), params=payload)

        if r.status_code == requests.codes.OK:
            rdict = r.json()

            return rdict
        else:
            raise error_factory(r.status_code, r.text)

    def get(self, uid):
        """
        Queries meta information for a specific file uid.
        """
        r = self._r.get(os.path.join(self._url, 'files', uid))

        if r.status_code == requests.codes.OK:
            return r.json()
        else:
            raise error_factory(r.status_code, r.text)

    def get_etag(self, uid):
        """
        Queries meta information for a specific file uid.
        """
        r = self._r.get(os.path.join(self._url, 'files', uid))

        if r.status_code == requests.codes.OK and 'etag' in r.headers:
            return r.headers['etag']
        else:
            raise Error('The server responded without an etag', -1)

    def create(self, metadata):
        """
        Tries to create a file in the file catalog.

        `metadata` must be a dictionary and needs to contain at least all mandatory fields.

        *Note*: The client does not check the metadata. Checks are entirely done by the server.
        """
        r = self._r.post(os.path.join(self._url, 'files'), json.dumps(metadata))

        if r.status_code == requests.codes.CREATED:
            # Add uid/mongo_id to cache
            rdict = r.json()
            return rdict
        elif r.status_code == requests.codes.OK:
            # Replica added
            return r.json()
        else:
            raise error_factory(r.status_code, r.text)

    def update(self, uid, metadata={}):
        """
        Updates/patches a metadata of a file.
        """
        return self._update_or_replace(uid=uid, metadata=metadata, method=self._r.patch)

    def _update_or_replace(self, uid, metadata={}, method=None):
        """
        Since `patch` and `put` have the same interface but do different things,
        we only need one method with a switch.
        """

        if not metadata:
            raise ClientError('No metadata has been passed to update file metadata')

        # TODO: Remove support for etag as they are not being used properly in the
        # patch method.
        etag = self.get_etag(uid)
        r = method(os.path.join(self._url, 'files', uid),
                   data=json.dumps(metadata),
                   headers={'If-None-Match': etag})

        if r.status_code == requests.codes.OK:
            return r.json()
        else:
            raise error_factory(r.status_code, r.text)

    def replace(self, uid, metadata={}):
        """
        Replaces the metadata of a file except for `mongo_id` and `uid`.
        """
        return self._update_or_replace(uid=uid, metadata=metadata, method=self._r.put)

    def delete(self, uid):
        """
        Deletes the metadata of a file.
        """
        r = requests.delete(os.path.join(self._url, 'files', uid))

        if r.status_code != requests.codes.NO_CONTENT:
            raise error_factory(r.status_code, r.text)
