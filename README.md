# file_catalog-py-client
Python API to talk with the file_catalog server

## Prerequisites
To get the prerequisites necessary for the client:

    pip install requests

## Running the Unit Tests
To run the unit tests, you must started the file_catalog server at `localhost:8888` (or you change the address in in the test set up).
Run the tests:

    python -m unittest discover

## Quickstart
The client is a simple python class that wraps the HTTP requests into methods.

### Get File List
In order to get the file list, just do:

    from file_catalog_py_client import filecatalogpyclient

    c = filecatalogpyclient.FileCatalogPyClient('http://localhost', 8888)
    
    # If you want to reuse the same connection, do:
    # c = filecatalogpyclient.FileCatalogPyClient('http://localhost', 8888, use_session = True)
    
    c.get_list()

The parameters `start`, `limit`, and `query` are also supported:

    c.get_list(query = {'filesize': {'$exists': True}})
    c.get_list(query = {'filesize': {'$exists': True}}, start = 42, limit = 3)

### Create a New File
To create a new file (that means a new entry for the metadata for a file) one can just use the `create()` method.

    c.create({'uid': '1234', 'checksum': '3d539...f5', 'locations': ['/a/path/to/a/copy/file.dat']})

The passed dict needs to fulfill the requirements of the server.

### Get File Meta Data
The metadata for a certain file can be queried by using `get()`. One can either query by `uid` or `mongo_id`.

    c.get(uid = '1234')
    c.get(mongo_id = '57fd49163a7d4957ca064089')

    # This will fail: either uid or mongo_id can be used
    c.get(uid = 'file uid', mongo_id = '57fd49163a7d4957ca064089')

### Delete a File
To delete the metadata of a file, use `delete()`:

    c.delete(uid = '1234')
    c.delete(mongo_id = '57fd49163a7d4957ca064089')

### Update a File
In order to update a file, `update()` can be used. One can use `mongo_id` or `uid` as identifier. `update()` utilizes the cache to find the `etag`. If no `etag` has been cached for this file, it queries the `etag` prior the update.

**Note:** If you want to ignore the cached `etag`, use `clear_cache = True`.

    c.update(uid = '1234', metadata = {'backupd': True})
    c.update(mongo_id = '57fd49163a7d4957ca064089', metadata = {'backupd': True})

### Replace a File
Replacing the metadata of a file is pretty similar to updating it. The difference is that any key that is not passed via the `metadata` will be deleted. Therefore, be sure to add the mandatory fields except for the `uid` and `mongo_id` since they cannot be changed.

**Note:** If you want to ignore the cached `etag`, use `clear_cache = True`.

    c.replace(uid = '1234', metadata = {'checksum': '3d539...f5', 'locations': ['/a/path/to/a/copy/file.dat'], 'backup': False})
    c.replace(mongo_id = '57fd49163a7d4957ca064089', metadata = {'checksum': '3d539...f5', 'locations': ['/a/path/to/a/copy/file.dat'], 'backup': False})

## Errors
There are two types of errors: client side errors and server side errors. Client side errors are instances of `filecatalogpyclient.ClientError`. Server side errors are instances of `filecatalogpyclient.Error`.

### `filecatalogpyclient.Error`
`filecatalogpyclient.Error` has the attributes `code` and `message`. `code` contains the status code with which the server responded. `message` stores the message that is returned by the server.

**Note:** The server usually responds with a JSON string and the error message has the key `message`. Therefore, the class tries to extract that message and tries to store only the message in the attribute `message`.

Subclasses of `filecatalogpyclient.Error`:
* `BadRequestError`: status code 400
* `NotFoundError`: status code 404
* `ConflictError`: status code 409
* `TooManyRequestsError`: status code 429
* `UnspecificServerError`: status code 500
* `ServiceUnavailableError`: status code 503
