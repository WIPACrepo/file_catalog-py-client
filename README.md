# file_catalog-py-client
Python API to talk with the file_catalog server

## Prerequisites
To get the prerequisites necessary for the client:

    pip install requests

## Quickstart
The client is a simple python class that wraps the HTTP requests into methods.

### Get File List
In order to get the file list, just do:

    from file_catalog_py_client import filecatalogpyclient

    c = filecatalogpyclient.FileCatalogPyClient('http://localhost', 8888)
    c.get_file_list()

The parameters `start`, `limit`, and `query` are also supported:

    c.get_file_list(query = {'filesize': {'$exists': True}})
    c.get_file_list(query = {'filesize': {'$exists': True}}, start = 42, limit = 3)

### Create a New File
To create a new file (that means a new entry for the metadata for a file) one can just use the `create_file()` method.

    c.create_file({'uid': '1234', 'checksum': '3d539...f5', 'locations': ['/a/path/to/a/copy/file.dat']})

The passed dict needs to fulfill the requirements of the server.

### Get File Meta Data
The metadata for a certain file can be queried by using `get_file()`. One can either query by `uid` or `mongo_id`.

    c.get_file(uid = '1234')
    c.get_file(mongo_id = '57fd49163a7d4957ca064089')

### Delete a File

    c.delete_file()

### Update a File

    c.update_file()

### Replace a File

    c.replace_file()

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
