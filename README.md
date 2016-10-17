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
    c.get_file_list(query = {'filesize': {'$exists': True}}, start = 42, limit 3)

### Get File Meta Data

    c.get_file()

### Delete a File

    c.delete_file()

### Update a File

    c.update_file()

### Replace a File

    c.replace_file()
