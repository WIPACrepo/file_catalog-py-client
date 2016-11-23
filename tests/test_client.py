import unittest
import hashlib
import datetime
import os
from file_catalog_py_client import filecatalogpyclient

class TestClient(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        self.client = filecatalogpyclient.FileCatalogPyClient('http://localhost', 8888)

        uid = '__test_' + str(datetime.datetime.now())
        uid2 = '__test2_' + str(datetime.datetime.now())

        self.metadata_pattern = {'uid': uid, 'checksum': self.get_checksum(), 'locations': ['/path/to/file.dat']}
        self.metadata_pattern2 = {'uid': uid2, 'checksum': self.get_checksum('3.14'), 'locations': ['/path/to/file2.dat']}

    def get_checksum(self, s = '42'):
        return hashlib.sha512(s).hexdigest()

    def check_metadata(self, expected, uid = None, mongo_id = None):
        r = self.client.get(uid = uid, mongo_id = mongo_id)

        mongo_id = r['mongo_id']

        # We don't know mongo_id and we don't know the value of _links (and we don't care)
        del r['_links']
        del r['mongo_id']
        del r['meta_modify_date']
        self.assertDictEqual(expected, r)

        return mongo_id

    def test_client(self):
        # Create metadata to test client
        print 'Create metadata'
        self.client.create(self.metadata_pattern.copy())

        # =======================================================================
        # Check if metadata exists and looks as expected
        print 'Check metadata'
        mongo_id = self.check_metadata(self.metadata_pattern, self.metadata_pattern['uid'])

        # =======================================================================
        # Check metadata and producing a ClientError
        print 'Check metadata but fail with ClientError'
        with self.assertRaises(filecatalogpyclient.ClientError) as cm:
            self.client.get(uid = 'a', mongo_id = 'b')

        self.assertEqual(cm.exception.message, 'The query is ambiguous. Do not specify `uid` and `mongo_id` at the same time.')

        # =======================================================================
        # Check metadata of a non-existing metadata set
        print 'Checking non-existing metadata'
        with self.assertRaises(filecatalogpyclient.NotFoundError) as cm:
            self.client.get(mongo_id = '8168B6493a7d4978e0e8792f')

        self.assertEqual(cm.exception.message, 'not found')

        # =======================================================================
        # Create replica
        print 'Create replica'
        replica = self.metadata_pattern.copy()
        replica['locations'] = ['http://www.example.com/test/file.txt']
        self.client.create(replica)

        # Check
        # Before we check we need to add the new location to self.metadata_pattern
        self.metadata_pattern['locations'].extend(replica['locations'])
        self.check_metadata(self.metadata_pattern, self.metadata_pattern['uid'])

        # =======================================================================
        # Try to create a duplicated uid
        # That means, that the uid is the same but the checksum is different
        print 'Try to create a uid-duplicate'
        duplicate = self.metadata_pattern.copy()
        duplicate['checksum'] = self.get_checksum('6.626E-34')
        with self.assertRaises(filecatalogpyclient.ConflictError) as cm:
            self.client.create(duplicate)

        self.assertEqual(cm.exception.message, 'conflict with existing file (uid already exists)')

        # =======================================================================
        # Update metadata
        print 'Update metadata'
        update = {'backup': True}
        updated_metadata = self.metadata_pattern.copy()
        updated_metadata.update(update)

        self.client.update(uid = self.metadata_pattern['uid'], metadata = update)
        self.check_metadata(updated_metadata, self.metadata_pattern['uid'])

        # =======================================================================
        # Update metadata (clear_cache = True)
        print 'Update metadata (clear_cache = True)'
        update = {'important': False}
        updated_metadata.update(update)

        self.client.update(uid = self.metadata_pattern['uid'], metadata = update, clear_cache = True)
        self.check_metadata(updated_metadata, self.metadata_pattern['uid'])

        # =======================================================================
        # Update including forbidden attributes
        print 'Update metadata with forbidden fields'
        with self.assertRaises(filecatalogpyclient.BadRequestError) as cm:
            self.client.update(uid = self.metadata_pattern['uid'], metadata = {'uid': 'foo'})

        self.assertEqual(cm.exception.message, 'forbidden attributes')

        # =======================================================================
        # Replace metadata with forbidden fields
        print 'Replace metadata with forbidden fields'
        replace_metadata = self.metadata_pattern2.copy()
        with self.assertRaises(filecatalogpyclient.BadRequestError) as cm:
            self.client.replace(uid = self.metadata_pattern['uid'], metadata = replace_metadata)

        self.assertEqual(cm.exception.message, 'forbidden attributes')

        # =======================================================================
        # Replace metadata correctly
        print 'Replace metadata'
        # uid is not allowed to be overridden
        del replace_metadata['uid']
        self.client.replace(uid = self.metadata_pattern['uid'], metadata = replace_metadata)

        # After replacement, we have the same uid as self.metadata_pattern (without the '2'!)
        replace_metadata['uid'] = self.metadata_pattern['uid']
        self.check_metadata(replace_metadata, self.metadata_pattern['uid'])

        # =======================================================================
        # Replacing data with missing mandatory field
        print 'Replace metadata with missing mandatory field'
        with self.assertRaises(filecatalogpyclient.BadRequestError) as cm:
            self.client.replace(uid = self.metadata_pattern['uid'], metadata = {'foo': 'bar'})

        self.assertTrue(cm.exception.message.startswith('mandatory metadata missing'))

        # =======================================================================
        # Check listing of files
        print 'List files'
        self.client.get_list()

        # =======================================================================
        # List files with error
        print 'List files with error'
        with self.assertRaises(filecatalogpyclient.BadRequestError) as cm:
            self.client.get_list(limit = -1)

        self.assertEqual(cm.exception.message, 'invalid query parameters')

        # =======================================================================
        # List files and produce a client error
        print 'List files and produce a ClientError'
        with self.assertRaises(filecatalogpyclient.ClientError) as cm:
            self.client.get_list(query = 42)

        self.assertEqual(cm.exception.message, 'Argument `query` must be a dict.')

        # =======================================================================
        # List files with query argument
        print 'List files with query argument'
        r = self.client.get_list(query = {'uid': self.metadata_pattern['uid']})

        # Since there is only one file with this uid, we know the output
        expected_output = {u'_embedded': {u'files': [{u'mongo_id': mongo_id, u'uid': self.metadata_pattern['uid']}]},
                           u'_links': {u'parent': {u'href': u'/api'}, u'self': {u'href': u'/api/files'}},
                           u'files': [os.path.join(u'/api/files', mongo_id)]}

        self.assertDictEqual(expected_output, r)

        # =======================================================================
        # =======================================================================
        # =======================================================================
        # =======================================================================

        # =======================================================================
        # Delete file
        print 'Delete metadata'
        self.client.delete(uid = self.metadata_pattern['uid'])





