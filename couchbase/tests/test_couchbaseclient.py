#
# Copyright 2012, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest
import uuid
import time
from testconfig import config
from couchbase.couchbaseclient import *
from couchbase.exception import *


class CouchbaseClientTest(unittest.TestCase):
    def setUp(self):
        self.url = config['node-1']['url']
        self.bucket = config['node-1']['bucket']
        self.client = CouchbaseClient(self.url, self.bucket, "", True)

    def tearDown(self):
        self.client.flush()
        self.client.done()

    def test_simple_add(self):
        self.client.add('key', 0, 0, 'value')
        self.assertTrue(self.client.get('key')[2] == 'value')

    def test_simple_append(self):
        self.client.set('key', 0, 0, 'value')
        self.client.append('key', 'appended')
        self.assertTrue(self.client.get('key')[2] == 'valueappended')

    def test_simple_delete(self):
        self.client.set('key', 0, 0, 'value')
        self.client.delete('key')

    def test_simple_decr(self):
        self.client.set('key', 0, 0, '4')
        self.client.decr('key', 1)
        self.assertTrue(self.client.get('key')[2] == '3')

    def test_simple_incr(self):
        self.client.set('key', 0, 0, '1')
        self.client.incr('key', 1)
        self.assertTrue(self.client.get('key')[2] == '2')

    def test_simple_get(self):
        try:
            self.client.get('key')
            raise Exception('Key existed that should not have')
        except MemcachedError as e:
            if e.status != 1:
                raise e
        self.client.set('key', 0, 0, 'value')
        self.assertTrue(self.client.get('key')[2] == 'value')

    def test_simple_prepend(self):
        self.client.set('key', 0, 0, 'value')
        self.client.prepend('key', 'prepend')
        self.assertTrue(self.client.get('key')[2] == 'prependvalue')

    def test_simple_replace(self):
        self.client.set('key', 0, 0, 'value')
        self.client.replace('key', 0, 0, 'replaced')
        self.assertTrue(self.client.get('key')[2] == 'replaced')

    def test_simple_touch(self):
        self.client.set('key', 2, 0, 'value')
        self.client.touch('key', 5)
        time.sleep(3)
        self.assertTrue(self.client.get('key')[2] == 'value')

    def test_set_and_get(self):
        kvs = [(str(uuid.uuid4()), str(uuid.uuid4())) for i in range(0, 100)]
        for k, v in kvs:
            self.client.set(k, 0, 0, v)

        for k, v in kvs:
            self.client.get(k)

    def test_set_and_delete(self):
        kvs = [(str(uuid.uuid4()), str(uuid.uuid4())) for i in range(0, 100)]
        for k, v in kvs:
            self.client.set(k, 0, 0, v)
        for k, v in kvs:
            self.client.delete(k)

    def test_set_integer_value(self):
        key = str(uuid.uuid4())
        self.client.set(key, 0, 0, 10)
        (_, cas, value) = self.client.get(id)
        self.assertEqual(value, 10, 'value should be the integer 10')

    def test_two_client_incr(self):
        """http://www.couchbase.com/issues/browse/PYCBC-16"""
        key = str(uuid.uuid4())
        client_one = self.client
        client_two = CouchbaseClient(self.url, self.bucket, "", True)
        # Client one sets a numeric key
        client_one.set(key, 0, 0, '20')
        # Client two tries to increment this numeric key
        (i, cas) = client_two.incr(key)
        self.assertEqual(i, 21)

        # Client two should be able to keep incrementing this key
        (i, cas) = client_two.incr(key)
        self.assertEqual(i, 22)

        # Get's are idempotent and return type
        (_, cas, i) = client_two.get(key)
        self.assertEqual(i, '22')
        (i, cas) = client_two.incr(key)
        self.assertEqual(i, 23)

if __name__ == '__main__':
    unittest.main()
