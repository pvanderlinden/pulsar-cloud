import os
import unittest
import string
import tempfile
import asyncio

from botocore.exceptions import ClientError

from pulsar.utils.string import random_string
from pulsar.apps.greenio import GreenPool

from cloud import Botocore
from cloud.pulsar_botocore import MULTI_PART_SIZE


def green(f):

    def _(self, *args, **kwargs):
        return self.green_pool.submit(f, self, *args, **kwargs)

    return _


ONEKB = 2**10
BUCKET = os.environ.get('TEST_S3_BUCKET', 'quantmind-tests')


class RandomFile:
    filename = None

    def __init__(self, size=ONEKB):
        self.size = size

    @property
    def key(self):
        if self.filename:
            return os.path.basename(self.filename)

    def __enter__(self):
        self.filename = tempfile.mktemp()
        with open(self.filename, 'wb') as fout:
            fout.write(os.urandom(self.size))
        return self

    def __exit__(self, *args):
        if self.filename:
            try:
                os.remove(self.filename)
            except FileNotFoundError:
                pass
            self.filename = None

    def body(self):
        if self.filename:
            with open(self.filename, 'rb') as f:
                return f.read()
        return b''


class GreenBotocoreTest(unittest.TestCase):

    @classmethod
    def config(cls):
        cls.green_pool = GreenPool()
        return {
            'green_pool': cls.green_pool
        }

    @classmethod
    def setUpClass(cls):
        config = cls.config()
        cls.ec2 = Botocore('ec2', 'us-east-1', **config)
        cls.s3 = Botocore('s3', **config)

    def assert_status(self, response, code=200):
        meta = response['ResponseMetadata']
        self.assertEqual(meta['HTTPStatusCode'], code)

    def clean_up(self, r):
        response = yield from self.s3.head_object(Bucket=BUCKET,
                                       Key=r.key)
        self.assert_status(response)
        self.assertEqual(response['ContentLength'], r.size)
        # Delete
        response = yield from self.s3.delete_object(Bucket=BUCKET,
                                         Key=r.key)
        self.assert_status(response, 204)
        with self.assertRaises(ClientError):
            yield from self.s3.get_object(Bucket=BUCKET, Key=r.key)

    # # TESTS
    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertTrue(response)
        # https_adapter = self.ec2._endpoint.http_session.adapters['https://']
        # pools = list(https_adapter.poolmanager.pools.values())
        # self.assertEqual(len(pools), 1)

    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertTrue(response)

    def test_list_buckets(self):
        buckets = yield from self.s3.list_buckets()
        self.assertTrue(buckets)

    def test_get_object_chunks(self):
        response = yield from self.s3.get_object(Bucket=BUCKET,
                                      Key='requirements.txt')
        self.assert_status(response)
        data = b''
        while True:
            text = response['Body'].read(10)
            if not text:
                break
            data += text
        self.assertTrue(data)

    def test_upload_text(self):
        with open(__file__, 'r') as f:
            body = f.read()
            key = '%s.py' % random_string(characters=string.ascii_letters)
            response = yield from self.s3.put_object(Bucket=BUCKET,
                                          Body=body,
                                          ContentType='text/plain',
                                          Key=key)
            self.assert_status(response)
        #
        # Read object
        response = yield from self.s3.get_object(Bucket=BUCKET,
                                      Key=key)
        self.assert_status(response)
        self.assertEqual(response['ContentType'], 'text/plain')
        #
        # Delete object
        response = yield from self.s3.delete_object(Bucket=BUCKET,
                                         Key=key)
        self.assert_status(response, 204)
        with self.assertRaises(ClientError):
            yield from self.s3.get_object(
                          Bucket=BUCKET, Key=key)

    def test_upload_binary(self):
        with RandomFile(2**12) as r:
            response = yield from self.s3.upload_file(BUCKET,
                                           r.filename)
            self.assert_status(response)
            yield from self.clean_up(r)

    def test_upload_binary_large(self):
        with RandomFile(int(1.5*MULTI_PART_SIZE)) as r:
            response = yield from self.s3.upload_file(BUCKET,
                                           r.filename)
            self.assert_status(response)
            yield from self.clean_up(r)


class TestsInGreen(type):
    def __new__(cls, name, bases, attrs):
        for key, value in attrs.items():
            if callable(value) and key.startswith('test_'):
                attrs[key] = green(value)
        return super().__new__(cls, name, bases, attrs)

class GreenInGreenBotocoreTest(GreenBotocoreTest, metaclass=TestsInGreen):
    pass


class AsyncBotocoreTest(GreenBotocoreTest):

    @classmethod
    def config(cls):
        return {
            'green': False,
            'loop': asyncio.get_event_loop()
        }
