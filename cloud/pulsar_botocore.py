import os
import mimetypes
import asyncio
import functools

import botocore.session

from pulsar.apps.greenio import GreenPool, getcurrent
from .sock import wrap_poolmanager, StreamingBodyWsgiIterator

# 8MB for multipart uploads
MULTI_PART_SIZE = 2**23


def nonblocking(func, pass_wrapper=False):
    def _nonblocking(wrapper, *args, **kwargs):
        # print('***', wrapper)
        if pass_wrapper:
            args = (wrapper,) + args
        return wrapper._nonblocking(func, *args, **kwargs)
    return functools.wraps(func)(_nonblocking)

class Botocore(object):
    '''An asynchronous wrapper for botocore
    '''
    def __init__(self, service_name, region_name=None,
                 endpoint_url=None, session=None, green_pool=None,
                 green=True, loop=None, **kwargs):
        self._green_pool = green_pool
        self.session = session or botocore.session.get_session()
        self.client = self.session.create_client(service_name,
                                                 region_name=region_name,
                                                 endpoint_url=endpoint_url,
                                                 **kwargs)

        # self._make_api_call = self.client._make_api_call
        if green:
            # endpoint = self.client._endpoint
            # for adapter in endpoint.http_session.adapters.values():
            #     adapter.poolmanager = wrap_poolmanager(adapter.poolmanager)

            # self.client._make_api_call = self._green_call
            self._nonblocking = self._green_call
        else:
            self._loop = loop or asyncio.get_event_loop()
            # self.client._make_api_call = self._executor_call
            self._nonblocking = self._executor_call


    def __getattr__(self, name):
        attr = getattr(self.client, name)
        if callable(attr):
            attr = functools.partial(nonblocking(attr), self)
        return attr

    def green_pool(self):
        if not self._green_pool:
            self._green_pool = GreenPool()
        return self._green_pool

    # def wsgi_stream_body(self, body, n=-1):
    #     '''WSGI iterator of a botocore StreamingBody
    #     '''
    #     return StreamingBodyWsgiIterator(body, self.green_pool(), n)

    @functools.partial(nonblocking, pass_wrapper=True)
    def upload_file(self, bucket, file, uploadpath=None, key=None,
                    ContentType=None, **kw):
        '''Upload a file to S3 possibly using the multi-part uploader

        Return the key uploaded
        '''
        if hasattr(file, 'read'):
            if hasattr(file, 'seek'):
                file.seek(0)
            file = file.read()

        is_file = False
        if not isinstance(file, str):
            size = len(file)
        else:
            is_file = True
            size = os.stat(file).st_size
            if not key:
                key = os.path.basename(file)
            if uploadpath:
                if not uploadpath.endswith('/'):
                    uploadpath = '%s/' % uploadpath
                key = '%s%s' % (uploadpath, key)
            if not ContentType:
                ContentType, _ = mimetypes.guess_type(file)

        assert key, 'key not available'

        params = dict(Bucket=bucket, Key=key)
        if ContentType:
            params['ContentType'] = ContentType

        if size > MULTI_PART_SIZE and is_file:
            resp = self._multipart(file, params)
        elif is_file:
            with open(file, 'rb') as fp:
                params['Body'] = fp.read()
            resp = self.client.put_object(**params)
        else:
            params['Body'] = file
            resp = self.client.put_object(**params)
        if 'Key' not in resp:
            resp['Key'] = key
        if 'Bucket' not in resp:
            resp['Bucket'] = bucket
        return resp

    def _green_call(self, func, *args, **kwargs):
        pool = self.green_pool()
        return pool.submit(func, *args, **kwargs)

    # def _green_call(self, func, *args, **kwargs):
    #     if getcurrent().parent:
    #         return func(*args, **kwargs)
    #     else:
    #         pool = self.green_pool()
    #         return pool.submit(func, *args, **kwargs)

    # def _green_call(self, operation, kwargs):
    #     if getcurrent().parent:
    #         return self._make_api_call(operation, kwargs)
    #     else:
    #         pool = self.green_pool()
    #         return pool.submit(self._make_api_call, operation, kwargs)
    #
    # def _executor_call(self, operation, kwargs):
    #     return self._loop.run_in_executor(None, functools.partial(
    #         self._make_api_call, operation, kwargs
    #     ))

    # def _read_body(self, body, n):
    #     return

    def _multipart(self, filename, params):
        response = self.client.create_multipart_upload(**params)
        bucket = params['Bucket']
        key = params['Key']
        uid = response['UploadId']
        params['UploadId'] = uid
        params.pop('ContentType', None)
        try:
            parts = []
            with open(filename, 'rb') as file:
                while True:
                    body = file.read(MULTI_PART_SIZE)
                    if not body:
                        break
                    num = len(parts) + 1
                    params['Body'] = body
                    params['PartNumber'] = num
                    part = self.upload_part(**params)
                    parts.append(dict(ETag=part['ETag'], PartNumber=num))
        except:
            self.client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=uid)
            raise
        else:
            if parts:
                all = dict(Parts=parts)
                return self.client.complete_multipart_upload(
                    Bucket=bucket, UploadId=uid, Key=key, MultipartUpload=all)
            else:
                self.client.abort_multipart_upload(Bucket=bucket, Key=key,
                                            UploadId=uid)
