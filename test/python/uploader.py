from __future__ import absolute_import

import requests

from utils import tls_opts_with_client_certs


class Uploader(object):

    def __init__(self, addr):
        self.addr = addr

    def _start(self, name):
        url = 'https://{addr}/namespace/testfs/blobs/sha256:{name}/uploads'.format(
            addr=self.addr, name=name)
        res = requests.post(url, **tls_opts_with_client_certs())
        res.raise_for_status()
        return res.headers['Location']

    def _patch(self, name, uid, start, stop, chunk):
        url = 'https://{addr}/namespace/testfs/blobs/sha256:{name}/uploads/{uid}'.format(
            addr=self.addr, name=name, uid=uid)
        res = requests.patch(url, headers={'Content-Range': '%d-%d' % (start, stop)}, data=chunk, **tls_opts_with_client_certs())
        res.raise_for_status()

    def _commit(self, name, uid):
        url = 'https://{addr}/namespace/testfs/blobs/sha256:{name}/uploads/{uid}'.format(
            addr=self.addr, name=name, uid=uid)
        res = requests.put(url, **tls_opts_with_client_certs())
        res.raise_for_status()

    def upload(self, name, blob):
        uid = self._start(name)
        self._patch(name, uid, 0, len(blob), blob)
        self._commit(name, uid)
