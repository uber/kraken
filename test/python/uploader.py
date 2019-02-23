# Copyright (c) 2016-2019 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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
