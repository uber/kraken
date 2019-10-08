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

import os
from socket import socket
from threading import Thread


def find_free_port():
    s = socket()
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    return port


class PortReservation(object):
    """
    PortReservation is a utility for finding and reserving an open port. Normally,
    find_free_port is sufficient for components which find an available port and
    immediately start their container. However, the more consecutive find_free_port
    calls are made without actually binding to the "free" ports, the higher chance
    that successive find_free_port calls will return a port twice.

    PortReservation solves this problem by binding a socket to the port until
    the port is ready to be used, at which point the client can call release
    to close the socket and assume ownership of the port.

    Obviously this is not bullet-proof and there is a race between releasing
    the PortReservation and the client binding to the port.
    """

    def __init__(self):
        self._sock = socket()
        self._sock.bind(('', 0))
        self._open = True
        self._port = self._sock.getsockname()[1]

    def get(self):
        return self._port

    def release(self):
        if self._open:
            self._sock.close()
            self._open = False


def concurrently_apply(f, inputs):

    errors = [None] * len(inputs)

    def worker(i):
        try:
            f(inputs[i])
        except Exception as e:
            errors[i] = e
            raise

    threads = [Thread(target=worker, args=(i,)) for i in range(len(inputs))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # If the exception is raised in a thread, it won't fail the test.
    for e in errors:
        assert e is None


def format_insecure_curl(url):
    return ' '.join([
        'curl',
        ## Use --insecure flag to disable server cert verification for test only.
        '--insecure',
        url,
    ])


def tls_opts():
    return {
        'verify': False, ## Set verify=False to disable server cert verification for test only.
    }


def tls_opts_with_client_certs():
    return {
        'cert': ('test/tls/client/client.crt', 'test/tls/client/client_decrypted.key'),
        'verify': False, ## Set verify=False to disable server cert verification for test only.
    }


def dev_tag(image_name):
    tag = os.getenv("PACKAGE_VERSION", "latest")
    return "{}:{}".format(image_name, tag)
