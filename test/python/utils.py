// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
from __future__ import absolute_import

from threading import Thread


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
