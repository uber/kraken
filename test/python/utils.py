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
        '--cert /home/udocker/kraken/tls/client/client.crt',
        '--key /home/udocker/kraken/tls/client/client_decrypted.key',
        '--cacert /home/udocker/kraken/tls/ca/server.crt',
        url,
    ])


def tls_opts():
    return {
        'cert': ('test/tls/client/client.crt', 'test/tls/client/client_decrypted.key'),
        'verify': False, ## Set verify=False to disable server cert verification for test only.
    }
