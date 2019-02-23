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

import hashlib
import os
import time
from threading import Thread

import pytest
import requests

from utils import concurrently_apply
from utils import tls_opts


def test_origin_upload_no_client_cert(origin_cluster):
    name, blob = _generate_blob()
    addr = origin_cluster.get_location(name)
    url = 'https://{addr}/namespace/testfs/blobs/sha256:{name}/uploads'.format(
            addr=addr, name=name)
    res = requests.post(url, **tls_opts())
    assert res.status_code == 403


def test_concurrent_agent_downloads(origin_cluster, agent_factory):
    name, blob = _generate_blob()

    origin_cluster.upload(name, blob)

    # TODO(codyg): This test struggles with more than 4 agents when we limit
    # the max origin connections to 1. I suspect this is because the agents
    # form isolated networks.
    with agent_factory.create(4) as agents:
        concurrently_apply(lambda agent: agent.download(name, blob), agents)


def test_blob_distribution_resilient_to_remote_backend_unavailability(testfs, origin_cluster, agent):
    testfs.stop()

    name, blob = _generate_blob()

    origin_cluster.upload(name, blob)

    agent.download(name, blob)


def test_agent_download_after_remote_backend_upload(testfs, agent):
    name, blob = _generate_blob()

    testfs.upload(name, blob)

    agent.download(name, blob)


def test_agent_download_after_origin_data_loss_after_origin_upload(origin_cluster, agent):
    name, blob = _generate_blob()

    origin_cluster.upload(name, blob)

    # Wipe out all data in the origin cluster.
    for origin in origin_cluster:
        origin.restart(wipe_disk=True)

    agent.download(name, blob)


def test_agent_download_returns_500_when_remote_backend_unavailable(testfs, agent):
    name, _ = _generate_blob()

    testfs.stop()

    with pytest.raises(requests.HTTPError) as exc_info:
        agent.download(name, None)

    assert exc_info.value.response.status_code == 500


def test_agent_download_404(agent):
    name, _ = _generate_blob()

    with pytest.raises(requests.HTTPError) as exc_info:
        agent.download(name, None)

    assert exc_info.value.response.status_code == 404


def test_agent_download_resilient_to_invalid_tracker_cache(origin_cluster, agent):
    name, blob = _generate_blob()

    origin_cluster.upload(name, blob)

    agent.download(name, blob)

    # Wipe out all data in the agent and origins, but leave metainfo cached in tracker.

    agent.restart(wipe_disk=True)

    for origin in origin_cluster:
        origin.restart(wipe_disk=True)

    # Origin should refresh blob even though metainfo was never requested.
    agent.download(name, blob)


def test_agent_download_resilient_to_offline_origin(origin_cluster, agent):
    name, blob = _generate_blob()

    origin_cluster.upload(name, blob)

    # With max_replica=2, we still have one replica left.
    list(origin_cluster)[1].stop()

    agent.download(name, blob)


@pytest.mark.xfail
def test_agent_download_resilient_to_initial_offline_origin(origin_cluster, agent):
    name, blob = _generate_blob()

    origin_cluster.upload(name, blob)

    for origin in origin_cluster:
        origin.stop()

    result = {'error': None}
    def download():
        try:
            agent.download(name, blob)
        except Exception as e:
            result['error'] = e

    # Agent initially has no one to download from since origin is offline.
    t = Thread(target=download)
    t.start()

    time.sleep(2)

    for origin in origin_cluster:
        origin.start()

    t.join()

    assert result['error'] is None


def _generate_blob():
    blob = os.urandom(5 * 1 << 20) # 5MB
    h = hashlib.sha256()
    h.update(blob)
    return h.hexdigest(), blob
