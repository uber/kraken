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

import time

import pytest

from conftest import TEST_IMAGE


def test_docker_image_replication_success(one_way_replicas):
    one_way_replicas.src.proxy.push(TEST_IMAGE)

    # Wait for replication to finish.
    time.sleep(5)

    with one_way_replicas.dst.agent_factory.create() as agent:
        agent.pull(TEST_IMAGE)


def test_docker_image_replication_retry(one_way_replicas):
    for build_index in one_way_replicas.dst.build_indexes:
        build_index.stop()

    one_way_replicas.src.proxy.push(TEST_IMAGE)

    time.sleep(2)

    with one_way_replicas.dst.agent_factory.create() as agent:
        with pytest.raises(AssertionError):
            agent.pull(TEST_IMAGE)

    for build_index in one_way_replicas.dst.build_indexes:
        build_index.start()

    time.sleep(2)

    with one_way_replicas.dst.agent_factory.create() as agent:
        agent.pull(TEST_IMAGE)


def test_docker_image_replication_resilient_to_build_index_data_loss(one_way_replicas):
    for build_index in one_way_replicas.dst.build_indexes:
        build_index.stop()

    one_way_replicas.src.proxy.push(TEST_IMAGE)

    one_way_replicas.src.build_indexes[0].stop() # Initial replicate was sent to this one.
    one_way_replicas.src.build_indexes[1].stop()

    # The replicate task should have been duplicated to the third build-index,
    # so once zone2 is available it should replicate the image.

    time.sleep(2)

    with one_way_replicas.dst.agent_factory.create() as agent:
        with pytest.raises(AssertionError):
            agent.pull(TEST_IMAGE)

    for build_index in one_way_replicas.dst.build_indexes:
        build_index.start()

    time.sleep(3)

    with one_way_replicas.dst.agent_factory.create() as agent:
        agent.pull(TEST_IMAGE)
