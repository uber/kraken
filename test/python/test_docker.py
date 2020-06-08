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

from conftest import (
    TEST_IMAGE,
    TEST_IMAGE_2,
)


def test_proxy_push_and_pull(proxy):
    proxy.push(TEST_IMAGE)

    proxy.pull(TEST_IMAGE)


def test_proxy_pull_after_data_loss(proxy):
    proxy.push(TEST_IMAGE)

    proxy.restart(wipe_disk=True)

    proxy.pull(TEST_IMAGE)


def test_agent_pull(proxy, agent):
    proxy.push(TEST_IMAGE)

    agent.pull(TEST_IMAGE)


def test_agent_preload(proxy, agent_factory):
    proxy.push(TEST_IMAGE)

    with agent_factory.create(with_docker_socket=True) as agent:
        agent.preload(TEST_IMAGE)


def test_proxy_list_repository_tags(proxy, build_index):
    tags = {'0001', '0002', '0003'}
    for tag in tags:
        proxy.push_as(TEST_IMAGE, tag)

    # Push a separate image to ensure we scope by repo properly.
    proxy.push_as(TEST_IMAGE_2, '0004')

    repo = TEST_IMAGE.split(':')[0]

    assert sorted(proxy.list(repo)) == sorted(tags)

    # Make sure old build-index endpoint still works.
    assert sorted(build_index.list_repo(repo)) == sorted(tags)


def test_proxy_catalog(proxy):
    # Push a few tags to make sure we deduplicate repos.
    for tags in {'0001', '0002', '0003'}:
        proxy.push(TEST_IMAGE)
        proxy.push(TEST_IMAGE_2)

    repos = map(lambda img: img.split(':')[0], (TEST_IMAGE, TEST_IMAGE_2))

    assert sorted(proxy.catalog()) == sorted(repos)


def test_docker_image_distribution_high_availability(testfs, proxy, origin_cluster, build_index,
                                                     agent_factory):
    # This is a long test for ensuring high availability during the entire docker
    # image distribution flow.

    # PART 1: Backend storage is unavailable. We should still be able to upload
    # and distribute builds by relying on on-disk caches.

    testfs.stop()

    proxy.push(TEST_IMAGE)

    # Must be able to survive soft restarts on our components. Ensures everything
    # is properly stored on disk.

    for origin in origin_cluster:
        origin.restart()

    build_index.restart()

    with agent_factory.create() as agent:
        agent.pull(TEST_IMAGE)

    # PART 2: Backend storage is back online. Tags and blobs should automatically
    # persist to backend storage, and we should survive total data loss by
    # recovering data from backend storage.

    testfs.start()

    time.sleep(3)

    # Tags and blobs should be backed up by now. Wipe all disks.

    for origin in origin_cluster:
        origin.restart(wipe_disk=True)

    build_index.restart(wipe_disk=True)

    with agent_factory.create() as agent:
        agent.pull(TEST_IMAGE)
