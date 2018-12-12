from __future__ import absolute_import

import time

import pytest

from conftest import TEST_IMAGE


def test_docker_image_replication(one_way_replicas):
    one_way_replicas.src.proxy.push(TEST_IMAGE)

    # Wait for replication to finish.
    time.sleep(2)
    
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
    # so once dca1 is available it should replicate the image.

    time.sleep(2)

    with one_way_replicas.dst.agent_factory.create() as agent:
        with pytest.raises(AssertionError):
            agent.pull(TEST_IMAGE)

    for build_index in one_way_replicas.dst.build_indexes:
        build_index.start()

    time.sleep(3)

    with one_way_replicas.dst.agent_factory.create() as agent:
        agent.pull(TEST_IMAGE)
