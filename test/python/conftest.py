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

import subprocess
import time
from collections import namedtuple

import pytest

from .components import (
    AgentFactory,
    BuildIndex,
    Cluster,
    Origin,
    OriginCluster,
    Proxy,
    TestFS,
    Tracker,
    find_free_port,
    get_docker_bridge,
)

DEFAULT = 'default'


# It turns out that URL path escaping Docker tags is a common bug which is very
# annoying to debug in production. This function prefixes images with a "test/",
# such that if the "/" is not properly escaped, the tests will break.
def _setup_test_image(name):
    new_name = 'test/' + name
    for command in [
        ['docker', 'pull', name],
        ['docker', 'tag', name, new_name],
    ]:
        subprocess.check_call(command)
    return new_name


TEST_IMAGE = _setup_test_image('alpine:latest')
TEST_IMAGE_2 = _setup_test_image('redis:latest')


@pytest.fixture
def tracker(origin_cluster, testfs):
    tracker = Tracker(DEFAULT, origin_cluster)
    yield tracker
    tracker.teardown()


@pytest.fixture
def origin_cluster(testfs, statsd_exporter):
    http_port, statsd_port = statsd_exporter

    instances = {
        name: Origin.Instance(name)
        for name in ('kraken-origin-01', 'kraken-origin-02', 'kraken-origin-03')
    }

    statsd_host_port = '{docker_bridge}:{statsd_port}'.format(
        docker_bridge=get_docker_bridge(),
        statsd_port=statsd_port)

    origin_cluster = OriginCluster([
        Origin(DEFAULT, instances, name, testfs, statsd_host_port=statsd_host_port)
        for name in instances
    ])
    yield origin_cluster
    for origin in origin_cluster:
        origin.teardown()


@pytest.fixture
def agent_factory(tracker, build_index):
    return AgentFactory(DEFAULT, tracker, [build_index])


@pytest.fixture
def agent(agent_factory):
    with agent_factory.create() as agent:
        yield agent


@pytest.fixture
def proxy(origin_cluster, build_index):
    proxy = Proxy(DEFAULT, origin_cluster, [build_index])
    yield proxy
    proxy.teardown()


@pytest.fixture
def build_index(origin_cluster, testfs):
    name = 'kraken-build-index-01'
    instances = {name: BuildIndex.Instance(name)}
    build_index = BuildIndex(DEFAULT, instances, name, origin_cluster, testfs, {})
    yield build_index
    build_index.teardown()


@pytest.fixture
def testfs():
    testfs = TestFS(DEFAULT)
    yield testfs
    testfs.teardown()


@pytest.fixture
def statsd_exporter():
    """StatsD exporter for collecting metrics from origins."""
    # Clean up any existing container with this name
    try:
        subprocess.call(['docker', 'rm', '-f', 'kraken-statsd-exporter'],
                       stderr=subprocess.DEVNULL)
    except Exception:
        pass

    statsd_port = find_free_port()  # Use dynamic port to avoid conflicts
    http_port = find_free_port()

    # Start statsd_exporter with both HTTP (TCP) and StatsD (UDP) ports
    subprocess.check_call([
        'docker', 'run', '-d',
        '--name=kraken-statsd-exporter',
        '-p', f'{http_port}:9102/tcp',
        '-p', f'{statsd_port}:9125/udp',
        'prom/statsd-exporter:latest'
    ])

    # Wait for it to be healthy
    time.sleep(2)

    # Create a dummy container object for cleanup
    class StatsdContainer:
        def __init__(self, name):
            self.name = name
        def remove(self, force=False):
            subprocess.call(['docker', 'rm', '-f', self.name], stderr=subprocess.DEVNULL)

    container = StatsdContainer('kraken-statsd-exporter')

    # Return both ports as a tuple
    yield (http_port, statsd_port)
    container.remove(force=True)


def _create_build_index_instances():
    return {
        name: BuildIndex.Instance(name)
        for name in ('kraken-build-index-01', 'kraken-build-index-02', 'kraken-build-index-03')
    }


@pytest.fixture
def one_way_replicas():
    Replicas = namedtuple('Replicas', ['src', 'dst'])

    src_build_index_instances = _create_build_index_instances()
    dst_build_index_instances = _create_build_index_instances()

    replicas = Replicas(
        src=Cluster('src', src_build_index_instances, [list(dst_build_index_instances.values())[0]]),
        dst=Cluster('dst', dst_build_index_instances))

    yield replicas

    replicas.src.teardown()
    replicas.dst.teardown()


@pytest.fixture
def two_way_replicas():
    Replicas = namedtuple('Replicas', ['zone1', 'zone2'])

    zone1_build_index_instances = _create_build_index_instances()
    zone2_build_index_instances = _create_build_index_instances()

    replicas = Replicas(
        zone1=Cluster('zone1', zone1_build_index_instances, [list(zone2_build_index_instances.values())[0]]),
        zone2=Cluster('zone2', zone2_build_index_instances, [list(zone1_build_index_instances.values())[0]]))

    yield replicas

    replicas.zone1.teardown()
    replicas.zone2.teardown()
