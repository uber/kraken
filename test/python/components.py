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
import platform
import random
import subprocess
import time
import urllib
from contextlib import contextmanager
from io import BytesIO
from os.path import abspath
from Queue import Queue
from socket import socket
from threading import Thread

import requests

from uploader import Uploader
from utils import (
    PortReservation,
    dev_tag,
    find_free_port,
    format_insecure_curl,
    tls_opts,
)


def get_docker_bridge():
    system = platform.system()
    if system == 'Darwin':
        return 'host.docker.internal'
    elif system == 'Linux':
        return '172.17.0.1'
    else:
        raise Exception('unknown system: ' + system)


def print_logs(container):
    title = ' {name} logs '.format(name=container.name)
    left_border = '<' * 20
    right_border = '>' * 20
    fill = ('<' * (len(title) / 2)) + ('>' * (len(title) / 2))
    print '{l}{title}{r}'.format(l=left_border, title=title, r=right_border)
    print container.logs()
    print '{l}{fill}{r}'.format(l=left_border, fill=fill, r=right_border)


def yaml_list(l):
    return '[' + ','.join(map(lambda x: "'" + str(x) + "'", l)) + ']'


def pull(source, image):
    cmd = [
        'tools/bin/puller/puller', '-source', source, '-image', image,
    ]
    assert subprocess.call(cmd, stderr=subprocess.STDOUT) == 0


class HealthCheck(object):

    def __init__(self, cmd, interval=1, min_consecutive_successes=1, timeout=10):
        self.cmd = cmd
        self.interval = interval
        self.min_consecutive_successes = min_consecutive_successes
        self.timeout = timeout

    def run(self, container):
        start_time = time.time()
        successes = 0
        msg = ''
        while time.time() - start_time < self.timeout:
            try:
                # We can't use container.exec_run since it doesn't expose exit code.
                subprocess.check_output(
                    'docker exec {name} {cmd}'.format(name=container.name, cmd=self.cmd),
                    shell=True)
                successes += 1
                if successes >= self.min_consecutive_successes:
                    return
            except Exception as e:
                msg = str(e)
                successes = 0
            time.sleep(self.interval)

        raise RuntimeError('Health check failure: {msg}'.format(msg=msg))


class DockerContainer(object):

    def __init__(self, name, image, command=None, ports=None, volumes=None, user=None):
        self.name = name
        self.image = image

        self.command = []
        if command:
            self.command = command

        self.ports = []
        if ports:
            for i, o in ports.iteritems():
                self.ports.extend(['-p', '{o}:{i}'.format(i=i, o=o)])

        self.volumes = []
        if volumes:
            for o, i in volumes.iteritems():
                bind = i['bind']
                mode = i['mode']
                self.volumes.extend(['-v', '{o}:{bind}:{mode}'.format(o=o, bind=bind, mode=mode)])

        self.user = ['-u', user] if user else []

    def run(self):
        cmd = [
            'docker', 'run',
            '-d',
            '--name=' + self.name,
        ]
        cmd.extend(self.ports)
        cmd.extend(self.volumes)
        cmd.extend(self.user)
        cmd.append(self.image)
        cmd.extend(self.command)
        assert subprocess.call(cmd) == 0

    def logs(self):
        subprocess.call(['docker', 'logs', self.name])

    def remove(self, force=False):
        cmd = [
            'docker', 'rm',
        ]
        if force:
            cmd.append('-f')
        cmd.append(self.name)
        assert subprocess.call(cmd) == 0


def new_docker_container(name, image, command=None, environment=None, ports=None,
                         volumes=None, health_check=None, user=None):
    """
    Creates and starts a detached Docker container. If health_check is specified,
    ensures the container is healthy before returning.
    """
    if command:
        # Set umask so jenkins user can delete files created by non-jenkins user.
        command = ['bash', '-c', 'umask 0000 && {command}'.format(command=' '.join(command))]

    c = DockerContainer(
        name=name,
        image=image,
        command=command,
        ports=ports,
        volumes=volumes,
        user=user)
    c.run()
    print 'Starting container {}'.format(c.name)
    try:
        if health_check:
            health_check.run(c)
        else:
            print 'No health checks supplied for {name}'.format(name=c.name)
    except:
        print_logs(c)
        raise
    return c


def populate_config_template(kname, filename, **kwargs):
    """
    Populates a test config template with kwargs for Kraken name `kname`
    and writes the result to the config directory of `kname` with filename.
    """
    template = abspath('config/{kname}/test.template'.format(kname=kname))
    yaml = abspath('config/{kname}/{filename}'.format(kname=kname, filename=filename))

    with open(template) as f:
        config = f.read().format(**kwargs)

    with open(yaml, 'w') as f:
        f.write(config)


def init_cache(cname):
    """
    Wipes and initializes a cache dir for container name `cname`.
    """
    cache = abspath('.tmptest/test-kraken-integration/{cname}/cache'.format(cname=cname))
    if os.path.exists(cache):
        subprocess.check_call(['rm', '-rf', cache])
    os.makedirs(cache)
    os.chmod(cache, 0777)
    return cache


def create_volumes(kname, cname, local_cache=True):
    """
    Creates volume bindings for Kraken name `kname` and container name `cname`.
    """
    volumes = {}

    # Mount configuration directory. This is necessary for components which
    # populate templates and need to mount the populated template into the
    # container.
    config = abspath('config/{kname}'.format(kname=kname))
    volumes[config] = {
        'bind': '/etc/kraken/config/{kname}'.format(kname=kname),
        'mode': 'ro',
    }

    if local_cache:
        # Mount local cache. Allows components to simulate unavailability whilst
        # retaining their state on disk.
        cache = init_cache(cname)
        volumes[cache] = {
            'bind': '/var/cache/kraken/kraken-{kname}/'.format(kname=kname),
            'mode': 'rw',
        }

    return volumes


class Component(object):
    """
    Base class for all containerized Kraken components. Each subclass implements
    the container property for exposing its underlying Docker container, and Component
    provides utilities acting upon said container.
    """
    def new_container(self):
        """
        Initializes a new container. All subclasses must implement this method.
        """
        raise NotImplementedError

    def start(self):
        self.container = self.new_container()

    def stop(self, wipe_disk=False):
        self.container.remove(force=True)
        if wipe_disk:
            cache = init_cache(self.container.name)

    def restart(self, wipe_disk=False):
        self.stop(wipe_disk=wipe_disk)
        # When a container is removed, there is a race condition
        # when starting the container with the same command right away,
        # which causes the start command to fail.
        # Sleep for one second to make sure that the container is really
        # removed from docker.
        time.sleep(1)
        self.start()

    def print_logs(self):
        print_logs(self.container)

    def teardown(self):
        try:
            self.print_logs()
            self.stop()
        except Exception as e:
            print 'Teardown {name} failed: {e}'.format(name=self.container.name, e=e)


class Tracker(Component):

    def __init__(self, zone, origin_cluster):
        self.zone = zone
        self.origin_cluster = origin_cluster
        self.port = find_free_port()
        self.config_file = 'test-{zone}.yaml'.format(zone=zone)
        self.name = 'kraken-tracker-{zone}'.format(zone=zone)

        populate_config_template(
            'tracker',
            self.config_file,
            origins=yaml_list([o.addr for o in self.origin_cluster.origins]))

        self.volumes = create_volumes('tracker', self.name)

        self.start()

    def new_container(self):
        return new_docker_container(
            name=self.name,
            image=dev_tag('kraken-tracker'),
            environment={},
            ports={self.port: self.port},
            volumes=self.volumes,
            command=[
                '/usr/bin/kraken-tracker',
                '--config=/etc/kraken/config/tracker/{config}'.format(config=self.config_file),
                '--port={port}'.format(port=self.port)],
            health_check=HealthCheck(format_insecure_curl('localhost:{port}/health'.format(port=self.port))))

    @property
    def addr(self):
        return '{}:{}'.format(get_docker_bridge(), self.port)


class Origin(Component):

    class Instance(object):

        def __init__(self, name):
            self.name = name
            self.hostname = get_docker_bridge()
            self.port_rez = PortReservation()
            self.peer_port = find_free_port()

        @property
        def port(self):
            return self.port_rez.get()

        @property
        def addr(self):
            return '{}:{}'.format(self.hostname, self.port)

    def __init__(self, zone, instances, name, testfs):
        self.zone = zone
        self.instance = instances[name]
        self.testfs = testfs
        self.config_file = 'test-{zone}.yaml'.format(zone=zone)
        self.name = '{name}-{zone}'.format(name=self.instance.name, zone=zone)

        populate_config_template(
            'origin',
            self.config_file,
            origins=yaml_list([i.addr for i in instances.values()]),
            testfs=self.testfs.addr)

        self.volumes = create_volumes('origin', self.name)

        self.start()

    def new_container(self):
        self.instance.port_rez.release()
        return new_docker_container(
            name=self.name,
            image=dev_tag('kraken-origin'),
            volumes=self.volumes,
            environment={},
            ports={
                self.instance.port: self.instance.port,
                self.instance.peer_port: self.instance.peer_port,
            },
            command=[
                '/usr/bin/kraken-origin',
                '--config=/etc/kraken/config/origin/{config}'.format(config=self.config_file),
                '--blobserver-port={port}'.format(port=self.instance.port),
                '--blobserver-hostname={hostname}'.format(hostname=self.instance.hostname),
                '--peer-ip={ip}'.format(ip=get_docker_bridge()),
                '--peer-port={port}'.format(port=self.instance.peer_port),
            ],
            health_check=HealthCheck(format_insecure_curl('https://localhost:{}/health'.format(self.instance.port))))

    @property
    def addr(self):
        return self.instance.addr


class OriginCluster(object):

    def __init__(self, origins):
        self.origins = origins

    def get_location(self, name):
        url = 'https://localhost:{port}/blobs/sha256:{name}/locations'.format(
            port=random.choice(self.origins).instance.port, name=name)
        res = requests.get(url, **tls_opts())
        res.raise_for_status()
        addr = random.choice(res.headers['Origin-Locations'].split(','))
        # Origin addresses are configured under the bridge network, but we
        # need to speak via localhost.
        addr = addr.replace(get_docker_bridge(), 'localhost')
        return addr

    def upload(self, name, blob):
        addr = self.get_location(name)
        Uploader(addr).upload(name, blob)

    def __iter__(self):
        return iter(self.origins)


class Agent(Component):

    def __init__(self, zone, id, tracker, build_indexes, with_docker_socket=False):
        self.zone = zone
        self.id = id
        self.tracker = tracker
        self.build_indexes = build_indexes
        self.torrent_client_port = find_free_port()
        self.registry_port = find_free_port()
        self.port = find_free_port()
        self.config_file = 'test-{zone}.yaml'.format(zone=zone)
        self.name = 'kraken-agent-{id}-{zone}'.format(id=id, zone=zone)
        self.with_docker_socket = with_docker_socket

        populate_config_template(
            'agent',
            self.config_file,
            trackers=yaml_list([self.tracker.addr]),
            build_indexes=yaml_list([bi.addr for bi in self.build_indexes]))

        if self.with_docker_socket:
            # In aditional to the need to mount docker socket, also avoid using
            # local cache volume, otherwise the process would run as root and
            # create local cache files that's hard to clean outside of the
            # container.
            self.volumes = create_volumes('agent', self.name, local_cache=False)
            self.volumes['/var/run/docker.sock'] = {
                'bind': '/var/run/docker.sock',
                'mode': 'rw',
            }
        else:
            self.volumes = create_volumes('agent', self.name)

        self.start()

    def new_container(self):
        # Root user is needed for accessing docker socket.
        user = 'root' if self.with_docker_socket else None
        return new_docker_container(
            name=self.name,
            image=dev_tag('kraken-agent'),
            environment={},
            ports={
                self.torrent_client_port: self.torrent_client_port,
                self.registry_port: self.registry_port,
                self.port: self.port,
            },
            volumes=self.volumes,
            command=[
                '/usr/bin/kraken-agent',
                '--config=/etc/kraken/config/agent/{config}'.format(config=self.config_file),
                '--peer-ip={}'.format(get_docker_bridge()),
                '--peer-port={port}'.format(port=self.torrent_client_port),
                '--agent-server-port={port}'.format(port=self.port),
                '--agent-registry-port={port}'.format(port=self.registry_port),
            ],
            health_check=HealthCheck('curl localhost:{port}/health'.format(port=self.port)),
            user=user)

    @property
    def registry(self):
        return '127.0.0.1:{port}'.format(port=self.registry_port)

    def download(self, name, expected):
        url = 'http://localhost:{port}/namespace/testfs/blobs/{name}'.format(
            port=self.port, name=name)
        s = requests.session()
        s.keep_alive = False
        res = s.get(url, stream=True, timeout=60)
        res.raise_for_status()
        assert res.content == expected

    def pull(self, image):
        return pull(self.registry, image)

    def preload(self, image):
        url = 'http://127.0.0.1:{port}/preload/tags/{image}'.format(
            port=self.port, image=urllib.quote(image, safe=''))
        s = requests.session()
        s.keep_alive = False
        res = s.get(url, stream=True, timeout=60)
        res.raise_for_status()


class AgentFactory(object):

    def __init__(self, zone, tracker, build_indexes):
        self.zone = zone
        self.tracker = tracker
        self.build_indexes = build_indexes

    @contextmanager
    def create(self, n=1, with_docker_socket=False):
        agents = [Agent(self.zone, i, self.tracker, self.build_indexes, with_docker_socket) for i in range(n)]
        try:
            if len(agents) == 1:
                yield agents[0]
            else:
                yield agents
        finally:
            for agent in agents:
                agent.teardown()


class Proxy(Component):

    def __init__(self, zone, origin_cluster, build_indexes):
        self.zone = zone
        self.origin_cluster = origin_cluster
        self.build_indexes = build_indexes
        self.port = find_free_port()
        self.config_file = 'test-{zone}.yaml'.format(zone=zone)
        self.name = 'kraken-proxy-{zone}'.format(zone=zone)

        populate_config_template(
            'proxy',
            self.config_file,
            build_indexes=yaml_list([bi.addr for bi in self.build_indexes]),
            origins=yaml_list([o.addr for o in self.origin_cluster.origins]))

        self.volumes = create_volumes('proxy', self.name)

        self.start()

    def new_container(self):
        return new_docker_container(
            name=self.name,
            image=dev_tag('kraken-proxy'),
            ports={self.port: self.port},
            environment={},
            command=[
                '/usr/bin/kraken-proxy',
                '--config=/etc/kraken/config/proxy/{config}'.format(config=self.config_file),
                '--port={port}'.format(port=self.port),
            ],
            volumes=self.volumes,
            health_check=HealthCheck('curl localhost:{port}/v2/'.format(port=self.port)))

    @property
    def registry(self):
        return '127.0.0.1:{port}'.format(port=self.port)

    def push(self, image):
        proxy_image = '{reg}/{img}'.format(reg=self.registry, img=image)
        for command in [
            ['docker', 'tag', image, proxy_image],
            ['docker', 'push', proxy_image],
        ]:
            subprocess.check_call(command)

    def push_as(self, image, new_tag):
        repo = image.split(':')[0]
        proxy_image = '{reg}/{repo}:{tag}'.format(reg=self.registry, repo=repo, tag=new_tag)
        for command in [
            ['docker', 'tag', image, proxy_image],
            ['docker', 'push', proxy_image],
        ]:
            subprocess.check_call(command)

    def list(self, repo):
        url = 'http://{reg}/v2/{repo}/tags/list'.format(reg=self.registry, repo=repo)
        res = requests.get(url)
        res.raise_for_status()
        return res.json()['tags']

    def catalog(self):
        url = 'http://{reg}/v2/_catalog'.format(reg=self.registry)
        res = requests.get(url)
        res.raise_for_status()
        return res.json()['repositories']

    def pull(self, image):
        pull(self.registry, image)


class BuildIndex(Component):

    class Instance(object):

        def __init__(self, name):
            self.name = name
            self.hostname = get_docker_bridge()
            self.port_rez = PortReservation()

        @property
        def port(self):
            return self.port_rez.get()

        @property
        def addr(self):
            return '{}:{}'.format(self.hostname, self.port)

    def __init__(self, zone, instances, name, origin_cluster, testfs, remote_instances=None):
        self.zone = zone
        self.instance = instances[name]
        self.origin_cluster = origin_cluster
        self.testfs = testfs
        self.config_file = 'test-{zone}.yaml'.format(zone=zone)
        self.name = '{name}-{zone}'.format(name=self.instance.name, zone=zone)

        remotes = "remotes:\n{remotes}".format(remotes='\n'.join("  {addr}: [.*]".format(addr=i.addr) for i in (remote_instances or [])))

        populate_config_template(
            'build-index',
            self.config_file,
            testfs=testfs.addr,
            origins=yaml_list([o.addr for o in self.origin_cluster.origins]),
            cluster=yaml_list([i.addr for i in instances.values()]),
            remotes=remotes)

        self.volumes = create_volumes('build-index', self.name)

        self.start()

    def new_container(self):
        self.instance.port_rez.release()
        return new_docker_container(
            name=self.name,
            image=dev_tag('kraken-build-index'),
            ports={self.port: self.port},
            environment={},
            command=[
                '/usr/bin/kraken-build-index',
                '--config=/etc/kraken/config/build-index/{config}'.format(config=self.config_file),
                '--port={port}'.format(port=self.port),
            ],
            volumes=self.volumes,
            health_check=HealthCheck(format_insecure_curl(
                'https://localhost:{}/health'.format(self.port))))

    @property
    def port(self):
        return self.instance.port

    @property
    def addr(self):
        return self.instance.addr

    def list_repo(self, repo):
        url = 'https://localhost:{port}/repositories/{repo}/tags'.format(
                port=self.port,
                repo=urllib.quote(repo, safe=''))
        res = requests.get(url, **tls_opts())
        res.raise_for_status()
        return res.json()['result']


class TestFS(Component):

    def __init__(self, zone):
        self.zone = zone
        self.port = find_free_port()
        self.start()

    def new_container(self):
        return new_docker_container(
            name='kraken-testfs-{zone}'.format(zone=self.zone),
            image=dev_tag('kraken-testfs'),
            ports={self.port: self.port},
            command=[
                '/usr/bin/kraken-testfs',
                '--port={port}'.format(port=self.port),
            ],
            health_check=HealthCheck('curl localhost:{port}/health'.format(port=self.port)))

    def upload(self, name, blob):
        url = 'http://localhost:{port}/files/blobs/{name}'.format(port=self.port, name=name)
        res = requests.post(url, data=BytesIO(blob))
        res.raise_for_status()

    @property
    def addr(self):
        return '{}:{}'.format(get_docker_bridge(), self.port)


class Cluster(object):

    def __init__(
        self,
        zone,
        local_build_index_instances,
        remote_build_index_instances=None):
        """
        Initializes a full Kraken cluster.

        Note, only use a full cluster if you need to test multiple clusters. Otherwise,
        the default fixtures should suffice.
        """
        self.zone = zone
        self.components = []

        self.testfs = self._register(TestFS(zone))

        origin_instances = {
            name: Origin.Instance(name)
            for name in ('kraken-origin-01', 'kraken-origin-02', 'kraken-origin-03')
        }
        self.origin_cluster = OriginCluster([
            self._register(Origin(zone, origin_instances, name, self.testfs))
            for name in origin_instances
        ])

        self.tracker = self._register(Tracker(zone, self.origin_cluster))

        self.build_indexes = []
        for name in local_build_index_instances:
            self.build_indexes.append(self._register(
                BuildIndex(
                    zone, local_build_index_instances, name, self.origin_cluster, self.testfs,
                    remote_build_index_instances)))

        # TODO(codyg): Some tests rely on the fact that proxy and agents point
        # to the first build-index.
        self.proxy = self._register(Proxy(zone, self.origin_cluster, self.build_indexes))

        self.agent_factory = AgentFactory(zone, self.tracker, self.build_indexes)

    def _register(self, component):
        self.components.append(component)
        return component

    def teardown(self):
        for c in self.components:
            c.teardown()
