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
"""Integration tests for memory cache feature in origin servers."""
from __future__ import absolute_import

import hashlib
import os

import polling2
import requests


def _generate_blob(size_bytes=1024):
    """Generate a test blob of specified size."""
    blob = os.urandom(size_bytes)
    name = hashlib.sha256(blob).hexdigest()
    return name, blob


def _get_metric_value(statsd_port, metric_name):
    """Fetch a specific metric value from statsd_exporter metrics endpoint."""
    url = 'http://localhost:{port}/metrics'.format(port=statsd_port)
    try:
        res = requests.get(url)
        res.raise_for_status()
        metrics_text = res.text

        # Metric names in tally with tags are formatted as:
        # prefix.component_tag.metric_name -> kraken_origin_blob_memory_cache_entries_added
        # StatsD converts dots to underscores
        full_metric_name = 'kraken_origin_blob_memory_cache_{}'.format(metric_name)

        for line in metrics_text.split('\n'):
            if line.startswith('#'):
                continue
            if full_metric_name in line:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        return float(parts[1])
                    except ValueError:
                        continue

        return 0.0
    except Exception as e:
        return 0.0


def test_memory_cache_write_and_read(origin_cluster, statsd_exporter, agent):
    """Test that blobs downloaded from backend are cached in memory and served from cache."""
    http_port, statsd_port = statsd_exporter

    name, blob = _generate_blob(size_bytes=10 * 1024)

    testfs = origin_cluster.origins[0].testfs

    initial_entries_added = _get_metric_value(http_port, 'entries_added')
    initial_get_hits = _get_metric_value(http_port, 'get_hit')

    testfs.upload(name, blob)

    agent.download(name, blob)

    # Poll until entries_added metric increases
    polling2.poll(
        lambda: _get_metric_value(http_port, 'entries_added') > initial_entries_added,
        step=0.2,
        timeout=5
    )

    agent.download(name, blob)

    # Poll until get_hit metric increases
    polling2.poll(
        lambda: _get_metric_value(http_port, 'get_hit') > initial_get_hits,
        step=0.2,
        timeout=5
    )


def test_memory_cache_drain_to_disk(origin_cluster, statsd_exporter, agent):
    """Test that blobs in memory cache are asynchronously drained to disk."""
    http_port, _ = statsd_exporter

    name, blob = _generate_blob(size_bytes=10 * 1024)

    testfs = origin_cluster.origins[0].testfs

    testfs.upload(name, blob)

    # Record initial metrics
    initial_entries = _get_metric_value(http_port, 'entries_added')

    agent.download(name, blob)

    # Poll until blob is added to memory cache
    polling2.poll(
        lambda: _get_metric_value(http_port, 'entries_added') > initial_entries,
        step=0.2,
        timeout=5
    )

    # Poll until blob is drained to disk (total_size_bytes becomes 0)
    polling2.poll(
        lambda: _get_metric_value(http_port, 'total_size_bytes') == 0,
        step=0.2,
        timeout=10
    )

    # Verify we can still download it (should come from disk now)
    agent.download(name, blob)


def test_memory_cache_large_blob_fallback(origin_cluster, statsd_exporter, agent):
    """Test that blobs larger than memory cache capacity fallback to disk."""
    http_port, _ = statsd_exporter

    name, blob = _generate_blob(size_bytes=2 * 1024 * 1024)

    testfs = origin_cluster.origins[0].testfs

    initial_entries_added = _get_metric_value(http_port, 'entries_added')

    testfs.upload(name, blob)

    agent.download(name, blob)


    try:
        polling2.poll(
            lambda: _get_metric_value(http_port, 'entries_added') > initial_entries_added,
            step=0.2,
            timeout=2
        )
        assert False, "Large blob should NOT be cached in memory"
    except polling2.TimeoutException:
        pass


def test_memory_cache_ttl_expiration(origin_cluster, statsd_exporter, agent):
    """Test that blobs are removed from memory cache after TTL expires."""
    http_port, _ = statsd_exporter

    name, blob = _generate_blob(size_bytes=5 * 1024)

    testfs = origin_cluster.origins[0].testfs

    testfs.upload(name, blob)

    # Record initial metrics
    initial_entries = _get_metric_value(http_port, 'entries_added')

    agent.download(name, blob)

    # Poll until blob is added to memory cache
    polling2.poll(
        lambda: _get_metric_value(http_port, 'entries_added') > initial_entries,
        step=0.2,
        timeout=5
    )

    # Poll until blob is removed from memory (TTL is 2s)
    polling2.poll(
        lambda: _get_metric_value(http_port, 'total_size_bytes') == 0,
        step=0.2,
        timeout=5
    )

    # Verify we can still download it
    agent.download(name, blob)
