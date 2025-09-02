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
import pytest

from .conftest import (
    TEST_IMAGE,
)


def test_kraken_signing_workflow(proxy):
    """Test signing images directly in Kraken registry."""
    # Push unsigned image to Kraken
    proxy.push(TEST_IMAGE)

    # Sign the image in the Kraken registry
    proxy.sign(TEST_IMAGE)

    # Verify the signature
    proxy.verify_signature(TEST_IMAGE)
