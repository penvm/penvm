#
# penvm/lib/base.py

# PENVM
#
# Copyright 2023 J4M Solutions
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

"""Provide `BaseObject` for all PENVM classes that need its
functionality."""

from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.misc import LoggerAdapter, get_uuid


class BaseObject:
    """Base object for PENVM classes.

    Provides common support for `oid` (UU object id) and
    object-specific `LoggerAdapter` logger."""

    name = "base"

    def __init__(self, oid: str, logger: "Logger"):
        """Initialize.

        Args:
            oid: Universally unique object id.
            logger: Logger to wrap with `LoggerAdapter`.
        """
        self.oid = oid or get_uuid()
        self.logger = LoggerAdapter(logger, {"self": self, "id": self.oid})
