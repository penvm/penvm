#
# penvm/lib/mqueue.py
#

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

import logging
from typing import Any, Callable, Dict, List, Tuple, Type, Union


from penvm.lib.base import BaseObject
from penvm.lib.message import Message
from penvm.lib.misc import State, get_uuid
from penvm.lib.queue import Queue

logger = logging.getLogger(__name__)


class MessageQueue(BaseObject, Queue):
    """Message queue.

    Built on top of [penvm.lib.queue.Queue][].
    """

    def __init__(self, qsize: int = 0):
        """Initialize.

        Args:
            qsize: Maximum queue size. 0 for unlimited.
        """
        BaseObject.__init__(self, None, logger)
        Queue.__init__(self, qsize)

    def find(
        self,
        id: str,
    ) -> "Message":
        """Find and return message in queue with given header id.

        Args:
            id: Message header id.

        Returns:
            Matching Message.
        """
        for v in self.values():
            if v.header.get("id") == id:
                return v

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            values = []
            for v in self.values():
                if type(v) == Message:
                    values.append(v.state())
                else:
                    values.append(None)
            return State(
                "mqueue",
                self.oid,
                {
                    "frozen": self.frozen,
                    "npop": self.npop,
                    "nput": self.nput,
                    "size": self.qsize,
                    "values": values,
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
