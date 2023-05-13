#
# penvm/lib/queue.py
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

import queue
from typing import Any, Callable, Dict, List, Tuple, Type, Union


class QueueEmpty:
    pass


class Queue:
    """Implementation allowing for inspection and peeking.

    Built on top of [queue.Queue][].
    """

    def __init__(
        self,
        qsize: int = 0,
    ):
        """Initialize.

        Args:
            qsize: Maximum queue size. 0 for unlimited.
        """
        self.frozen = False
        # TODO: support sizing of queue
        self.qsize = qsize = 10
        self._queue = queue.Queue(qsize)
        self._tmp = QueueEmpty
        self.npop = 0
        self.nput = 0

    def clear(self):
        """Clear all queued objects."""
        try:
            while True:
                self._queue.get(block=False)
        except queue.Empty as e:
            pass
        except Exception:
            pass

    def freeze(
        self,
        state: bool,
    ):
        """Allow/disallow additions.

        Args:
            state: New state of queue.
        """
        self.frozen = state

    def get(self) -> Any:
        """Get copy of object from queue.

        Returns:
            Item.
        """
        # TODO: needs some work?
        if self._queue.qsize():
            return self._queue.queue[0].copy()
        return QueueEmpty

    def pop(
        self,
        block: bool = True,
    ) -> Any:
        """Pop object from queue.

        Args:
            block: Wait for object.

        Returns:
            Item.
        """
        try:
            v = self._queue.get(block=block)
            self.npop += 1
            # TODO: what to return if not blocking and no value? None!
        except queue.Empty as e:
            v = None
        return v

    def put(self, o: Any):
        """Put object on queue.

        Args:
            o: Object.
        """
        if not self.frozen:
            self.nput += 1
            self._queue.put(o)
        # TODO: raise exception if frozen

    def size(self) -> int:
        """Return queue size.

        Returns:
            Queue size.
        """
        return self._queue.qsize()

    def values(self) -> List[Any]:
        """Return (actual) queued values.

        Returns:
            Queue values.
        """
        return [v for v in self._queue.queue]


class RoutingQueue(Queue):
    """Route queue operations elsewhere.

    Three functions are registered to handle the different kinds of
    queueing (and this routing) operations.
    """

    def __init__(
        self,
        get: Union[Callable, None] = None,
        pop: Union[Callable, None] = None,
        put: Union[Callable, None] = None,
    ):
        """Initialize.

        Args:
            get: Function to call for `get`.
            pop: Function to call for `pop`.
            put: Function to call for `put`.
        """
        super().__init__()
        self._get = get
        self._pop = pop
        self._put = put

    def get(self) -> Union[Any, None]:
        """Get a copy of an object.

        Returns:
            Copy of an object.
        """
        return None if self._get == None else self._get()

    def pop(
        self,
        block: bool = True,
    ) -> Union[Any, None]:
        """Pop an object.

        Returns:
            An object.
        """
        return None if self._pop == None else self._pop(block)

    def put(
        self,
        o: Any,
    ):
        """Put an object.

        Args:
            o: Object to queue/route.
        """
        self._put(o)

    def size(self) -> int:
        """Queue size is always 0.

        Returns:
            0
        """
        return 0

    def values(self) -> List:
        """Never any values on queue.

        Returns:
            Empty list."""
        return []
