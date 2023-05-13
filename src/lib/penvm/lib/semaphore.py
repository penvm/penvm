#
# penvm/lib/semaphore.py
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
from threading import Lock
from typing import Any, Callable, Dict, List, Tuple, Type, Union


logger = logging.getLogger(__name__)


class AdjustableSemaphore:
    """Semaphore with support for adjusting the limit `n` while in
    use."""

    def __init__(
        self,
        n: int = 1,
    ):
        """Initialize.

        Args:
            n: Count.
        """
        self._max = n

        self._curr = 0

        self._lock = Lock()
        self._waitlock = Lock()

    def acquire(self):
        """Acquire a semaphore.

        Increase current count.
        """
        while True:
            if self._lock.acquire(blocking=False):
                if self._curr < self._max:
                    self._curr += 1
                    self._lock.release()
                    return
                self._lock.release()
            self._waitlock.acquire()

    def adjust(self, n: int = 1):
        """Adjust count.

        Args:
            n: New count.
        """
        self._lock.acquire()
        n = n if n > 0 else 1
        self._max = n
        self._lock.release()
        # wake up waiter that might *now* acquire a lock
        try:
            self._waitlock.release()
        except:
            # ignore if already unlocked!
            pass

    def count(self) -> int:
        """Return current count.

        Returns:
            Current count.
        """
        return self._curr

    def max(self) -> int:
        """Return maximum count allowed.

        Returns:
            Maximum count.
        """
        return self._max

    def release(self):
        """Release a semaphore.

        Decreases current count.
        """
        self._lock.acquire()
        self._curr = max(self._curr - 1, 0)
        self._lock.release()

        try:
            # wake up waiters
            self._waitlock.release()
        except Exception as e:
            pass
