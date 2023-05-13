#
# penvm/lib/thread.py

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

"""Provides modified `Thread` to support thread termination.
"""

import ctypes
import logging
import threading
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.lib.misc import State

logger = logging.getLogger(__name__)


class ThreadInterrupt(Exception):
    """Special exception for augmented Thread."""

    def __init__(self):
        """Initialize."""
        super().__init__("interrupted thread")


class Thread(BaseObject, threading.Thread):
    """Thread with specific settings and functionality:
    * Will die on main thread exit.
    * Support for exception to thread.
    * Terminatable.

    See https://code.activestate.com/recipes/496960-thread2-killable-threads/.
    """

    def __init__(
        self,
        *args: List,
        **kwargs: Dict,
    ):
        """Initialize.

        See [theading.Thread][].
        """
        try:
            BaseObject.__init__(self, None, logger)
            threading.Thread.__init__(self, *args, **kwargs)
            self.oid = self.name
            self.daemon = True
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            pass

    def get_id(self) -> int:
        """Return thread id.

        Returns:
            Thread id.
        """
        if hasattr(self, "_thread_id"):
            return self._thread_id

        for id, thread in threading._active.items():
            if thread is self:
                return id

    def raise_exception(self, exc: Exception):
        """Raise a specific exception.

        Args:
            exc: Exception to raise.
        """
        self.logger.debug("raising exception ({exc}) ...")

        # TODO: should this be repeated until it is effective?

        threadid = self.get_id()
        if threadid != None:
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(threadid),
                ctypes.py_object(exc),
            )
            self.logger.info("raise exception result ({res})")
            if res > 1:
                res2 = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_long(threadid),
                    0,
                )
                # exception raise failure

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            return State(
                "thread",
                self.oid,
                {
                    "args": [str(arg) for arg in self._args],
                    "kwargs": [str(arg) for arg in self._kwargs.items()],
                    "name": self._name,
                    "native-id": self.native_id,
                    "target": str(self._target),
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")

    def terminate(self):
        """Terminate. Raises exception."""
        # TODO: will this ultimately work, e.g., even when returning
        # from C code/extension? should it repeat until effective?
        self.logger.debug("terminating ...")
        self.raise_exception(ThreadInterrupt)
