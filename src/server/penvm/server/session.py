#
# penvm/server/session.py

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

"""
A (server-side) session isolates communication, operations, and
processing. Except for special kernel operations which allow for one
session to affect another, there are no inter-session dependencies.

Each session has its own incoming and outgoing message queues.

Each session has and manages its own `Processor` which performs all
session-related processing.

Concurrency is supported by the `Processor` (dynamically adjustable
number of threads) and between sessions. Which means that while
one/some session(s) may be blocked, others may not be.
"""

import logging
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.lib.misc import State
from penvm.lib.mqueue import MessageQueue
from penvm.lib.thread import Thread
from penvm.kernels.base.server import OpContext
from penvm.server.processor import Processor

logger = logging.getLogger(__name__)


class Session(BaseObject):
    """Server-side session."""

    def __init__(
        self,
        machine: "Machine",
        sessionid: str,
        kernelname: str = "default",
    ):
        """Initialize.

        Args:
            machine: Owning machine.
            sessionid: Session id.
            kernelname: Kernel name.
        """
        try:
            super().__init__(sessionid, logger)
            tlogger = self.logger.enter()

            self.machine = machine

            self.exit = False
            # TODO: should session/sessionid be passed to Processor()?
            self.imq = MessageQueue()
            self.omq = MessageQueue()
            self.proc = Processor(self, kernelname)
            self.th = None
            self.pinned = False
        finally:
            tlogger.exit()

    def __repr__(self):
        return f"<Session id={self.oid} machine={self.machine}>"

    def is_empty(self) -> bool:
        """Return if session is "empty", and therefore deletable.

        Returns:
            "Emptiness" status.
        """
        # TODO: verify that active_count is an appropriate check
        if self.proc.active_count() == 0 and self.imq.size() == 0 and self.omq.size() == 0:
            return True
        return False

    def is_running(self) -> bool:
        """Return running status.

        Returns:
            Running status.
        """
        return self.th != None

    def run(self):
        """Run session.

        Loops until `self.exit` is `False`.
        """
        try:
            tlogger = self.logger.enter()

            self.proc.start()
            ctxt = OpContext()
            ctxt.machine = self.machine
            ctxt.processor = self.proc
            ctxt.session = self

            try:
                while not self.exit:
                    tlogger.debug("waiting for message ...")
                    req = self.imq.pop()
                    if req == None:
                        # reject dummy "message"
                        continue

                    tlogger.debug(f"popped message and scheduling ({req.payload.get('op')}) ...")
                    if self.machine.debug and not self.oid.startswith("-debug-"):
                        self.machine.schedlock.acquire()
                    self.proc.schedule(ctxt, req)

                    tlogger.debug(f"message scheduled ({req.payload.get('op')})")
            except Exception as e:
                tlogger.warning(f"EXCEPTION ({e})")
                raise

            self.th = None
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def set_kernel(
        self,
        kernel: "Kernel",
    ):
        """Set the kernel to use.

        Args:
            kernel: Kernel.
        """
        self.logger.debug("set kernel")
        self.proc.kernel = kernel

    def start(self):
        """Start the main session thread."""
        try:
            tlogger = self.logger.enter()

            if not self.th:
                try:
                    self.th = Thread(target=self.run)
                    self.th.daemon = True
                    self.th.start()
                except Exception as e:
                    tlogger.debug(f"failed to start ({e})")
                    self.th = None
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def stop(self):
        """Stop the `run` method running in a thread."""
        self.exit = True
        # dummy "message" to wake up thread
        self.imq.put(None)

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            return State(
                "session",
                self.oid,
                {
                    "imq": self.imq.state(),
                    "nimq": self.imq.size(),
                    "nomq": self.omq.size(),
                    "omq": self.omq.state(),
                    "processor": self.proc.state(),
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
