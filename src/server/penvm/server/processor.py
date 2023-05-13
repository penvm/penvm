#
# penvm/server/processor.py

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
import threading
import time
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union


from penvm.lib.base import BaseObject
from penvm.lib.misc import State
from penvm.lib.thread import Thread, ThreadInterrupt
from penvm.lib.semaphore import AdjustableSemaphore

logger = logging.getLogger(__name__)


class Processor(BaseObject):
    """Runs kernel operations.

    Kernel operations are scheduled by the session and run on the
    processor. Operations are run on a selected kernel. Up to
    `max_threads` messages can be processsed at a time.

    The number of running "threads" can be adjusted dynamically with
    `max_threads` limiting the number of running thread. Scheduling of
    new threads resumes when the number of running threads goes below
    `max_threads`."""

    def __init__(
        self,
        session: "Session",
        kernelname: str = "default",
    ):
        """Initialize.

        Args:
            session: Owning session.
            kernelname: Kernel name.
        """
        try:
            super().__init__(None, logger)
            tlogger = self.logger.enter()

            self.session = session
            self.kernelname = kernelname

            self.kill = False
            self.max_threads = 1
            self.kernel = self.session.machine.kernelmgr.get(kernelname)
            self.runsem = AdjustableSemaphore(self.max_threads)
            self.th = None
            self.threads = set()
            self.reqid2thread = {}
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def __repr__(self):
        return f"<Processor id={self.oid} max_threads={self.max_threads} nthreads={self.active_count()}>"

    def active_count(self) -> int:
        """Get number of running threads.

        Returns:
            Number of running threads.
        """
        return len(self.threads)

    def get_thread_reqids(self) -> List[str]:
        """Get list of request ids for each running thread.

        Returns:
            List of request ids.
        """
        return list(self.reqid2thread.keys())

    def run_thread(self, fn: Callable, fargs: List):
        """Runs a thread (to handle a request).

        Machinery manages creating a context, starting a thread,
        adhering to the thread limit, `ThreadInterrupt` handling,
        tracking information, and `runsem` semaphore.

        Args:
            fn: Function to call.
            fargs: Arguments to pass to function.
        """

        def _run(reqid, target, *args, **kwargs):
            try:
                # tlogger = self.logger.enter()
                try:
                    tlogger.debug(f"_run calling target={target} args={args}")
                    target(*args)
                    tlogger.lap(f"target {target} run completed ")
                except ThreadInterrupt as e:
                    tlogger.info(f"thread interrupted {threading.current_thread()}")
                except Exception as e:
                    tlogger.debug(f"_run exception ({traceback.format_exc()}")

                try:
                    tlogger.debug(f"removing thread {threading.current_thread()}...")
                    self.threads.discard(threading.current_thread())
                    self.reqid2thread.pop(reqid, None)
                    tlogger.debug(f"removed thread {threading.current_thread()}")

                    if 0:
                        # clean up "empty" sessions unless pinned
                        tlogger.debug("checking to clean up")
                        self.session.machine.sessmgr.cleanup(self.session.oid)

                except Exception as e:
                    tlogger.debug(f"thread cleanup EXCEPTION ({e})")
                tlogger.debug(f"releasing runsem ({self.session.oid=})...")
                self.runsem.release()
                tlogger.debug(f"release runsem ({self.session.oid=})")
            except Exception as e:
                self.logger.warning(f"EXCEPTION ({e})")
            finally:
                # tlogger.exit()
                pass

        try:
            tlogger = self.logger.enter()

            tlogger.debug(f"acquiring runsem ({self.session.oid=})...")
            self.runsem.acquire()
            tlogger.debug(f"acquired runsem ({self.session.oid=})")

            _opname, _ctxt, _req = fargs
            reqid = _req.header.get("id")
            sessionid = _ctxt.session.oid

            args = [reqid, fn] + list(fargs)
            th = Thread(target=_run, name=reqid, args=args)
            th.daemon = True
            self.threads.add(th)
            self.reqid2thread[reqid] = th
            th.start()
            # cleanup is done in _run()
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def schedule(self, ctxt: "OpContext", req: "Message"):
        """Schedule thread to run the requested kernel operation.

        Args:
            ctxt: Context in which to run operation.
            req: Request message.
        """
        try:
            tlogger = self.logger.enter()

            opname = req.payload.get("op")
            if opname:
                tlogger.info(f"opname={opname}")
                self.run_thread(self.kernel.run, (opname, ctxt, req))
            else:
                tlogger.warning(f"opname={opname} not found")
                # TODO: does this hang here?
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def set_max_threads(self, n: int):
        """Set the upper limit of the number of threads to run.

        Updates the `runsem` semaphore to allow acquisition, if
        possible.

        Args:
            n: Maximum number of threads.
        """
        try:
            self.max_threads = max(1, n)
            self.runsem.adjust(self.max_threads)
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")

    def start(self):
        """Start main processor thread."""
        try:
            tlogger = self.logger.enter()
            return

            # TODO: are start()/run() necessary?
            if not self.th:
                self.th = Thread(target=self.run)
                self.th.daemon = True
            self.th.start()

        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            return State(
                "processor",
                self.oid,
                {
                    "kernel": self.kernelname,
                    "max-threads": self.max_threads,
                    "nthreads": len(self.threads),
                    "runsem": {
                        "_lock-locked": self.runsem._lock.locked(),
                        "_waitlock-locked": self.runsem._waitlock.locked(),
                        "count": self.runsem.count(),
                        "max": self.runsem.max(),
                    },
                    "threads": [th.state() for th in self.threads],
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")

    def terminate_thread(self, reqid: str):
        """Forcefully terminate a running thread.

        A `ThreadInterrupt` exception is raised for the thread.

        Args:
            reqid: Request id.
        """
        try:
            tlogger = self.logger.enter()
            tlogger.info("terminating thread ({reqid})")

            th = self.reqid2thread.get(reqid)
            th.terminate()
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()
