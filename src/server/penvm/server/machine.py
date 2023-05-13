#
# penvm/server/machine.py

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
import os
import socket
import sys
import threading
import time
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.lib.connectionmanager import ConnectionManager
from penvm.lib.misc import State, get_timestamp, get_uuid1
from penvm.lib.queue import RoutingQueue
from penvm.server.kernelmanager import KernelManager
from penvm.server.sessionmanager import SessionManager

# from penvm.server.storage import StorageManager
from penvm.lib.kvstore import FileKVStore

from penvm.kernels.core.server import Kernel as CoreKernel
from penvm.kernels.default.server import Kernel as DefaultKernel

logger = logging.getLogger(__name__)


class Machine(BaseObject):
    """Server-side machine.

    Provides access to all "managers" (e.g, connection, kernel,
    session, store), main incoming and outgoing message queues, and
    state/control to support debug mode."""

    def __init__(
        self,
        host: str,
        port: int,
        sslprofile: Union[str, None] = None,
        machineid: Union[str, None] = None,
    ):
        """Set up server-side machine.

        Args:
            host: Host address.
            port: Port.
            sslprofile: SSL profile for SSL context.
            machineid: Machine id. Generated if not provided.
        """
        try:
            super().__init__(machineid, logger)
            tlogger = self.logger.enter()

            self.sslprofile = sslprofile
            self.sslcontext = self.get_sslcontext(self.sslprofile)

            self.connmgr = ConnectionManager(self, host, port, self.sslcontext)
            self.kernelmgr = KernelManager(self)
            self.sessmgr = SessionManager(self)
            # self.storemgr = StorageManager(self)
            self.fkvstore = FileKVStore(f"/tmp/penvm-store-{self.oid}-{get_uuid1()}")

            for kernel_cls in [CoreKernel, DefaultKernel]:
                self.kernelmgr.set(kernel_cls.name, kernel_cls())

            self._background = False
            self.debug = False

            self.schedlock = threading.Lock()
            self.imq = RoutingQueue(put=self.imq_put)
            self.omq = RoutingQueue(put=self.omq_put)

        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def features(self) -> Dict:
        """Get a dictionary of features.

        Returns:
            Dictionary of features.
        """
        try:
            # languages
            d = {
                "language": "python",
                "python": {
                    "platform": sys.platform,
                    "version": list(sys.version_info),
                },
                "library": {},
            }

            # libraries
            try:
                import numpy

                d["library"]["numpy"] = numpy.version.full_version
            except:
                pass

            return d
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")

    def get_addr_port(self) -> Tuple[str, int]:
        """Get machine listener address and port.

        Returns:
            Tuple of listener host and port.
        """
        return (self.connmgr.listener.lhost, self.connmgr.listener.lport)

    def get_session(
        self,
        sessionid: str = "default",
    ) -> "Session":
        """Get/create session.

        Args:
            sessionid (str): Session id (optional).

        Returns:
            Session.
        """
        # TODO: no default. must be provided by client
        return self.sessmgr.setdefault(sessionid)

    def get_sslcontext(
        self,
        sslprofile: Union[str, None],
    ) -> Union["ssl.SSLContext", None]:
        """Load server-side SSLContext based on named ssl profile.

        Args:
            sslprofile: SSL profile name.

        Returns:
            SSLContext.
        """
        if sslprofile:
            try:
                import ssl

                sslprofile_dir = os.path.expanduser(f"~/.penvm/ssl/{sslprofile}")
                sslcontext = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                # sslcontext.verify_mode = ssl.CERT_REQUIRED
                sslcontext.load_cert_chain(
                    certfile=f"{sslprofile_dir}/server.crt",
                    keyfile=f"{sslprofile_dir}/server.key",
                )
                return sslcontext
            except Exception as e:
                logger.debug(f"ssl required but missing for ssl profile ({sslprofile})")
                raise Exception(f"ssl required but missing for ssl profile ({sslprofile})")

    def imq_put(
        self,
        msg: "Message",
    ):
        """Triage message and put on proper session incoming message
        queue.

        Note: This is where incoming requests result in session
        creation, then request processing!

        Args:
            msg: Message to enqueue.
        """
        try:
            tlogger = self.logger.enter()

            sessionid = msg.header.get("session-id", "default")
            sess = self.sessmgr.setdefault(sessionid)
            if sess:
                sess.imq.put(msg)
            else:
                # DROP!
                tlogger.warning(f"dropped message for session {sessionid}")
                pass
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def omq_put(
        self,
        msg: "Message",
    ):
        """Triage message and put on proper connection outgoing
        message queue.

        Args:
            msg: Message to enqueue.
        """
        try:
            tlogger = self.logger.enter()

            connectionid = msg.header.get("connection-id")
            conn = self.connmgr.get(connectionid)
            if conn:
                conn.omq.put(msg)
            else:
                # DROP!
                tlogger.warning("omq_put dropped message")
                pass
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def pop_session(
        self,
        sessionid: str = "default",
    ) -> "Session":
        """Pop session.

        Args:
            sessionid: Session id.

        Returns:
            Session.
        """
        return self.sessmgr.pop(sessionid)

    def run(self):
        """Run "runnable" managers."""
        try:
            tlogger = self.logger.enter()
            self.connmgr.run()
            # self.sessmgr.run()
            # self.opsmgr.run()
        finally:
            tlogger.exit()

    def set_debug(
        self,
        enabled: bool,
    ):
        """Set debug mode

        Args:
            enabled: New state for debug mode.
        """
        try:
            tlogger = self.logger.enter()
            tlogger.debug("setting debug ({enabled})")

            self.debug = enabled
            if self.debug:
                try:
                    if self.schedlock.locked():
                        self.schedlock.release()
                except Exception as e:
                    pass
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def shutdown(
        self,
        now: bool = False,
    ):
        """Shutdown machine.

        Args:
            now: To shut down "now".
        """
        try:
            tlogger = self.logger.enter()
            tlogger.debug("SHUTTTING DOWN ...")
            if now:
                os._exit(0)
        finally:
            tlogger.exit()

    def start(self):
        """Start machine."""
        try:
            tlogger = self.logger.enter()
            self.connmgr.start()
            self.sessmgr.start()
            self.kernelmgr.start()
        finally:
            tlogger.exit()

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            uname = os.uname()
            return State(
                "machine",
                self.oid,
                {
                    "debug": self.debug,
                    "features": self.features(),
                    "host": socket.gethostname(),
                    "schedlock": self.schedlock.locked(),
                    "timestamp": get_timestamp(),
                    "uname": {
                        "machine": uname.machine,
                        "nodename": uname.nodename,
                        "release": uname.release,
                        "sysname": uname.sysname,
                        "version": uname.version,
                    },
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")

    def step_debug(self):
        """Allow a step if in debug mode."""
        try:
            tlogger = self.logger.enter()
            tlogger.debug("stepping debug")
            if self.debug:
                try:
                    if self.schedlock.locked():
                        self.schedlock.release()
                except Exception as e:
                    pass
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def stop(self):
        """Step machine."""
        try:
            tlogger = self.logger.enter()
            self.connmgr.stop()
            # self.sessmgr.stop()
        finally:
            tlogger.exit()

    def wait(self):
        """Wait (indefinitely) for machine to exit."""
        try:
            tlogger = self.logger.enter()
            time.sleep(1000000000)
        finally:
            tlogger.exit()
