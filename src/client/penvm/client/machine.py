#
# penvm/client/machine.py

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

import importlib
import logging
import threading
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.client.session import Session
from penvm.lib.base import BaseObject
from penvm.lib.connection import ClientConnection
from penvm.lib.misc import MachineConnectionSpec
from penvm.lib.mqueue import MessageQueue
from penvm.lib.queue import RoutingQueue

logger = logging.getLogger(__name__)


class Machine(BaseObject):
    """Client-side representation of a server-side machine."""

    def __init__(
        self,
        host: str,
        port: int,
        sslprofile: Union[str, None] = None,
        machineid: Union[str, None] = None,
    ):
        """Initialize.

        Set up client-side machine representation.

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

            self.conn = ClientConnection(self, host, port, self.sslcontext)
            self.conn.connect()
            # time.sleep(0.5)
            self.conn.start()

            self.exit = False

            self.imq = RoutingQueue(put=self.imq_put)
            if self.conn:
                self.omq = RoutingQueue(put=self.omq_put)
            else:
                # for testing without a connection
                self.omq = MessageQueue()

            self.lock = threading.Lock()
            self.kernels = {}
            self.sessions = {}

            # standard kernels
            self.load_kernel("core", "penvm.kernels.core")
            self.load_kernel("default", "penvm.kernels.default")
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def __repr__(self):
        return f"<Machine id={self.oid} conn={self.conn} nsessions={len(self.sessions)}>"

    def get_debug_session(self, sessionid: str = None) -> "Session":
        """Get debug-specific session.

        Debug sessions (should) start with "-debug-". They are treated
        specially by machines: they are not subject to the debug mode.

        Args:
            sessionid: Session id (suffix) for a debug session.

        Returns:
            Session for debugging.
        """
        return self.get_session("""-debug-{sessionid or ""}""")

    def get_kernel_class(self, kernel_name: str) -> Type["KernelClient"]:
        """Get kernel client class.

        Args:
            kernel_name: Kernel name.

        Returns:
            Kernel client class for `kernel_name`.
        """
        return self.kernels.get(kernel_name)

    def get_machconnspec(self) -> MachineConnectionSpec:
        """Get `MachineConnectionSpec` object for this machine.

        Returns:
            `MachineConnectionSpec` of object for this machine.
        """
        return MachineConnectionSpec(machine=self)

    def get_machconnstr(self) -> str:
        """Get machine connection string for this machine.

        Returns:
            Machine connection string.
        """
        return str(self.get_machconnspec())

    def get_session(
        self, sessionid: Union[str, None] = None, kernelname: str = "default"
    ) -> "Session":
        """Get session.

        Args:
            sessionid: Session id. Generated if not provided.
            kernelname: Kernel name.

        Returns:
            New `Session` for `sessionid` and `kernelname`.
        """
        try:
            tlogger = self.logger.enter()
            tlogger.debug(f"getting session for sessionid={sessionid}")

            # lock
            self.lock.acquire()
            try:
                kernelcls = self.get_kernel_class(kernelname)
                if kernelcls == None:
                    tlogger.debug(f"kernel ({kernelname}) not found")
                    return

                session = self.sessions.get(sessionid) if sessionid != None else None
                if session == None:
                    session = Session(self, sessionid, kernelcls)
                    session = self.sessions.setdefault(session.oid, session)
            finally:
                self.lock.release()
            return session
        finally:
            tlogger.exit()

    def get_sslcontext(self, sslprofile: str) -> "SSLContext":
        """Load client-side SSLContext based on named ssl profile.

        Args:
            sslprofile (str): SSL profile name.

        Returns:
            `SSLContext` for ssl profile.
        """
        if sslprofile:
            try:
                import ssl

                sslcontext = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                sslcontext.check_hostname = False
                sslcontext.verify_mode = ssl.CERT_NONE
                return sslcontext
            except Exception as e:
                logger.debug(f"ssl required but missing for ssl profile ({sslprofile})")
                raise Exception(f"ssl required but missing for ssl profile ({sslprofile})")

    def imq_put(self, msg: "Message"):
        """Put a message on the IMQ.

        Args:
            msg: Message object.
        """
        try:
            tlogger = self.logger.enter()

            sessionid = msg.header.get("session-id")

            tlogger.debug(f"imq_put sessionid={sessionid}")
            sess = self.sessions.get(sessionid)
            if sess:
                sess.imq.put(msg)
            else:
                # DROP!
                tlogger.debug("imp_put dropping message")
        finally:
            tlogger.exit()

    def list_kernels(self) -> List[str]:
        """Get list of kernel names.

        Returns:
            List of kernel names.
        """
        try:
            tlogger = self.logger.enter()
            return list(self.kernels.keys())
        finally:
            tlogger.exit()

    def load_assets(self, assets):
        """Load/copy assets to the machines.

        NIY.
        """
        try:
            tlogger = self.logger.enter()
        finally:
            tlogger.exit()

    def load_kernel(self, kernel_name: str, pkgname: str) -> "KernelClient":
        """Load/copy kernel to the machine.

        Kernel support is provided as:

        ```
            <pkg>/
              client.py
                KernelClient
              server.py
                Kernel
        ```

        Args:
            kernel_name: Kernel name.
            pkgname: Package name (as a string).

        Returns:
            Kernel client class.
        """
        try:
            tlogger = self.logger.enter()
            if kernel_name in self.kernels:
                # TODO: test? allow overwrite for now?
                pass

            try:
                mod = importlib.import_module(".client", pkgname)
                cls = getattr(mod, "KernelClient")
                self.kernels[kernel_name] = cls
            except Exception as e:
                pass

            # TODO: send kernel to machine (server side)
            return cls
        finally:
            tlogger.exit()

    def omq_put(self, msg: "Message"):
        """Put a message on the OMQ.

        Args:
            msg: Message to enqueue.
        """
        try:
            tlogger = self.logger.enter()
            self.conn.omq.put(msg)
        finally:
            tlogger.exit()

    def start(self):
        """Start machine."""
        try:
            tlogger = self.logger.enter()
            if 0 and self.conn:
                self.conn.connect()
                self.conn.start()
        finally:
            tlogger.exit()

    def stop(self):
        """Stop machine.

        Set `exit` attribute to signal machine should exit.
        """
        try:
            tlogger = self.logger.enter()
            self.exit = True
        finally:
            tlogger.exit()
