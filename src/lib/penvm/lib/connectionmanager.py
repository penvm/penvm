#
# penvm/lib/connectionmanager.py

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
import socket
import threading
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.lib.connection import Listener
from penvm.lib.misc import State

logger = logging.getLogger(__name__)


class ConnectionManager(BaseObject):
    """Manage connections set up by Listener.

    * Listen for new connections (running in a thread).
    * Spawn accepted connections (`Connection` runs its own threads).
    """

    def __init__(
        self,
        machine: "Machine",
        host: str,
        port: int,
        sslcontext: Union["ssl.SSLContext", None] = None,
    ):
        """Set up.

        Args:
            machine: Machine owning this manager.
            host: Host address to listen on. 0.0.0.0 for all
                interfaces.
            port: Port to listen on. 0 for auto assign.
            sslcontext: SSL context for SSL encrypted connections.
        """
        try:
            super().__init__(None, logger)
            tlogger = self.logger.enter()

            self.machine = machine
            self.host = host
            self.port = port
            self.sslcontext = sslcontext

            self.conns = {}
            self.exit = False
            self.listener = Listener(self.machine, host, port, sslcontext)
            # TODO: should listen() be called here?
            self.listener.listen(100)
            self.ltimeout = 30
            self.th = None
            self.init_conn = None
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def drop(
        self,
        connectionid: str,
    ):
        """Drop connection by connection id.

        Args:
            connectionid: Connection id.
        """
        self.logger.debug(f"DROPPED connection {connectionid=} {self.conns.get(connectionid)=}")
        self.conns.pop(connectionid)

    def get(
        self,
        connectionid: str,
    ) -> "Connection":
        """Get connection by connection id.

        Args:
            connectionid: Connection id.

        Returns:
            Connection for connection id.
        """
        return self.conns.get(connectionid)

    def list(self) -> List[str]:
        """Get list of connection ids.

        Returns:
            Connection ids.
        """
        return list(self.conns.keys())

    def run(self):
        """Run.

        Listen for connections and add.

        The initial connection must occur within a short amount of
        time (first-wait) and remain up for the lifetime of the
        machine. Once the initial connection drops, all others are
        dropped and the machine will end up shutting down.
        """

        try:
            tlogger = self.logger.enter()

            CHECK_TIMEOUT = 10.125
            CHECK_TIMEOUT = 120.125
            CHECK_TIMEOUT = 10000000

            try:
                # initial/firstwait timeout
                self.listener.settimeout(self.ltimeout)
                while not self.exit:
                    try:
                        tlogger.debug(
                            f"listening for connection {self.listener.lhost=} {self.listener.lport=} ..."
                        )
                        conn = self.listener.accept()
                        tlogger.debug(f"accepted connection {conn=}")

                        if self.init_conn == None:
                            # set to shutdown on connection close/fail of initial connection
                            self.init_conn = conn
                            conn.onclose = self.shutdown
                        else:
                            # TODO: should be part of accept step?
                            conn.onclose = self.drop

                        self.conns[conn.oid] = conn
                        conn.start()

                        # non-initial/post-firstwait update to timeout
                        if self.ltimeout >= 0 and self.ltimeout != CHECK_TIMEOUT:
                            tlogger.debug("non-intial timeout")
                            self.ltimeout = CHECK_TIMEOUT
                            self.listener.settimeout(self.ltimeout)
                    except (socket.timeout, TimeoutError):
                        # periodic wakeup
                        self.logger.debug(f"listener timed out after {self.ltimeout}")

                        if len(self.conns) == 0 and self.ltimeout == CHECK_TIMEOUT:
                            # cleanup opportunity
                            break

                        # setup check timeout if not already in place
                        if self.ltimeout != CHECK_TIMEOUT:
                            self.ltimeout = CHECK_TIMEOUT
                            self.listener.settimeout(self.ltimeout)
                    except Exception as e:
                        self.logger.debug(f"EXCEPTION ({e})")
            finally:
                # cleanup
                for conn in self.conns.values():
                    conn.close()
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def shutdown(
        self,
        connectionid: str,
    ):
        """Force termination.

        Intended to be called by `Connection.onclose` instead of
        `ConnectionManager.drop()` when the initial connection
        (`init_conn`) is closed.

        Args:
            connectionid: Connection id.
        """
        try:
            tlogger = self.logger.enter()
            self.machine.shutdown(now=True)
            # should not reach here!
        finally:
            tlogger.exit()

    def start(self):
        """Start.

        Background thread to handle connections."""
        try:
            self.logger.debug("starting ...")

            if not self.th:
                try:
                    self.th = threading.Thread(target=self.run)
                    self.th.daemon = True
                    self.th.start()
                except Exception as e:
                    self.logger.critical(f"failed to start ({e}")
                    self.th = None
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            return State(
                "connection-manager",
                None,
                {
                    "connection-ids": self.list(),
                    "nconnections": len(self.conns),
                    "listener": {
                        "timeout": self.ltimeout,
                        "addr": self.listener.lhost,
                        "port": self.listener.lport,
                    },
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
