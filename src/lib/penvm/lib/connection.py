#
# penvm/lib/connection.py

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

"""Wrappers for low-level connections.
"""

import logging
import socket
import time
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.lib.message import Message
from penvm.lib.misc import State
from penvm.lib.mqueue import MessageQueue
from penvm.lib.queue import RoutingQueue
from penvm.lib.thread import Thread


DEBUG = False
if DEBUG:
    import os
    from penvm.lib.debug import DataDumper

    ddrecv = DataDumper(f"/tmp/penvm-ddrecv-{os.getpid()}.log")
    ddsend = DataDumper(f"/tmp/penvm-ddsend-{os.getpid()}.log")

logger = logging.getLogger(__name__)

HOSTNAME = socket.gethostname()

BLK_SZ_LEN = 8
BLK_SZ_FMT = b"%%0.%dd" % BLK_SZ_LEN
BLK_SZ_MAX = 100_000_000 - 1

KB = 1024
MB = KB * 1024
GB = MB * 1024


class ConnectionError(Exception):
    pass


class Listener(BaseObject):
    """Listener side/socket."""

    def __init__(
        self,
        machine: "Machine",
        host: str,
        port: int,
        sslcontext: Union["ssl.SSLContext", None] = None,
    ):
        try:
            super().__init__(None, logger)
            tlogger = self.logger.enter()

            self.machine = machine
            self.host = host
            self.port = port
            self.sslcontext = sslcontext

            self.lsock = None
            self.lhost = None
            self.lport = None
        finally:
            tlogger.exit()

    def accept(self):
        try:
            tlogger = self.logger.enter()
            tlogger.debug("accepting ...")

            sock, addr = self.lsock.accept()
            tlogger.debug(f"accepted from sock={sock} addr={addr}")

            if self.sslcontext:
                # TODO: ensure (failed/slow/non-ssl) ssl negotiation does not block
                tlogger.debug(f"setting up ssl {self.sslcontext=}...")
                sock = self.sslcontext.wrap_socket(sock, server_side=True)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                tlogger.debug(f"ssl socket ({sock=})")

            tlogger.debug("creating ServerConnection ...")

            return ServerConnection(self.machine, addr[0], addr[1], self.sslcontext, sock)
        except Exception as e:
            tlogger.debug(f"EXCEPTION ({e})")
            # allow timeout to percolate up
            raise
        finally:
            tlogger.exit()

    def is_listening(self):
        return self.lsock != None

    def listen(
        self,
        n: int = 1,
    ):
        """Set up to listen for connection.

        Args:
            n: Number of outstanding socket connection requests
                allowed.
        """
        try:
            tlogger = self.logger.enter()

            self.lsock = lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lsock.bind((self.host, self.port))
            lsock.listen(n)
            self.lhost, self.lport = lsock.getsockname()
        finally:
            tlogger.exit()

    def settimeout(
        self,
        delay: int,
    ):
        """Set timeout.

        Args:
            delay: Seconds before timing out on idle connection.
        """
        self.lsock.settimeout(delay)


class SocketConnection(BaseObject):
    """Socket connection.

    Provides the interface to the socket."""

    def __init__(
        self,
        host: str,
        port: int,
        sslcontext: Union["ssl.SSLContext", None] = None,
        sock: Union["socket.socket", None] = None,
    ):
        """Setup.

        Args:
            host: Host address.
            port: Port.
            sslcontext: SSL context used for wrapping a regular socket
                to provide encryption.
            sock: Network socket.
        """
        try:
            super().__init__(None, logger)
            tlogger = self.logger.enter()

            self.host = host
            self.port = port
            self.sslcontext = sslcontext
            self.sock = sock
        finally:
            tlogger.exit()

    def __repr__(self):
        return f"<SocketConnection id={self.oid} host={self.host} port={self.port}>"

    def connect(self):
        """Connection to server."""
        try:
            tlogger = self.logger.enter()

            try:
                sock = None
                for i in range(5):
                    try:
                        tlogger.debug(f"connecting to server ({self.host}) ({self.port}) ...")
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        if self.sslcontext:
                            sock = self.sslcontext.wrap_socket(sock, server_side=False)
                            tlogger.debug(f"socket ssl wrapped")
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                        tlogger.debug(f"connection info host={self.host} port={self.port}")
                        sock.connect((self.host, self.port))
                        break
                    except socket.error as e:
                        # TODO: clean up sock if/as needed
                        tlogger.debug(f"socket error ({e})")
                        time.sleep(2)
                    except Exception as e:
                        if i == 5:
                            raise

                tlogger.debug("connected")
                self.sock = sock
            except Exception as e:
                tlogger.debug(f"connection failed ({e})")
        finally:
            tlogger.exit()

    def recv(
        self,
        sz: int,
    ) -> bytes:
        """Receive (all) bytes.

        Args:
            sz: Number of bytes.

        Returns:
            Bytes received.
        """
        l = []
        while sz > 0:
            b = self.sock.recv(sz)
            # TODO: handle signal? not needd since v3.5!
            if len(b) == 0:
                self.logger.debug(f"connection closed ({sz=}) ({l=})")
                raise Exception("connection closed")
                break
            # self.logger.debug(f"recv {len(b)=}")
            l.append(b)
            sz -= len(b)
        # TODO: improve this!?!
        return b"".join(l)

    def recvblk(
        self,
        blkszlen: int = BLK_SZ_LEN,
    ) -> bytes:
        """Receive and return a block.

        Args:
            blkszlen: Maximum receivable block size.

        Returns:
            Bytes received.
        """
        try:
            tlogger = self.logger.enter()

            try:
                sz = int(self.recv(blkszlen))
                t0 = time.time()
                b = self.recv(sz)
                elapsed = time.time() - t0
                if DEBUG:
                    ddrecv.writebytes(b)
                tlogger.lap(f"size ({len(b)}) perf ({len(b)/MB/elapsed:.4f} MB/s)")
                return b

            except Exception as e:
                # traceback.print_exc()
                raise ConnectionError("failed to receive block")
        finally:
            tlogger.exit()

    def xsend(
        self,
        b: bytes,
    ):
        """Send all bytes."""
        self.sock.sendall(b)

    def send(
        self,
        b: bytes,
    ):
        """Send bytes.

        Args:
            b: Bytes to send.
        """
        total = 0
        sz = len(b)
        while total < sz:
            count = self.sock.send(b)
            if count == 0:
                raise Exception("send failed")
            total += count
            b = b[count:]

        # print(f"------------ send ({total=}) ({sz=})")
        self.logger.debug(f"------------ send ({total=}) ({sz=})")
        return total

    def sendblk(
        self,
        b: bytes,
        blkszmax: int = BLK_SZ_MAX,
        blkszfmt: int = BLK_SZ_FMT,
    ):
        """Send a block (as bytes).

        Size information is sent over the stream before the data.

        Args:
            b: Bytes to send.
            blkszmax: Maximum sendable block size.
            blkszfmt: Block size field format.
        """
        try:
            tlogger = self.logger.enter()
            try:
                if len(b) > blkszmax:
                    raise Exception(f"block exceeds size ({blkszmax})")

                b = (blkszfmt % len(b)) + b
                t0 = time.time()
                self.send(b)
                elapsed = time.time() - t0
                if DEBUG:
                    ddsend.writebytes(b)
                # print(f"size ({len(b)}) perf ({len(b)/MB/elapsed:.4f} MB/s)")
                tlogger.lap(f"size ({len(b)}) perf ({len(b)/MB/elapsed:.4f} MB/s)")
            except Exception as e:
                traceback.print_exc()
                raise ConnectionError("failed to send block")
        finally:
            tlogger.exit()

    def close(self):
        """Close connection."""
        self.logger.debug("close")
        self.sock.close()
        self.sock = None

    def is_alive(self) -> bool:
        """Indicate if connection is alive or not.

        Returns:
            Alive status.
        """
        return True

    def is_connected(self) -> bool:
        """Indicate if connected or not.

        Returns:
            Connection status.
        """
        return self.sock != None


class MessageConnection(SocketConnection):
    """Message connection.

    Provides the interface to the socket with support for messages.

    The headers of incoming and outgoing message are all updated with
    the connection id."""

    def __init__(
        self,
        host: str,
        port: int,
        sslcontext: Union["ssl.SSLContext", None] = None,
        sock: Union["socket.socket", None] = None,
    ):
        """Setup.

        Args:
            host: Host address.
            port: Port.
            sslcontext: SSL context used for wrapping a regular
                socket to provide encryption.
            sock: Network socket.
        """
        try:
            super().__init__(host, port, sslcontext, sock)

            # self.logger already set up
            tlogger = self.logger.enter()
        finally:
            tlogger.exit()

    def __repr__(self):
        return f"<MessageConnection id={self.oid} host={self.host} port={self.port}>"

    def recvmsg(self) -> "Message":
        """Receive message and return (as generic Message).

        Returns:
            Received message.
        """
        try:
            tlogger = self.logger.enter()

            b = self.recvblk()
            t0 = time.time()
            h, p = Message.decode(b)
            elapsed = time.time() - t0
            tlogger.lap(f"size ({len(b)}) decode time ({elapsed}:.4f)")
            # print(f"size ({len(b)}) decode time ({elapsed}:.4f)")
            return Message(h, p)
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
            raise
        finally:
            tlogger.exit()

    def sendmsg(
        self,
        msg: "Message",
    ):
        """Send message (serialized).

        Args:
            msg: Message to send.
        """
        try:
            tlogger = self.logger.enter()

            t0 = time.time()
            b = msg.encode()
            elapsed = time.time() - t0
            tlogger.lap(f"size ({len(b)}) encode time ({elapsed}:.4f)")
            self.sendblk(b)
            # print(f"size ({len(b)}) encode time ({elapsed}:.4f)")
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
            raise
        finally:
            tlogger.exit()


class Connection(MessageConnection):
    """Connection.

    Provides the interface between the network socket and message
    queues (incoming, outgoing).

    The headers of incoming and outgoing message are all augmented
    with the connection id."""

    def __init__(
        self,
        machine: "Machine",
        host: str,
        port: int,
        sslcontext: Union["ssl.SSLContext", None] = None,
        sock: Union["socket.socket", None] = None,
        onclose: Union[Callable, None] = None,
    ):
        """Initialize.

        Args:
            machine: Machine.
            host: Host address.
            port: Port.
            sslcontext: SSL Context.
            onclose: Function to call to when connection is closed.
        """
        try:
            super().__init__(host, port, sslcontext, sock)
            self.machine = machine
            self.onclose = onclose

            self.exit = False
            self.imq = RoutingQueue(put=self.imq_put)
            self.omq = MessageQueue()
            self.recvmsgs_th = None
            self.sendmsgs_th = None
        finally:
            pass

    def __repr__(self):
        return f"<Connection id={self.oid} machine={self.machine.oid} host={self.host} port={self.port}>"

    def close(self):
        """Close connection."""
        try:
            tlogger = self.logger.enter()
            super().close()
            self.exit = True
            tlogger.debug(f"onclose ({self.onclose})")
            if self.onclose:
                self.onclose(self.oid)
        except Exception as e:
            tlogger.debug(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def imq_put(
        self,
        msg: "Message",
    ):
        """Put message on IMQ.

        Args:
            msg: Message to enqueue.
        """
        self.machine.imq.put(msg)

    def recvmsgs(self):
        """Receive messages over connection.

        Loops while `self.exit` is `True`.
        """
        try:
            tlogger = self.logger.enter()

            while not self.exit:
                try:
                    tlogger.debug("recvmsgs waiting ...")
                    msg = self.recvmsg()

                    tlogger.debug("recvmsgs message received")
                    # patch header with `Connection.oid`
                    msg.header["connection-id"] = self.oid

                    self.imq.put(msg)

                    tlogger.debug("recvmsgs message put")
                except Exception as e:
                    tlogger.debug(f"EXCEPTION ({e})")
                    # raise
                    # TODO: close/cleanup should be elsewhere
                    self.close()
                    break
        finally:
            tlogger.exit()

    def sendmsgs(self):
        """Send message over connection.

        Loops while `self.exit` is `True`.
        """
        try:
            tlogger = self.logger.enter()

            while not self.exit:
                try:
                    tlogger.debug("sendmsgs waiting ...")
                    msg = self.omq.pop()

                    tlogger.debug("sendmsgs popped message")
                    msg.header["connection-id"] = self.oid
                    self.sendmsg(msg)

                    tlogger.debug("sendmsgs sent")
                except Exception as e:
                    tlogger.debug(f"EXCEPTION ({e})")
                    self.close()
                    raise
                    break
        finally:
            tlogger.exit()

    def start(self):
        """Start recv and send message handing."""
        try:
            tlogger = self.logger.enter()

            # TODO: fix to handle ssl situation
            if self.sock == None:
                self.connect()
                # raise Exception("no connection!!!")

            if not self.sendmsgs_th:
                try:
                    tlogger.debug("starting sendmsgs ...")

                    self.sendmsgs_th = Thread(target=self.sendmsgs)
                    self.sendmsgs_th.daemon = True
                    self.sendmsgs_th.start()
                except Exception as e:
                    tlogger.debug(f"starting sendmsgs EXCEPTION ({e})")
                    self.sendmsgs_th = None

            if not self.recvmsgs_th:
                try:
                    tlogger.debug("starting recvmsgs ...")

                    self.recvmsgs_th = Thread(target=self.recvmsgs)
                    self.recvmsgs_th.daemon = True
                    self.recvmsgs_th.start()
                except Exception as e:
                    tlogger.debug(f"starting recvmsgs EXCEPTION ({e})")
                    self.recvmsgs_th = None
        except Exception as e:
            tlogger.debug(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            peer = self.sock.getpeername()
            return State(
                "connection",
                self.oid,
                {
                    "initial-connection": self.machine.connmgr.init_conn == self,
                    "host": self.host,
                    "port": self.port,
                    "peer-host": peer[0],
                    "peer-port": peer[1],
                    "nimq": self.imq.size(),
                    "nomq": self.omq.size(),
                    "ssl": self.sslcontext != None,
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")


class ClientConnection(Connection):
    """Client-side message queue connection."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        try:
            super().__init__(*args, **kwargs)
            tlogger = self.logger.enter()
        finally:
            tlogger.exit()


class ServerConnection(Connection):
    """Server-side message queue connection.

    Returned by Listener."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        try:
            super().__init__(*args, **kwargs)
            tlogger = self.logger.enter()
        finally:
            tlogger.exit()
