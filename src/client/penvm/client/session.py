#
# penvm/client/session.py

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

"""A client-side session is used to interact with a machine. Each
session has its own unique session id, specified on the client side.
Any number of concurrent sessions are supported, subject to resource
limits.

Each client-side session has its own incoming and outgoing message
queues (not the same as those on the server side but related). All
communication is mediated with these message queues with forwarding
to and from handle automatically (see
[penvm.lib.connection][]).

Convenience methods are provided for interacting at a high level
with the message queues (e.g., `Session.get_response()`).

Each client-side session is set up with access to a selected,
session-specific, client-side kernel interface. Although not
absolutely required, this interface is highly recommended to provide
a function-style interface rather than having to build a `Request`
message.

See [penvm.server.session][] for details.
"""

import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.lib.message import Request
from penvm.lib.mqueue import MessageQueue

logger = logging.getLogger(__name__)


class Session(BaseObject):
    """Client-side session object.

    Provides unique sessionid and access to client-side machine services.

    Low-level methods return the request object. This allows for tracking and
    followup of a session.

    High-level methods return None.
    """

    def __init__(
        self,
        machine: "Machine",
        sessionid: Union[str, None] = None,
        kernelcls: Type["KernelClient"] = None,
    ):
        """Initialize.

        Args:
            machine: Machine owning this session.
            sessionid: Session id.
            kernelcls: `KernelClient` class.
        """
        try:
            super().__init__(sessionid, logger)
            tlogger = self.logger.enter()

            self.machine = machine
            self.imq = MessageQueue()
            self.kernel = kernelcls(self)
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def __repr__(self):
        return f"<Session id={self.oid} machine={self.machine}>"

    def get_response(self) -> "Message":
        """Pull (from remote) and pop from local and return.

        Returns:
            Response message.
        """
        self.pull_response()
        return self.pop_response()

    def new_request(self, op: str, d: Union[dict, None] = None) -> "Message":
        """Return session-specific Request object with `op`.

        Args:
            op: Operation name.
            d: Request payload settings.

        Returns:
            Created request.
        """
        req = Request()
        req.header["session-id"] = self.oid
        req.payload["op"] = op
        if d != None:
            req.payload.update(d)
        return req

    def newput_request(self, op: str, d: Union[dict, None] = None) -> "Message":
        """Create request and *put it out* to be sent.

        Args:
            op: Operation name.
            d: Request payload settings.

        Returns:
            Created request.
        """
        req = self.new_request(op, d)
        return self.put_request(req)

    def pop_response(self) -> "Message":
        """Pop response from local and return.

        Returns:
            Response message.
        """
        return self.imq.pop()

    def pull_response(self, sessionid: Union[str, None] = None):
        """Pull (pop from remote to local) response.

        Args:
            sessionid: Session id.
        """
        self.kernel.session_pop_omq(sessionid)

    def put_request(self, req: "Message") -> "Message":
        """Put the request on the machine OMQ. Return it, also.

        Args:
            req: Request message.

        Returns:
            The Request message, itself.
        """
        self.machine.omq.put(req)
        return req
