#
# penvm/server/sessionmanager.py

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

"""All sessions are managed by `SessionManager`. Creation, removal,
cleanup, and lookups are centrally handled by the `SessionManager`.
"""

import logging
import threading
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.lib.misc import State
from penvm.server.session import Session

logger = logging.getLogger(__name__)


class SessionManager(BaseObject):
    """Manages server-side sessions.

    All sessions are managed by a SessionManager owned by the machine."""

    def __init__(
        self,
        machine: "Machine",
    ):
        """Initialize.

        Args:
            machine: Owning machine.
        """
        try:
            super().__init__(None, logger)
            tlogger = self.logger.enter()

            self.machine = machine

            self.lock = threading.Lock()
            self.sessions = {}
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def cleanup(
        self,
        sessionid: str,
    ):
        """Clean up session (if "empty").

        Args:
            sessionid: Session id.
        """
        try:
            self.lock.acquire()
            sess = self.sessions.get(sessionid)
            if sess and not sess.pinned:
                if sess.is_empty():
                    sess.stop()
                    self.pop(sessionid)
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            self.lock.release()

    def delete(
        self,
        sessionid: str,
    ):
        """Delete (forced) a session by session id.

        Args:
            sessionid: Session id.
        """
        try:
            tlogger = self.logger.enter()

            sess = self.sessions.pop(sessionid)
            if sess:
                # delete
                pass
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def get(
        self,
        sessionid: str,
    ) -> "Session":
        """Get a session by session id.

        Args:
            sessionid: Session id.

        Returns:
            Session.
        """
        try:
            self.lock.acquire()
            return self.sessions.get(sessionid)
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            self.lock.release()

    def list(self) -> List[str]:
        """List sessions by session id.

        Returns:
            Session ids.
        """
        return list(self.sessions.keys())

    def pop(self, sessionid: str) -> "Session":
        """Pop a session by session id.

        Args:
            sessionid: Session id.

        Returns:
            Session.
        """
        try:
            self.lock.acquire()
            sess = self.sessions.pop(sessionid)
            return sess
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            self.lock.release()

    def setdefault(
        self,
        sessionid: Union[str, None],
        default: Union["Session", None] = None,
    ) -> "Session":
        """Get a session (or create new one if not present) by session
        id.

        Args:
            sessionid: Session id.
            default: Session if session not found.

        Returns:
            Session.
        """
        try:
            self.lock.acquire()
            session = self.sessions.get(sessionid)
            if not session:
                session = self.sessions[sessionid] = default or Session(self.machine, sessionid)
                session.start()
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            self.lock.release()
        return session

    def start(self):
        """Start.

        NOOP. No background threads."""
        pass

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            return State(
                "session-manager",
                None,
                {
                    "nsessions": len(self.sessions),
                    "session-ids": self.list(),
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
