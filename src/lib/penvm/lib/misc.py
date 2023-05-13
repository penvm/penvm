#
# penvm/lib/misc.py

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

"""Collection of miscellaneous code.
"""

# import secrets
import logging
from logging import LoggerAdapter as _LoggerAdapter
from threading import Lock
import time
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from uuid import uuid1

logger = logging.getLogger(__name__)

VERSION = (0, 1, 0)


class LogMark:
    """Provides unique id and time information: t0 (start time) and
    tlast (lap time)."""

    def __init__(self):
        """Initialize."""
        self.t0 = time.time()
        self.tlast = self.t0
        self.uuid = get_log_uuid()

    def elapsed(self) -> float:
        """Return elapsed time since object initialize.

        Returns:
            Elapsed time in seconds.
        """
        t1 = time.time()
        return t1, t1 - self.t0

    def lap(self) -> Tuple[float, float, float]:
        """Return triple (now, elasped since init, elapsed since last lap).

        Returns:
            Tuple of (not, elapsed since init, elapsed since lap) in
            seconds.
        """
        tnow = time.time()
        tlast = self.tlast
        self.tlast = tnow
        return tnow, tnow - self.t0, tnow - tlast

    def reset(self):
        """Reset the "init" time."""
        self.t0 = time.time()


class LoggerAdapter(_LoggerAdapter):
    """Log adapter which provides "owner" information as a prefix in
    the log entry.
    """

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Initialize."""
        super().__init__(*args, **kwargs)

        o = self.extra.get("self")
        if o != None:
            classname = o.__class__.__name__
            idval = self.extra.get("id", "-")
            self.extra["prefix"] = f"""{classname}[{idval}]"""
        else:
            self.extra["prefix"] = self.extra.get("prefix", "")

    def process(
        self,
        msg: str,
        kwargs: dict,
    ) -> str:
        """Return processed log message.

        Args:
            msg: Message format string.
            kwargs: Dictionary containers items for populating `msg.

        Return:
            Processed message format string.
        """
        return "%s: %s" % (self.extra.get("prefix"), msg), kwargs

    def enter(
        self,
        *args: List,
        **kwargs: Dict,
    ) -> "TaggedLoggerAdapter":
        """Set up a TaggedLoggerAdapter, call enter(), return new
        adapter.

        Args:
            args: Log message args.
            kwargs: Log message kwargs.

        Returns:
            TaggedLoggerAdapter.
        """
        tlogger = TaggedLoggerAdapter(self, stacklevel=4)
        tlogger.enter(*args, stacklevel=5, **kwargs)
        return tlogger


class MachineConnectionSpec:
    """Machine connection spec.

    Information required to establish a connection to a machine.
    """

    def __init__(
        self,
        machconnstr: Union[str, None] = None,
        config: Union[dict, None] = None,
        machine: Union["Machine", None] = None,
    ):
        """
        Initialize.

        One of the arguments is used to configure.

        Args:
            machconnstr: Machine connection string: colon-separated
                string consisting of machine id, ssl profile, host,
                port.
            config: Dictionary of machine configuration settings.
            machine: Machine object.
        """
        if machconnstr != None:
            self.machid, self.sslprofile, self.host, self.port = machconnstr.split(":", 3)
            if self.sslprofile == "":
                self.sslprofile = None
            self.port = int(self.port)
        elif config != None:
            self.machid = config.get("machine-id")
            self.sslprofile = config.get("ssl-profile")
            self.host = config.get("host")
            self.port = config.get("port")
        elif machine != None:
            self.machid = machine.oid
            self.sslprofile = machine.sslprofile
            self.host = machine.conn.host
            self.port = machine.conn.port
        else:
            raise Exception("cannot set up MachineConnectionSpec")

    def __str__(self):
        return f"{self.machid}:{self.sslprofile or ''}:{self.host}:{self.port}"


class State(dict):
    """Object state.

    Holds object identifying information and a dictionary containing
    relevant object state.
    """

    def __init__(self, otype: str, oid: str, state: dict):
        """Initialize.

        Args:
            otype: Object type string.
            oid: Object id string.
            state: State information.
        """
        super().__init__()
        self.update(
            {
                "timestamp": get_timestamp(),
                "object": {
                    "type": otype,
                    "id": oid,
                },
                "state": state,
            }
        )


class TaggedLoggerAdapter(_LoggerAdapter):
    """Logger adapter which tags each log entry with a unique id to
    allow for tracking. Also provides a means to track timing
    performance.

    Also provides special methods for local (e.g., function/method)
    logger: enter, exit, elapsed, lap.
    """

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.stacklevel = kwargs.pop("stacklevel", 4)
        super().__init__(*args, **kwargs)
        self.default_level = logging.DEBUG
        self.mark = LogMark()

    def critical(self, msg, *args, **kwargs):
        """Make a "critical" log entry."""
        _msg = f"[{self.mark.uuid}] {msg}"
        self.log(logging.CRITICAL, _msg, *args, stacklevel=self.stacklevel, **kwargs)

    def debug(self, msg, *args, **kwargs):
        """Make a "debug" log entry."""
        _msg = f"[{self.mark.uuid}] {msg}"
        self.log(logging.DEBUG, _msg, *args, stacklevel=self.stacklevel, **kwargs)

    def elapsed(
        self,
        msg: Union[str, None] = None,
    ):
        """Make a log entry with special format string containing:
        "ELAPSED", a tag, time, and elapsed time (from start).

        Args:
            msg: Additional message to log.
        """
        t1, elapsed = self.mark.elapsed()
        _msg = f"ELAPSED [{self.mark.uuid}, {t1:.5f}, {elapsed:.5f}]"
        if msg:
            _msg = f"{_msg} {msg}"
        self.log(self.default_level, _msg, stacklevel=self.stacklevel)

    def enter(
        self,
        msg: Union[str, None] = None,
        stacklevel: Union[int, None] = None,
    ):
        """Make a log entry with special format string containing:
        "ENTER", tag, and time.

        Args:
            msg: Additional message to log.
            stacklevel: Stack level to extract information from.
        """
        _msg = f"ENTER [{self.mark.uuid}, {self.mark.t0:.5f}]"
        if msg:
            _msg = f"{_msg} {msg}"
        stacklevel = stacklevel if stacklevel != None else self.stacklevel
        self.log(self.default_level, _msg, stacklevel=stacklevel)
        return self

    def error(self, msg, *args, **kwargs):
        """Mark an "error" log entry."""
        _msg = f"[{self.mark.uuid}] {msg}"
        self.log(logging.ERROR, _msg, *args, stacklevel=self.stacklevel, **kwargs)

    def exit(
        self,
        msg: Union[str, None] = None,
    ):
        """Make a log entry with special format string containing:
        "EXIT", tag, time, and elapsed (from start).

        Args:
            msg: Additional message to log.
        """
        t1, elapsed = self.mark.elapsed()
        _msg = f"EXIT [{self.mark.uuid}, {t1:.5f}, {elapsed:.5f}]"
        if msg:
            _msg = f"{_msg} {msg}"
        self.log(self.default_level, _msg, stacklevel=self.stacklevel)

    def state(
        self,
        msg,
        *args: List,
        **kwargs: Dict,
    ):
        _msg = f"[{self.mark.uuid}] {msg}"
        self.log(logging.INFO, _msg, *args, stacklevel=self.stacklevel, **kwargs)

    def lap(
        self,
        msg: Union[str, None] = None,
    ):
        """Make a log entry with special format string containing:
        "LAP", tag, time, and elapsed from split, and from lap split.

        Args:
            msg: Additional message to log.
        """
        t1, elapsed, lap = self.mark.lap()
        _msg = f"LAP [{self.mark.uuid}, {t1:.5f}, {elapsed:.5f}, {lap:.5f}]"
        if msg:
            _msg = f"{_msg} {msg}"
        self.log(self.default_level, _msg, stacklevel=self.stacklevel)

    def warning(self, msg, *args, **kwargs):
        """Make a "warning" log entry."""
        _msg = f"[{self.mark.uuid}] {msg}"
        self.log(logging.WARNING, _msg, *args, stacklevel=self.stacklevel, **kwargs)


# variables for logging
_log_counter = int(time.time() * 100)
_log_counter_lock = Lock()


def get_log_uuid() -> str:
    """Return UUID for logging.

    Returns:
        UUID string value.
    """
    global _log_counter, _log_counter_lock
    with _log_counter_lock:
        _log_counter += 1
        return "%x" % _log_counter


def get_log_uuid1() -> str:
    """Return UUID1 log logging.

    Returns:
        UUID1 string value.
    """
    return str(uuid1())


def get_log_mark() -> Tuple[float, str]:
    """Get log mark of (now, uuid)."""
    return (time.time(), get_log_uuid())


def get_timestamp() -> str:
    """Get timestamp as string."""
    return str(time.time())


_counter = int(time.time() * 100)
_counter_lock = Lock()


def get_uuid() -> str:
    """Alternate `get_uuid` implementation."""
    global _counter, _counter_lock
    with _counter_lock:
        _counter += 1
        return "%x" % _counter


def get_uuid1() -> str:
    """Get UUID1 value.

    Returns:
        UUID1 string value.
    """
    # return uuid1().hex
    return str(uuid1())
    # return secrets.token_urlsafe()


def get_version() -> Tuple:
    """Return PENVM version tuple.

    Returns:
        Version tuple."""
    return VERSION


def get_version_string() -> str:
    """Return PENVM version as a string.

    Returns:
        Version string.
    """
    return "%s.%s.%s" % VERSION
