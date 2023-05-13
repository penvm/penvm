#
# penvm/lib/message.py
#

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

import base64
from collections.abc import MutableMapping
import copy
import io
import json
import logging
import time
from typing import Any, Callable, Dict, List, Tuple, Type, Union
import yaml

from penvm.lib.misc import State, get_uuid

logger = logging.getLogger(__name__)

HEADER_SZ_LEN = 5
HEADER_SZ_FMT = b"%%0.%dd" % HEADER_SZ_LEN
HEADER_SZ_MAX = 100_000 - 1
PAYLOAD_SZ_LEN = 8
PAYLOAD_SZ_FMT = b"%%0.%dd" % PAYLOAD_SZ_LEN
PAYLOAD_SZ_MAX = 100_000_000 - 1 - HEADER_SZ_MAX - 40

NON_CONFORMING = "__non-conforming__"


class JSONEncoder(json.JSONEncoder):
    """Enhanced JSONEncoder with support for non-conforming types:

    * `bytes` - Bytes.
    * `complex` - Complex.
    * `numpy.*` - Python-only.
    """

    def default(self, o):
        # TODO: optimize/clean up
        otype = type(o)
        if otype == bytes:
            o = {
                NON_CONFORMING: {
                    "type": "bytes",
                    "value": base64.b64encode(o).decode("utf-8"),
                }
            }
            return o
        elif otype == complex:
            o = {
                NON_CONFORMING: {
                    "type": "complex",
                    "value": [o.real, o.imag],
                }
            }
            return o
        elif otype.__module__ == "numpy":
            try:
                import numpy
            except:
                numpy = None

            f = io.BytesIO()
            numpy.save(f, o)
            o = {
                NON_CONFORMING: {
                    "type": "numpy",
                    "class": o.__class__.__name__,
                    "value": base64.b64encode(f.getvalue()).decode("utf-8"),
                }
            }
            return o

        return json.JSONEncoder.default(self, o)


def json_decoder_object_hook(o):
    """Support for decoding of non-conforming types:

    * `bytes` - Bytes.
    * `complex` - Complex.
    * `numpy.*` - Numpy serialized objects (Python-only).

    The encoding is an object as:

    ```
    NON_CONFORMING: {
        "type": <str>,
        "value": <base64encoding>,
    }
    ```

    Note: Object keys cannot be bytes.
    """

    if NON_CONFORMING in o:
        nctype = o.get(NON_CONFORMING).get("type")
        ncvalue = o.get(NON_CONFORMING).get("value")
        if nctype:
            if nctype == "bytes":
                return base64.b64decode(ncvalue.encode("utf-8"))
            elif nctype == "complex":
                return complex(ncvalue[0], ncvalue[1])
            elif nctype == "numpy":
                try:
                    import numpy
                except:
                    numpy = None

                f = io.BytesIO(base64.b64decode(ncvalue.encode("utf-8")))
                return numpy.load(f, allow_pickle=False)

    return o


class MessagePart(MutableMapping):
    """Dict-type object with enhanced json codec support for bytes."""

    def __init__(
        self,
        d: dict = None,
    ):
        """Initialize.

        Args:
            d: Initial configuration.
        """
        self.d = {}
        if d != None:
            self.d.update(d)

    def __delitem__(
        self,
        k: Any,
    ):
        del self.d[k]

    def __getitem__(self, k):
        return self.d[k]

    def __iter__(self):
        return iter(self.d.keys())

    def __len__(self):
        return len(self.d)

    def __repr__(self):
        # this can be expensive for big payloads
        s = str(self.d)
        if len(s) > 256:
            s = s[:253] + "..."
        return f"<{self.__class__.__name__} ({s})>"

    def __setitem__(self, k, v):
        self.d[k] = v

    @staticmethod
    def decode(b: bytes) -> str:
        """Decode bytes with support for local decodings.

        Args:
            b: Bytes to decode.

        Returns:
            JSON string.
        """
        return json.loads(b.decode("utf-8"), object_hook=json_decoder_object_hook)

    def dict(
        self,
        clean: bool = False,
    ) -> Dict:
        """Return a copy as a dict. Optionally clean of "private"
        top-level items (keys start with "-").

        Args:
            clean: Remove keys starting with "-".

        Returns:
            Dictionary.
        """
        d = copy.deepcopy(self.d)
        if clean:
            for k in list(d.keys()):
                if k.startswith("-"):
                    del d[k]
        return d

    def dumps(
        self,
        indent: Union[int, None] = None,
        sort_keys: Union[bool, None] = None,
    ) -> str:
        """Dump contents as a stringified dict.

        Args:
            indent: Indent size.
            sort_keys: Sort keys.

        Returns:
            Stringified dictionary.
        """
        # TODO: not reall a stringified dict. drop in favor of json()?
        kwargs = {}
        if indent != None:
            kwargs["indent"] = indent
        if sort_keys != None:
            kwargs["sort_keys"] = sort_keys
        return json.dumps(self.d, **kwargs)

    def encode(self) -> str:
        """Encode items and return JSON string.

        Returns:
            JSON string.
        """
        return json.dumps(self.d, cls=JSONEncoder).encode("utf-8")

    def json(
        self,
        indent: Union[int, None] = 2,
        clean: bool = False,
    ) -> str:
        """Return JSON string.

        Args:
            indent: Indent size.
            clean: Remove for keys starting with "-".

        Returns:
            JSON string.
        """
        d = self.dict(clean)
        return json.dumps(d, indent=indent)

    def yaml(
        self,
        clean: bool = False,
    ) -> str:
        """Return YAML.

        Args:
            clean: Remove for keys starting with "-".

        Returns:
            YAML.
        """
        d = self.dict(clean)
        return yaml.dump(d)


class Header(MessagePart):
    """Header."""

    def __init__(self, d=None):
        """Initialize.

        See [penvm.lib.message.MessagePart][].
        """
        super().__init__(d)
        # for new only
        if "id" not in self:
            self["id"] = get_uuid()


class Payload(MessagePart):
    """Payload."""

    def __init__(self, d=None):
        """Initialize.

        See [penvm.lib.message.MessagePart][].
        """
        super().__init__(d)
        # for new only
        if "-id" not in self:
            self["-id"] = get_uuid()


class Message:
    """Object consisting of header and payload objects.

    The header and payload objects are `MessagePart` objects.

    All parts have a `-id` setting unique to the object.

    Message payloads take special fields with `-` prefix:

    * `-type` - Message type (e.g., request, response)
    * `-status` - Status (e.g., ok, error)
    * `-message` - Status message, usually for "error" status.
    """

    def __init__(
        self,
        header: Union[Header, None] = None,
        payload: Union[Payload, None] = None,
    ):
        """Initialize.

        Args:
            header: Header object.
            payload: Payload object.
        """
        self.header = header or Header()
        self.payload = payload or Payload()

    def __repr__(self):
        return f"<{self.__class__.__name__} header ({self.header}) payload ({self.payload})>"

    @staticmethod
    def decode(b):
        """Decode bytes according to Message format.

        Bytes as:

        * header[HEADER_SZ_LEN] -> sz (encoded as plain text number)
        * header[HEADER_SZ_LEN:HEADER_SZ_LEN+sz] -> bytes
        * payload[PAYLOAD_SZ_LEN] -> sz (encoded as plain text number)
        * payload[PAYLOAD_SZ_LEN:PAYLOAD_SZ_LEN+sz] -> bytes

        See [penvm.lib.message.MessagePart][].
        """
        hsz = int(b[:HEADER_SZ_LEN])
        t0 = time.time()
        h = Header(Header.decode(b[HEADER_SZ_LEN : HEADER_SZ_LEN + hsz]))
        t1 = time.time()
        h["-decode-elapsed"] = t1 - t0

        t0 = time.time()
        pb = b[HEADER_SZ_LEN + hsz :]
        psz = int(pb[:PAYLOAD_SZ_LEN])
        p = Payload(Payload.decode(pb[PAYLOAD_SZ_LEN : PAYLOAD_SZ_LEN + psz]))
        t1 = time.time()
        p["-decode-elapsed"] = t1 - t0

        return h, p

    def encode(self):
        """Encode."""
        h = self.header.encode()
        p = self.payload.encode()
        return (HEADER_SZ_FMT % len(h)) + h + (PAYLOAD_SZ_FMT % len(p)) + p

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            return State(
                "message",
                self.header.get("id"),
                {
                    "header": self.header.dict(),
                    "payload": self.payload.dict(),
                },
            )
        except Exception as e:
            logger.warning(f"EXCEPTION ({e})")


#
# conveniences
#


def Request(*args, **kwargs):
    """Return a request message.

    See [penvm.lib.message.Message][].
    """
    msg = Message(*args, **kwargs)
    msg.payload["-type"] = "request"
    return msg


def Response(*args, **kwargs):
    """Return a response message.

    See [penvm.lib.message.Message][].
    """
    refmsg = kwargs.pop("refmsg", None)
    msg = Message(*args, **kwargs)
    if refmsg:
        msg.header["ref-id"] = refmsg.header.get("id")
    msg.payload["-type"] = "response"
    return msg


def ErrorResponse(message, *args, **kwargs):
    """Return an error response message.

    See [penvm.lib.message.Message][].
    """
    msg = Response(*args, **kwargs)
    msg.payload.update(
        {
            "-status": "error",
            "-message": message,
        }
    )
    return msg


def OkResponse(*args, **kwargs):
    """Return an ok response message.

    See [penvm.lib.message.Message][].
    """
    msg = Response(*args, **kwargs)
    msg.payload["-status"] = "ok"
    return msg
