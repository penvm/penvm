#
# penvm/kernels/base/server.py

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

"""Server-side base Kernel, Op, and OpContext support."""

import importlib.util
import logging
import time
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.lib.misc import State
from penvm.lib.message import Message

logger = logging.getLogger(__name__)


class Kernel(BaseObject):
    """Base kernel.

    Provides base functionality for working with the kernel.
    """

    name = "base"

    def __init__(self):
        """Initialize."""
        try:
            super().__init__(self.name, logger)
            tlogger = self.logger.enter()

            self.updated = False
            self.ops = {}
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def copy(self):
        """Deep copy of this kernel."""
        return copy.deepcopy(self)

    def get_bases(self):
        """Bases (MRO) for kernel."""
        return self.__class__.__mro__

    def list(self):
        """List operation names."""
        return list(self.ops.keys())

    def register(
        self,
        opname: str,
        op: "Op",
    ):
        """Register operation.

        Args:
            opname: Operation name.
            op: Op reference.
        """
        self.updated = True
        self.ops[opname] = op

    def register_code(
        self,
        opname: str,
        opclassname: str,
        code: str,
    ):
        """Register an op by code (in a string).

        Args:
            opname: Operation name.
            opclassname: Class name in code snippet.
            code: Code snippet.
        """
        try:
            spec = importlib.util.spec_from_loader("ext", loader=None)
            ext = importlib.util.module_from_spec(spec)
            exec(code, ext.__dict__)

            cls = getattr(ext, opclassname)
            if cls:
                op = cls()
                if hasattr(op, "run"):
                    self.ops[opname] = cls()
                    self.logger.debug(f"ops ({list(self.ops.keys())} {cls=}")
                    self.logger.debug("register code succeeded")
                else:
                    self.logger.debug("register class has no run method")
            else:
                self.logger.debug(f"class {opclassname} not found in code")
        except Exception as e:
            self.logger.warning(f"EXCEPTION register code failed {e}")

    def run(
        self,
        opname: str,
        ctxt: "OpContext",
        req: "Message",
    ):
        """Run the op for `opname`.

        As a *convenience*, a response return value is put into the
        `session.omq`.

        Args:
            opname: Operation to run, referenced by name.
            ctxt: Context to provide operation.
            req: Request message to provide operation.
        """
        t0 = time.time()
        resp = self.run_local(opname, ctxt, req)
        if isinstance(resp, Message):
            t1 = time.time()
            resp.payload["-oprun-elapsed"] = t1 - t0
            ctxt.session.omq.put(resp)
        else:
            if opname not in self.ops:
                self.logger.error(f"run opname={opname} not found")
            else:
                # TODO: why is this here? a non-responding op is ok!
                self.logger.error(f"run opname={opname} did not succeed")

    def run_local(
        self,
        opname: str,
        ctxt: "OpContext",
        req: "Message",
    ) -> "Message":
        """Run the op for `opname` and return result.

        Suitable for local use.

        Args:
            opname: See [penvm.kernels.base.server.Kernel.run][].
            ctxt: See [penvm.kernels.base.server.Kernel.run][].
            req: See [penvm.kernels.base.server.Kernel.run][].

        Returns:
            Response (if generated).
        """
        op = self.ops.get(opname)
        if op:
            return op.run(ctxt, req)

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            opnames = sorted(list(self.ops.keys()))
            return State(
                "kernel",
                self.name,
                {
                    "bases": [getattr(cls, "name") for cls in self.get_bases() if cls != object],
                    "nops": len(opnames),
                    "ops": opnames,
                    "updated": self.updated,
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")

    def unregister(self, opname: str):
        """Unregister operation.

        Args:
            opname: Operation name.
        """
        try:
            del self.ops[opname]
        except Exception as e:
            pass


class Op(BaseObject):
    """Encapsulates functionality for an operation."""

    def __init__(self):
        """Initialize."""
        super().__init__(None, logger)

    def run(self, req: "Message"):
        """Run method."""
        pass


class OpContext:
    """Context to run operation.

    Provides an easy way for an operation to access important and
    necessary objects and information.
    """

    def __init__(self):
        """Initialize."""
        self.conn = None
        self.machine = None
        self.processor = None
        self.session = None
