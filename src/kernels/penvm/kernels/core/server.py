#
# penvm/kernels/core/server.py

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

"""Server-side "core" kernel operations.

The core operations provide the necessary functionality on which to
build others.

All kernel operations use messages to receive and replay. Convenience
functions are available:

* request: `Request`
* response: `Response`, `OkResponse`, `ErrorResponse`

When a `Request` or `Response` is unused/empty, this is reflected in
the respective attributes section.

Whereas only one `Request` possible, responses may be `OkResponse` or
`ErrorResponse`. Both may be described to account for possible
responses according to the situation.

The `OkResponse` indicates success and contains response information
in its payload. The `ErrorResponse` indicates an error (or failure)
and contains an error messsage in its payload (`-message` field)."""

from importlib.machinery import SourceFileLoader
import logging
import sys
import tempfile
import time
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union


from penvm.kernels.base.server import Kernel as _Kernel, Op
from penvm.lib.message import ErrorResponse, OkResponse, Payload, Request
from penvm.lib.thread import ThreadInterrupt

logger = logging.getLogger(__name__)


class ConnectionGetState(Op):
    """Get connection info.

    Attributes: Request:
        connection-id (str): Connection id.

    Attributes: OkResponse:
        connection-id (str): Connection id.
        initial-connection (str): Is initial machine connection.
        host (str): Connection host name.
        port (int): Connection port.
        peer-host (str): Client/peer host name.
        peer-port (int): Client/peer port.
        nimq (int): Number of IMQ messages.
        nomq (int): Number of OMQ messages.

    Attributes: ErrorResponse:
        -: Error message.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            connid = req.payload.get("connection-id")
            conn = ctxt.machine.connmgr.get(connid)

            if conn != None:
                return OkResponse(
                    payload=Payload(conn.state()),
                    refmsg=req,
                )
            else:
                raise Exception("bad connection")
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                "connection not found",
                refmsg=req,
            )


class Echo(Op):
    """Echo request payload.

    Attributes: Request:
        -: Unused.

    Attributes: OkResponse:
        -: Matches request payload.
    """

    def run(self, ctxt, req):
        try:
            resp = OkResponse(
                payload=req.payload,
                refmsg=req,
            )
            return resp
        except Exception as e:
            self.logger.error(f"{e}")


class Log(Op):
    def run(self, ctxt, req):
        try:
            with open("/tmp/log", "a") as f:
                f.write("%s\n" % req.payload.get("text", "n/a"))
        except Exception as e:
            self.logger.error(f"{e}")


class KernelAdd(Op):
    """Add a new kernel.

    Attributes: Request:
        kernel (str): Name of the kernel.
        data (str): Kernel file.

    Attributes: Response:
        -: None.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            kernel_name = req.payload.get("kernel")
            kernel_source = req.payload.get("source")

            if None in [kernel_name, kernel_source]:
                return

            try:
                with tempfile.NamedTemporaryFile() as f:
                    f.write(kernel_source.encode("utf-8"))
                    f.seek(0)
                    mod = SourceFileLoader("", f.name).load_module()
                ctxt.machine.kernelmgr.set(kernel_name, mod.Kernel())
                self.logger.info(f"completed ({kernel_name})")
            except Exception as e:
                self.logger.critical(f"EXCEPTION ({e})")
        except Exception as e:
            self.logger.error(f"{e}")


class KernelCopy(Op):
    """Make a copy of an existing kernel.

    Attributes: Request:
        dst-kernel (str): Name of destination kernel.
        src-kernel (str): Name of source kernel.

    Attributes: Response:
        -: None.
    """

    # TODO: what kind of return value if any?

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            src_kernelname = req.payload.get("src-kernel")
            dst_kernelname = req.payload.get("dst-kernel")
            if None in [src_kernelname, dst_kernelname]:
                return

            src_kernel = ctxt.machine.kernelmgr.get(src_kernelname)
            dst_kernel = src_kernel.copy()

            if None in [src_kernel, dst_kernel]:
                return

            ctxt.machine.kernelmgr.set(dst_kernelname, dst_kernel)
        except Exception as e:
            self.logger.error(f"{e}")


class KernelDrop(Op):
    """Drop an kernel. The "default" kernel cannot be dropped.

    Attributes: Request:
        kernel (str): Name of kernel.

    Attributes Response:
        -: None.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            name = req.payload.get("kernel")
            ctxt.machine.kernelmgr.drop(name)
        except Exception as e:
            self.logger.error(f"{e}")


class KernelGetState(Op):
    """Get kernel info.

    Attributes: Request:
        kernel (str): Kernel name.

    Attributes: OkResponse:
        bases (list of str): List of base kernel names.
        nops (int): Number of operations.
        ops (list of str): List of operation names.
        updated (bool): If updated.

    Attributes: ErrorResponse:
        -: Error message.
    """

    def run(self, ctxt, req):
        try:
            kernelname = req.payload.get("kernel")
            kernel = ctxt.machine.kernelmgr.get(kernelname)
            return OkResponse(
                payload=Payload(kernel.state()),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                "kernel not found",
                refmsg=req,
            )


class KernelListOps(Op):
    """List op names for kernel.

    Attributes: Request:
        kernel (str): Kernel name.

    Attributes: OkResponse:
        names (list of str): List of kernel operation names.

    Attributes: ErrorResponse:
        -: Error message.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            kernelname = req.payload.get("kernel")
            if kernelname == None:
                return ErrorResponse(
                    f"kernel name not provided",
                    refmsg=req,
                )

            kernel = ctxt.machine.kernelmgr.get(kernelname)
            if kernel == None:
                return ErrorResponse(
                    f"kernel ({kernelname}) not found",
                    refmsg=req,
                )

            names = kernel.list()
            return OkResponse(
                payload=Payload({"names": names}),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")


class KernelRegisterOp(Op):
    """Register a new op for an kernel.

    Attributes: Request:
        kernel (str): Kernel name.
        new-op (str): Op name.
        op-class (str): Op class name.
        code (str): Kernel class definition.

    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            kernel_name = req.payload.get("kernel")
            opname = req.payload.get("new-op")
            opclassname = req.payload.get("op-class")
            code = req.payload.get("code")
            if code == None:
                code_key = req.payload.get("code-key")
                if code_key != None:
                    code = ctxt.machine.fkvstore.get(code_key)

            if None in [kernel_name, opname, opclassname, code]:
                self.logger.debug(
                    f"missing params ({kernel_name=} {opname=} {opclassname=} {code=}"
                )
                return

            kernel = ctxt.machine.kernelmgr.get(kernel_name)
            if kernel == None:
                self.logger.debug(f"cannot find kernel")
                return

            kernel.register_code(opname, opclassname, code)
        except Exception as e:
            self.logger.error(f"{e}")


class KernelUnregisterOp(Op):
    """Unregister an op for an kernel."""

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            kernel_name = req.payload.get("kernel")
            opname = req.payload.get("old-op")
            if None in [kernel_name, opname]:
                return
            kernel = ctxt.machine.kernelmgr.get(kernel_name)
            kernel.unregister(opname)
        except Exception as e:
            self.logger.error(f"{e}")


class MachineDisableDebug(Op):
    """Disable debug mode.

    Attributes: Request:
        : Unused.

    Attributes: Response:
        : None.
    """

    def run(self, ctxt, req):
        try:
            ctxt.machine.set_debug(False)
        except Exception as e:
            self.logger.error(f"{e}")


class MachineDropConnection(Op):
    """Drop connection.

    Attributes: Request:
        connection-id: Sessionid for session processor. Defaults to
            sessionid of session running this op.

    Attributes: Response:
        : None.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            connid = re.payload.get("connection-id")
            conn = ctxt.machine.connmgr.get(connid)
            if conn:
                conn.close()
        except Exception as e:
            self.logger.error(f"{e}")


class MachineDropSession(Op):
    """Drop session.

    Attributes: Request:
        session-id: Sessionid for session processor. Defaults to
            sessionid of session running this op.

    Attributes: Response:
        : None.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            reqid = req.header.get("id")
            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            if sessionid == None:
                self.logger.warning("bad/missing session id")
                return

            session = ctxt.machine.pop_session(sessionid)
            if session == None:
                self.logger.warning(f"session not found for session id ({sessionid}")
                return

            self.logger.debug(f"dropping session ({sessionid}) reqid ({reqid}) ...")

            # clean up
            self.logger.debug(f"imq ({session.imq.values()}) omq ({session.omq.values()}) ...")
            self.logger.debug(f"freezing imq ...")
            session.imq.freeze(True)

            self.logger.debug(f"freezing omq ...")
            session.omq.freeze(True)

            self.logger.debug(f"clearing imq ...")
            session.imq.clear()

            self.logger.debug(f"clearing omq ...")
            session.omq.clear()

            self.logger.debug(f"imq ({session.imq.values()}) omq ({session.omq.values()}) ...")

            self.logger.debug(f"terminating request threads ({session.proc.active_count()})...")
            for _reqid in session.proc.get_thread_reqids():
                try:
                    if _reqid == reqid:
                        self.logger.debug(f"skip terminating reqid ({_reqid})")
                    else:
                        self.logger.debug(f"terminating reqid ({_reqid})")
                        session.proc.terminate_thread(_reqid)
                except Exception as e:
                    self.logger.debug(f"EXCEPTION ({e})")
        except Exception as e:
            self.logger.error(f"{e}")


class MachineEnableDebug(Op):
    """Disable debug mode.

    Attributes: Request:
        : Unused.

    Attributes: Response:
        : None.
    """

    def run(self, ctxt, req):
        try:
            ctxt.machine.set_debug(True)
        except Exception as e:
            self.logger.error(f"{e}")


class MachineStepDebug(Op):
    """Trigger step of debug.

    Requires that debug is on.

    See [penvm.kernels.core.server.MachineEnableDebug][] and
    [penvm.kernels.core.server.MachineDisableDebug][].

    Attributes: Request:
        -: Unused.

    Attributes: OkResponse:
        -: None.
    """

    def run(self, ctxt, req):
        try:
            ctxt.machine.step_debug()
        except Exception as e:
            self.logger.error(f"{e}")


class MachineGetFeatures(Op):
    """Get and return machine features.

    Features are settings which describe the environment and context
    that a machine runs with. E.g., the base language (i.e., python)
    and the version.

    Attributes: Request:
        -: Unused.

    Attributes: OkResponse:
        -: Dictionary of machine features.
    """

    def run(self, ctxt, req):
        try:
            return OkResponse(
                payload=Payload(ctxt.machine.features()),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                "machine not found",
                refmsg=req,
            )


class MachineGetState(Op):
    """Get and return machine state.

    Attributes: Request:
        -: Unused.

    Attributes: OkResponse:
        : Dictionary of state.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            return OkResponse(
                payload=Payload(ctxt.machine.state()),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                "machine not found",
                refmsg=req,
            )


class MachineListConnections(Op):
    """List machine connections.

    Attributes: Request:
        -: Unused.

    Attributes: OkResponse:
        names: List of connection ids.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            connids = ctxt.machine.connmgr.list()
            return OkResponse(
                payload=Payload(
                    {
                        "names": list(connids),
                    }
                ),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                f"{e}",
                refmsg=req,
            )


class MachineListKernels(Op):
    """List kernels by name registered with the machine.

    Attributes: Request:
        -: Unused.

    Attributes: OkResponse:
        names: List of kernel names.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            return OkResponse(
                payload=Payload(
                    {
                        "names": ctxt.machine.kernelmgr.list(),
                    }
                ),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                f"{e}",
                refmsg=req,
            )


class MachineListSessions(Op):
    """List machine sessions.

    Attributes: Request:
        -: Unused.

    Attributes: OkResponse:
        names: List of session ids.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            sessionids = ctxt.machine.sessmgr.list()
            return OkResponse(
                payload=Payload(
                    {
                        "names": list(sessionids),
                    },
                ),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                f"{e}",
                refmsg=req,
            )


class MachineSnapshot(Op):
    """Snapshot machine state and return.

    Snapshottable state taken from:

    * Single: [penvm.kernels.core.server.MachineGetState][].
    * Single: [penvm.kernels.default.server.StoreGetState][].
    * For each: [penvm.kernels.core.server.ConnectionGetState][].
    * For each: [penvm.kernels.core.server.SessionGetState][].
    * For each: [penvm.kernels.core.server.KernelGetState][].

    Attributes: Request:
        -: Unused.

    Attributes: OkResponse:
        -: Dictionary of state.
    """

    def run(self, ctxt, req):
        try:
            # TODO: ensure this runs in a session with the required ops
            self.logger.debug("run")
            run_local = ctxt.session.proc.kernel.run_local
            payload = Payload()

            _req = Request()
            _resp = run_local("machine-get-state", ctxt, _req)
            self.logger.debug(f"{_resp=}")
            payload["machine"] = _resp.payload.dict(clean=True)

            _resp = run_local("store-get-state", ctxt, _req)
            self.logger.debug(f"{_resp=}")
            payload["store"] = _resp.payload.dict(clean=True)

            _req = Request()
            conns = payload["connections"] = {}
            for connid in ctxt.machine.connmgr.list():
                _req.payload["connection-id"] = connid
                _resp = run_local("connection-get-state", ctxt, _req)
                self.logger.debug(f"{_resp=}")
                conns[connid] = _resp.payload.dict(clean=True)

            _req = Request()
            sessions = payload["sessions"] = {}
            for sessid in ctxt.machine.sessmgr.list():
                _req.payload["session-id"] = sessid
                _resp = run_local("session-get-state", ctxt, _req)
                self.logger.debug(f"{_resp=}")
                sessions[sessid] = _resp.payload.dict(clean=True)

            _req = Request()
            kernels = payload["kernels"] = {}
            for name in ctxt.machine.kernelmgr.list():
                _req.payload["kernel"] = name
                _resp = run_local("kernel-get-state", ctxt, _req)
                self.logger.debug(f"{_resp=}")
                kernels[name] = _resp.payload.dict(clean=True)

            self.logger.debug(f"{payload=}")
            return OkResponse(
                payload=payload,
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                f"{e}",
                refmsg=req,
            )


class MachineShutdown(Op):
    """Shut down the machine.

    Attributes: Request:
        : Unused.

    Attributes: Response:
        : None.
    """

    def run(self, ctxt, req):
        """Shut down machine."""
        self.logger.debug("run")
        # TODO: can a reply be sent to ack? I think so!
        sys.exit(0)


class SessionDropRequest(Op):
    """Drop request from session IMQ."""

    def run(self, ctxt, req):
        """Run operation."""
        pass


class SessionGetMessageState(Op):
    """Get session message state (from message queue).

    Attributes: Request:
        session-id: Session id for session.
        message-id: Message id of request held by session.
        message-queue: One of "in" or "out".

    Attributes: Response:
        dict: Request state dictionary.
    """

    def run(self, ctxt, req):
        """Run operation."""

        try:
            self.logger.debug("run")
            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            msgid = req.payload.get("message-id")
            msgqueue = req.payload.get("message-queue")

            sess = ctxt.machine.get_session(sessionid)

            if msgqueue == "in":
                mq = sess.imq
            elif msgqueue == "out":
                mq = sess.omq
            else:
                mq = None
            msg = mq.find(msgid)
            if msg == None:
                return ErrorResponse(
                    "message not found",
                    refmsg=req,
                )
            return OkResponse(payload=Payload(msg.state()))
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                "message not found",
                refmsg=req,
            )


class SessionGetProcessorState(Op):
    """Get session processor state.

    Attributes: Request:
        session-id: Session id for session.

    Attributes: Response:
        dict: Request state dictionary.
    """

    def run(self, ctxt, req):
        """Run operation."""

        try:
            self.logger.debug("run")

            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            sess = ctxt.machine.get_session(sessionid)
            return OkResponse(payload=Payload(sess.proc.state()))
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                "session not found",
                refmsg=req,
            )


class SessionGetState(Op):
    """Get session info.

    Attributes: Request:
        session-id: Session id for session.

    Attributes: OkResponse:
        -: Dictionary of state.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            sess = ctxt.machine.get_session(sessionid)
            return OkResponse(
                payload=Payload(sess.state()),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                "session not found",
                refmsg=req,
            )


class SessionPinSession(Op):
    """Pin session.

    Attributes: Request:
        session-id: Session id for session.

    Attributes: Response:
        : None.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            sess = ctxt.machine.get_session(sessionid)
            sess.pin = True
        except Exception as e:
            self.logger.error(f"{e}")


class SessionPopOmq(Op):
    """
    Pop waiting message from session OMQ.

    Attributes: Request:
        count: Number of times to pop the OMQ. Blocking for values
            of >= 1. For 0, pop all values. Defaults to 1.
        session-id: Override sessionid. Defaults to sessionid of
            session running this op.

    Attributes: Response:
        : None.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            connectionid = req.header.get("connection-id")
            session = ctxt.machine.sessmgr.get(sessionid)

            count = req.payload.get("count", 1)
            if count >= 1:
                for _ in range(count):
                    resp = session.omq.pop()
                    resp.header["session-id"] = sessionid
                    resp.header["connection-id"] = connectionid
                    ctxt.machine.omq.put(resp)
            elif count == 0:
                while True:
                    resp = session.omq.pop(block=False)
                    if resp == None:
                        break

                    resp.header["session-id"] = sessionid
                    resp.header["connection-id"] = connectionid
                    ctxt.machine.omq.put(resp)
        except Exception as e:
            self.logger.error(f"{e}")


class SessionSetMaxThreads(Op):
    """Set max threads for session."""

    def run(self, ctxt, req):
        try:
            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            nthreads = req.payload.get("nthreads")

            if None in [nthreads, sessionid]:
                return
            if type(nthreads) != int:
                return

            session = ctxt.machine.get_session(sessionid)
            session.proc.set_max_threads(nthreads)
        except Exception as e:
            self.logger.error(f"{e}")


class SessionUseKernel(Op):
    """Set session to use an kernel."""

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            kernel_name = req.payload.get("kernel")

            if None in [sessionid, kernel_name]:
                self.logger.warning(f"kernel and or sessionid not provided")
                return

            kernel = ctxt.machine.kernelmgr.get(kernel_name)
            if not kernel:
                self.logger.warning(f"kernel ({kernel_name}) not found")
                return

            sess = ctxt.machine.get_session(sessionid)
            sess.set_kernel(kernel)
            self.logger.state(f"session ({sessionid}) using kernel ({kernel_name})")
        except Exception as e:
            self.logger.error(f"{e}")


class SessionTerminateRequest(Op):
    """Terminate running request thread/task for session.

    Attributes: Request:
        reqid: Request id.
        session-id: Session id for session processor.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            reqid = req.payload.get("request-id")
            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            self.logger.debug(f"^^^^^^^^ terminate payload {reqid=} {sessionid=}")

            if reqid == None:
                self.logger.warning("bad/missing request id")
                return

            if sessionid == None:
                self.logger.warning("bad/missing session id")
                return

            session = ctxt.machine.get_session(sessionid)
            if session == None:
                self.logger.warning(f"session not found for session id ({sessionid}")
                return

            session.proc.terminate_thread(reqid)
        except Exception as e:
            self.logger.error(f"{e}")


class SessionUnpinSession(Op):
    """Unpin session.

    Attributes: Request:
        session-id: Session id for session.

    Attributes: Response:
        : None.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            sess = ctxt.machine.get_session(sessionid)
            sess.pin = False
        except Exception as e:
            self.logger.error(f"{e}")


class SessionUsekernel(Op):
    """Update session to use a specific, previously registered kernel."""

    def run(self, ctxt, req):
        try:
            sessionid = req.payload.get("session-id", req.header.get("session-id"))
            if sessionid == None:
                return
            session = ctxt.machine.get_session(sessionid)

            kernelname = req.payload.get("kernel")
            kernel = ctxt.machine.kernelmgr.get(kernelname)
            session.set_kernel(kernel)
        except Exception as e:
            self.logger.error(f"{e}")


class Sleep(Op):
    """Sleep.

    Attributes: Request:
        seconds: Number (int or float) of seconds to sleep.
        check-interval: Number of seconds to between checks for an
            interrupt.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            seconds = req.payload.get("seconds")
            check_interval = req.payload.get("check-interval", 1)

            if type(seconds) not in [int, float] or seconds < 0:
                return
            if type(check_interval) not in [int, float] or seconds < 0:
                return

            while seconds > 0:
                time.sleep(min(seconds, check_interval))
                seconds -= check_interval
        except ThreadInterrupt as e:
            raise
        except Exception as e:
            self.logger.error(f"{e}")
            self.logger.warning(f"bad/missing sleep time ({seconds})")


class Kernel(_Kernel):
    name = "core"

    def __init__(self):
        super().__init__()
        self.ops.update(
            {
                "connection-get-state": ConnectionGetState(),
                "echo": Echo(),
                "log": Log(),
                "machine-disable-debug": MachineDisableDebug(),
                "machine-drop-connection": MachineDropConnection(),
                "machine-drop-session": MachineDropSession(),
                "machine-enable-debug": MachineEnableDebug(),
                "machine-get-features": MachineGetFeatures(),
                "machine-get-state": MachineGetState(),
                "machine-list-connections": MachineListConnections(),
                "machine-list-kernels": MachineListKernels(),
                "machine-list-sessions": MachineListSessions(),
                "machine-shutdown": MachineShutdown(),
                "machine-step-debug": MachineStepDebug(),
                "machine-snapshot": MachineSnapshot(),
                "kernel-add": KernelAdd(),
                "kernel-copy": KernelCopy(),
                "kernel-drop": KernelDrop(),
                "kernel-get-state": KernelGetState(),
                "kernel-list-ops": KernelListOps(),
                "kernel-register-op": KernelRegisterOp(),
                "kernel-unregister-op": KernelUnregisterOp(),
                "session-drop-request": SessionDropRequest(),
                "session-get-message-state": SessionGetMessageState(),
                "session-get-processor-state": SessionGetProcessorState(),
                "session-get-state": SessionGetState(),
                "session-pop-omq": SessionPopOmq(),
                "session-set-max-threads": SessionSetMaxThreads(),
                "session-terminate-request": SessionTerminateRequest(),
                "session-use-kernel": SessionUseKernel(),
                "sleep": Sleep(),
            },
        )
