#
# penvm/kernels/core/client.py

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

"""Client-side interface to access the "core" kernel operations.

See [penvm.kernels.core.server][]."""

import logging
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.kernels.base.client import KernelClient as _KernelClient

logger = logging.getLogger(__name__)


class KernelClient(_KernelClient):
    name = "core"

    def connection_get_state(self, connectionid):
        """See [penvm.kernels.core.server.ConnectionGetState][]."""
        d = {
            "connection-id": connectionid,
        }
        return self.session.newput_request("connection-get-info", d)

    def echo(self, d):
        """See [penvm.kernels.core.server.Echo][]."""
        return self.session.newput_request("echo", d)

    def kernel_add(self, kernelname, kernelsrc):
        """See [penvm.kernels.core.server.KernelAdd][]."""
        d = {
            "kernel": kernelname,
            "source": kernelsrc,
        }
        return self.session.newput_request("kernel-add", d)

    def kernel_copy(self, src_kernelname, dst_kernelname):
        """See [penvm.kernels.core.server.KernelCopy][]."""
        d = {
            "src-kernel": src_kernelname,
            "dst-kernel": dst_kernelname,
        }
        return self.session.newput_request("kernel-copy", d)

    def kernel_drop(self, kernelname):
        """See [penvm.kernels.core.server.KernelDrop][]."""
        d = {
            "kernel": kernelname,
        }
        return self.session.newput_request("kernel-drop", d)

    def kernel_get_state(self, kernelname):
        """See [penvm.kernels.core.server.KernelGetState][]."""
        d = {
            "kernel": kernelname,
        }
        return self.session.newput_request("kernel-get-state", d)

    def kernel_list_ops(self, kernelname):
        """See [penvm.kernels.core.server.KernelListOps][]."""
        d = {
            "kernel": kernelname,
        }
        return self.session.newput_request("kernel-list-ops", d)

    def kernel_register_op(
        self,
        kernelname,
        opname,
        opclassname,
        code,
        code_key=None,
    ):
        """See [penvm.kernels.core.server.KernelRegisterOp][]."""
        d = {
            "code": code,
            "kernel": kernelname,
            "new-op": opname,
            "op-class": opclassname,
        }
        if code_key:
            d["code-key"] = code_key
        return self.session.newput_request("kernel-register-op", d)

    def kernel_unregister_op(
        self,
        kernelname,
        opname,
    ):
        """See [penvm.kernels.core.server.KernelUnregisterOp][]."""
        d = {
            "kernel": kernelname,
            "old-op": opname,
        }
        return self.session.newput_request("kernel-unregister-op", d)

    def machine_disable_debug(self):
        """See [penvm.kernels.core.server.MachineDisableDebug][]."""
        # TODO: should this force sessionid to "debug"?
        return self.session.newput_request("machine-enable-debug")

    def machine_drop_session(self, sessionid=None):
        """See [penvm.kernels.core.server.MachineDropSession][]."""
        d = {}
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("machine-drop-session", d)

    def machine_enable_debug(self):
        """See [penvm.kernels.core.server.MachineEnableDebug][]."""
        # TODO: should this force sessionid to "debug"?
        return self.session.newput_request("machine-enable-debug")

    def machine_get_features(self):
        """See [penvm.kernels.core.server.MachineGetFeatures][]."""
        return self.session.newput_request("machine-get-features")

    def machine_get_state(self):
        """See [penvm.kernels.core.server.MachineGetState][]."""
        return self.session.newput_request("machine-get-state")

    def machine_list_kernels(self):
        """See [penvm.kernels.core.server.MachineListKernels][]."""
        return self.session.newput_request("machine-list-kernels")

    def machine_list_sessions(self):
        """See [penvm.kernels.core.server.MachineListSessions][]."""
        return self.session.newput_request("machine-list-sessions")

    def machine_shutdown(self):
        """See [penvm.kernels.core.server.MachineShutdown][]."""
        return self.session.newput_request("machine-shutdown")

    def machine_snapshot(self):
        """See [penvm.kernels.core.server.MachineSnapshot][]."""
        return self.session.newput_request("machine-snapshot")

    def machine_step_debug(self):
        """See [penvm.kernels.core.server.MachineStepDebug][]."""
        # TODO: should this force sessionid to "debug"?
        return self.session.newput_request("machine-step-debug")

    def session_drop_request(self, reqid, sessionid=None):
        """See [penvm.kernels.core.server.SessionDropRequest][]."""
        d = {
            "request-id": reqid,
        }
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("session-drop-request", d)

    def session_get_message_state(self, msgid, msgqueue, sessionid=None):
        """See [penvm.kernels.core.server.SessionGetMessageState][]."""
        d = {
            "message-id": msgid,
            "message-queue": msgqueue,
        }
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("session-get-message-state", d)

    def session_get_processor_state(self, sessionid=None):
        """See [penvm.kernels.core.server.SessionGetProcessorState][]."""
        d = {}
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("session-get-processor-state", d)

    def session_get_state(self, sessionid=None):
        """See [penvm.kernels.core.server.SessionGetState][]."""
        d = {}
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("session-get-state", d)

    def session_pin_session(self, sessionid=None):
        """See [penvm.kernels.core.server.SessionPinSession][]."""
        d = {}
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("session-pin-session", d)

    def session_pop_omq(self, sessionid=None, count=1):
        """See [penvm.kernels.core.server.SessionPopOmq][]."""
        d = {
            "count": count,
        }
        if sessionid != None:
            d["session-id"] = sessionid
        return self.session.newput_request("session-pop-omq", d)

    def session_set_max_threads(self, nthreads, sessionid=None):
        """See [penvm.kernels.core.server.SessionSetMaxThreads][]."""
        d = {
            "nthreads": nthreads,
        }
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("session-set-max-threads", d)

    def session_terminate_request(self, reqid, sessionid=None):
        """See [penvm.kernels.core.server.SessionTerminateRequest][]."""
        d = {
            "request-id": reqid,
        }
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("session-terminate-request", d)

    def session_unpin_session(self, sessionid=None):
        """See [penvm.kernels.core.server.SessionUnpinSession][]."""
        d = {}
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("session-unpin-session", d)

    def session_use_kernel(self, kernelname, sessionid=None):
        """See [penvm.kernels.core.server.SessionUseKernel][]."""
        d = {
            "kernel": kernelname,
        }
        if sessionid:
            d["session-id"] = sessionid
        return self.session.newput_request("session-use-kernel", d)

    def sleep(self, seconds):
        """See [penvm.kernels.core.server.Sleep][]."""
        d = {
            "seconds": seconds,
        }
        return self.session.newput_request("sleep", d)
