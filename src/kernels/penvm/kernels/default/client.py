#
# penvm/kernels/default/client.py

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

"""Client-side interface to access the "default" kernel operations.

See [penvm.kernels.default.server][]."""

import copy
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.message import Request
from penvm.kernels.core.client import KernelClient as _KernelClient

logger = logging.getLogger(__name__)


class KernelClient(_KernelClient):
    """Additional kernel ops."""

    name = "default"

    def __init__(self, session):
        super().__init__(session)

    def file_copyto(self, lpath, rpath, blksize=1024):
        """Copy local file to remote."""
        lpp = Path(lpath)
        if not lpp.exists():
            # TODO: what to do?
            return

        try:
            with open(lpath, "rb") as f:
                count = 0
                start = 0
                while True:
                    req = Request("file-put")
                    req.payload["path"] = rpath
                    req.payload["start"] = start
                    data = req.payload["data"] = f.read(blksize)
                    self.session.put_request(req)

                    self.session.put_request(Request("omq-pop"))
                    if data == b"":
                        break
                    start + len(data)
                    count += 1

            for i in range(count):
                resp = self.imq.pop()
                if resp.payload.get("-status") == "error":
                    raise Exception(f"expected {count} responses got error on {i}")
        except Exception as e:
            # TODO: what to do here?
            self.logger.debug("failed ({e})")

    def file_copyfrom(self, rpath, lpath, blksize=1024):
        """Copy remote file to local."""
        pass

    def file_get_block(self, filename, start=None, size=None):
        """See [penvm.kernels.default.server.FileGetBlock][]."""
        d = {
            "path": filename,
        }
        if start != None:
            d["start"] = start
        if size != None:
            d["size"] = size
        return self.session.newput_request("file-get-block", d)

    def file_get_status(self, path):
        """See [penvm.kernels.default.server.FileGetStatus][]."""
        d = {
            "path": path,
        }
        return self.session.newput_request("file-get-status", d)

    def file_list(self, path, pattern=None, sort=None, split=None):
        """See [penvm.kernels.default.server.FileList][]."""
        d = {
            "path": path,
        }
        if pattern != None:
            d["pattern"] = pattern
        if sort != None:
            d["sort"] = sort
        if split != None:
            d["split"] = split
        return self.session.newput_request("file-list", d)

    def file_put_block(self, path, data, start=None, size=None, truncate=None):
        """See [penvm.kernels.default.server.FilePutBlock][]."""
        d = {
            "path": path,
            "data": data,
        }
        if start != None:
            d["start"] = start
        if size != None:
            d["size"] = size
        if truncate != None:
            d["truncate"] = truncate
        return self.session.newput_request("file-put-block", d)

    def file_set_status(self, path, uid=None, gid=None, user=None, group=None, mode=None):
        """See [penvm.kernels.default.server.FileSetStatus][]."""
        d = {
            "path": path,
            "uid": uid,
            "gid": gid,
            "user": user,
            "group": group,
            "mode": mode,
        }
        return self.session.newput_request("file-set-status", d)

    def run_exec(
        self,
        path,
        args,
        capture_output=False,
        text=True,
        env=None,
        cwd=None,
        path_key=None,
    ):
        """See [penvm.kernels.default.server.RunExec][]."""
        d = {
            "args": [str(arg) for arg in args],
            "capture-output": capture_output,
            "text": text,
        }
        if path != None:
            d["path"] = path
        elif path_key != None:
            d["path-key"] = path_key
        if env != None:
            d["env"] = env
        if cwd != None:
            d["cwd"] = cwd
        return self.session.newput_request("run-exec", d)

    def run_exec_stream(
        self,
        path,
        args,
        capture_output=False,
        text=True,
        env=None,
        wd=None,
        path_key=None,
    ):
        """See [penvm.kernels.default.server.RunExecStream][]."""
        d = {
            "args": [str(arg) for arg in args],
            "capture-output": capture_output,
            "text": text,
        }
        if path != None:
            d["path"] = path
        elif path_key != None:
            d["path-key"] = path_key
        if env != None:
            d["env"] = env
        if wd != None:
            d["wd"] = wd
        return self.session.newput_request("run-exec-stream", d)

    def run_python(self, code, args=None, kwargs=None, _globals=None, _locals=None):
        """See [penvm.kernels.default.server.RunPython][]."""
        _locals = copy.deepcopy(_locals) if _locals != None else {}
        _locals["__penvm__"] = True
        d = {
            "code": code,
        }
        if args != None:
            d["args"] = args
        if kwargs != None:
            d["kwargs"] = kwargs
        if _locals != None:
            d["locals"] = _locals
        if _globals != None:
            d["globals"] = _globals
        return self.session.newput_request("run-python", d)

    def run_python_function(self, code, fnname, args=None, kwargs=None):
        """See [penvm.kernels.default.server.RunPythonFunction][]."""
        d = {
            "args": args,
            "code": code,
            "fn-name": fnname,
            "kwargs": kwargs,
        }
        return self.session.newput_request("run-python-function", d)

    def store_drop(self, name):
        """See [penvm.kernels.default.server.StoreDrop][]."""
        d = {
            "name": name,
        }
        return self.session.newput_request("store-drop", d)

    def store_get(self, name):
        """See [penvm.kernels.default.server.StoreGet][]."""
        d = {
            "name": name,
        }
        return self.session.newput_request("store-get", d)

    def store_get_state(self):
        """See [penvm.kernels.default.server.StoreGetState][]."""
        return self.session.newput_request("store-get-state")

    def store_list(self, pattern=None):
        """See [penvm.kernels.default.server.StoreList][]."""
        d = {}
        if pattern:
            d["pattern"] = pattern
        return self.session.newput_request("store-list", d)

    def store_pop(self, name):
        """See [penvm.kernels.default.server.StorePop][]."""
        d = {
            "name": name,
        }
        return self.session.newput_request("store-pop", d)

    def store_put(self, name, data):
        """See [penvm.kernels.default.server.StorePut][]."""
        d = {
            "name": name,
            "data": data,
        }
        return self.session.newput_request("store-put", d)
