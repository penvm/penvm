#
# penvm/kernels/default/server.py

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

"""Server-side "default" kernel operations.

The default operations providing the necessary functionality for
many/most use cases including support for: filesystem, kv store, and
binary and python code execution.

For an explanation of the Request and Response sections see
[penvm.kernels.core.server][]."""

import fnmatch

try:
    import grp
except:
    grp = None
import logging
import os
import os.path
from pathlib import Path

try:
    import pwd
except:
    pwd = None
import select
import shutil
import subprocess
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.kernels.base.server import Op
from penvm.kernels.core.server import Kernel as _Kernel
from penvm.lib.message import ErrorResponse, OkResponse, Payload


logger = logging.getLogger(__name__)


class FileGetBlock(Op):
    """Read data (block) from a file object.

    Attributes: Request:
        path (str): File path.
        size (int): Size (in bytes) to read. Defaults to 1024.
        start (int): Offset from file start. Defaults to 0.

    Attributes: OkResponse:
        data (bytes): Bytes (up to `size`) from file block.
        size (int): Size (in bytes) read.
        start (int): Offset from file start.

    Attributes: ErrorResponse:
        -: Error message.
    """

    def run(self, ctxt, req):
        try:
            payload = req.payload
            path = payload.get("path")
            size = payload.get("size", 1024)
            start = payload.get("start", 0)

            if not path:
                return ErrorResponse(
                    "path not provided",
                    refmsg=req,
                )

            pp = Path(path)
            if not pp.exists():
                return ErrorResponse(
                    f"path ({path}) not found",
                    refmsg=req,
                )

            try:
                f = pp.open("rb")
                f.seek(start)
                return OkResponse(
                    payload=Payload(
                        {"data": f.read(size), "size": size, "start": start},
                    ),
                    refmsg=req,
                )
            except Exception as e:
                return ErrorResponse(
                    f"{e}",
                    refmsg=req,
                )
        except Exception as e:
            self.logger.error(f"{e}")


class FileGetStatus(Op):
    """Get file status (`os.stat()`).

    Attributes: Request:
        path (str): File path to stat.

    Attributes: OkResponse:
        mode (int): File mode.
        ino (int): Inode.
        dev (int): Device identifier.
        nlink (int): Number of hard links.
        uid (int): User id of file owner.
        gid (int): Group id of file owner.
        size (int): File size (in bytes).
        atime (int): Most recent access time (in seconds).
        mtime (int): Most recent content modication time (in seconds).
        ctime (int): Most recent metadata change time (in seconds).

    Attributes: ErrorResponse:
        -: Error message.
    """

    def run(self, ctxt, req):
        try:
            path = req.payload.get("path")
            if path != None:
                return ErrorResponse("path not provided")

            if not os.path.exists(path):
                return ErrorResponse("path not found")

            st = os.stat(path)
            return OkResponse(
                payload={
                    "mode": st.st_mode,
                    "ino": st.st_ino,
                    "dev": st.st_dev,
                    "nlink": st.st_nlink,
                    "uid": st.st_uid,
                    "gid": st.st_gid,
                    "size": st.st_size,
                    "atime": st.st_atime,
                    "mtime": st.st_mtime,
                    "ctime": st.st_ctime,
                },
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")


class FileList(Op):
    """List filenames/dirnames at filesystem path.

    Attributes: Request:
        path (str): Directory path.
        filter (str): Filename filter. Defaults to no filter.
        sort (bool): Sort names. Defaults to False.

    Attributes: Response:
        names (list of str): List of directory entries.

    Attributes: ErrorResponse:
        -: Error message.
    """

    def run(self, ctxt, req):
        try:
            payload = req.payload
            path = payload.get("path")
            pattern = payload.get("pattern")
            sort = payload.get("sort", False)
            split = payload.get("split", False)

            if not path:
                return ErrorResponse(
                    "path not provided",
                    refmsg=req,
                )

            try:
                if split:
                    if os.path.isdir(path):
                        for root, dirnames, filenames in os.walk(path):
                            break
                    else:
                        root, dirnames, filenames = None, [], [path]

                    if pattern:
                        dirnames = fnmatch.filter(dirnames, pattern)
                        filenames = fnmatch.filter(filenames, pattern)
                    if sort:
                        dirnames.sort()
                        filenames.sort()

                    return OkResponse(
                        payload=Payload(
                            {
                                "dirnames": dirnames,
                                "filenames": filenames,
                            }
                        ),
                        refmsg=req,
                    )
                else:
                    if os.path.isdir(path):
                        names = os.listdir(path)
                    elif os.path.isfile(path):
                        names = [os.path.basename(path)]

                    if pattern:
                        names = fnmatch.filter(names, pattern)
                    if sort:
                        names.sort()

                    return OkResponse(
                        payload=Payload({"names": names}),
                        refmsg=req,
                    )
            except Exception as e:
                return ErrorResponse(
                    f"{e}",
                    refmsg=req,
                )
        except Exception as e:
            self.logger.error(f"{e}")


class FilePutBlock(Op):
    """Write data (block) to a file object.

    Attributes: Request:
        data (bytes): Data to store.
        path (str): File path.
        size (int): Size (in bytes) of data.
        start (int): Offset from file start. Defaults to 0.
        truncate (bool): Flag to indicate truncation of file. Defaults
            to False.

    Attributes: OkResponse:
        -: Empty.

    Attributes: ErrorResponse:
        -: Error message.
    """

    def run(self, ctxt, req):
        try:
            payload = req.payload
            data = payload.get("data")
            path = payload.get("path")
            size = payload.get("size", len(data))
            start = payload.get("start", 0)
            truncate = payload.get("truncate", False)

            if not path:
                return ErrorResponse(
                    "path not provided",
                    refmsg=req,
                )
            if not data:
                return ErrorResponse(
                    "data not provided",
                    refmsg=req,
                )

            pp = Path(path)
            if not pp.parent.is_dir():
                return ErrorResponse(
                    f"parent path ({pp.parent.isabsolute()}) does not exist",
                    refmsg=req,
                )

            try:
                with pp.open("wb+") as f:
                    if truncate:
                        # TODO: support truncateat?
                        f.truncate()
                    f.seek(start)
                    if size != None:
                        f.write(data[:size])
                    else:
                        f.write(data)
                    f.close()
                return OkResponse(
                    refmsg=req,
                )
            except Exception as e:
                return ErrorResponse(
                    f"{e}",
                    refmsg=req,
                )
        except Exception as e:
            self.logger.error(f"{e}")


class FileSetStatus(Op):
    """Set file status settings.

    Attributes: Request:
        path (str): File object path.
        uid (int): User id of file owner.
        user (str): User name of file owner.
        gid (int): Group id of file owner.
        group (int): Group name of file owner.
        mode (int): File mode.

    Attributes: Response:
        -: None.
    """

    def run(self, ctxt, req):
        try:
            payload = req.payload
            path = payload.get("path")
            uid = payload.get("uid")
            user = payload.get("user")
            gid = payload.get("gid")
            group = payload.get("group")
            mode = payload.get("mode")

            if path == None:
                return

            if not os.path.exists(path):
                return

            if user and pwd:
                pw = pwd.getpwnam(user)
                if pw:
                    uid = pw.pw_uid
            if group and grp:
                gr = grp.getgrnam(group)
                if gr:
                    gid = gr.gr_gid

            if uid == None:
                uid = -1
            if gid == None:
                gid = -1

            try:
                if mode != None:
                    os.chmod(path, mode)
            except:
                pass

            try:
                if uid != -1 and gid != -1:
                    os.chown(path, uid, gid)
            except:
                pass
        except Exception as e:
            self.logger.error(f"{e}")


class RunExec(Op):
    """Run a locally available executable via a path with arguments.

    Attributes: Request:
        args (list of string): Arguments. Defaults to empty list.
        capture_output (bool): Capture stdout/stderr. Defaults to
            True.
        env (dict|list of strings): Dictionary or list of strings for
            the environment.
        path (str): Path of executable. See also `path-key`.
        path-key (str): Name of item in file kvstore. See also `path`.
        text (bool): Treat output as "text". Otherwise, "binary".
        cwd (str): Working directory to run under. Defaults to "/".

    Attributes: OkResponse:
        returncode (int): Exit/return code
        stderr (str): stderr output
        stdout (str): stdout output
    """

    def run(self, ctxt, req):
        """Run operation."""
        try:
            self.logger.debug("run")

            payload = req.payload

            args = list(payload.get("args", []))
            capture_output = payload.get("capture-output", True)
            env = payload.get("env", None)
            path = payload.get("path")
            path_key = payload.get("path-key")
            text = payload.get("text", False)
            cwd = payload.get("cwd", "/")

            self.logger.debug(f"path {path} path-key {path_key}")
            if path == None:
                path = ctxt.machine.fkvstore.get_path(path_key)
                # TODO: make it executable in a different call?
                os.chmod(path, 0o700)

            if None in [path]:
                return ErrorResponse(
                    "path not provided",
                    refmsg=req,
                )
            elif not os.path.exists(path):
                _path = shutil.which(path)
                if _path == None:
                    return ErrorResponse(
                        f"file ({path}) not found",
                        refmsg=req,
                    )
                path = _path

            kwargs = {
                "capture_output": capture_output,
                "cwd": cwd,
                "text": text,
            }
            if env:
                kwargs["env"] = env

            cp = subprocess.run(
                [path] + args,
                **kwargs,
            )
            return OkResponse(
                payload=Payload(
                    {
                        "stdout": cp.stdout,
                        "stderr": cp.stderr,
                        "returncode": cp.returncode,
                    }
                ),
                refmsg=req,
            )
        except Exception as e:
            return ErrorResponse(
                f"{e}",
                refmsg=req,
            )


class RunExecStream(Op):
    """Run a locally available executable via a path with arguments
    with streaming capability.

    Whereas `RunExec` runs the executable, waits for it to complete,
    then collects and returns the `stdout`, `stderr`, and
    `returncode`, `RunExecStream` streams partial results over the
    lifetime of the operation. As such, an undetermined number of
    responses are collected and returned. When the operation
    completes, a final message will contain a `returncode` != `None`.
    The requester must perform queue management (e.g.,
    `session-pop-omq` at the server, and consume the messages at the
    client side).

    Note:
        `stdin` is not supported.

    Attributes: Request:
        args (list of string): Arguments. Defaults to empty list.
        capture_output (bool): Capture stdout/stderr. Defaults to
            True.
        env: (dict|list of strings): Dictionary or list of strings for
            the environment.
        path (str): Path of executable. See also `path-key`.
        path-key (str): Name of item in file kvstore. See also `path`.
        text (bool): Treat output as "text". Otherwise, "binary".
        cwd (str): Working directory to run under. Defaults to "/".

    Attributes: OkResponse:
        returncode (int|None): Exit/return code. Last response sends
            `returncode` != `None`.
        stderr (str): `stderr` output.
        stdout (str): `stdout` output.
    """

    def run(self, ctxt, req):
        """Run operation."""
        try:
            self.logger.debug("run")

            payload = req.payload

            args = list(payload.get("args", []))
            capture_output = payload.get("capture-output", True)
            env = payload.get("env", None)
            path = payload.get("path")
            path_key = payload.get("path-key")
            text = payload.get("text", False)
            cwd = payload.get("cwd", "/")

            self.logger.debug(f"path {path} path-key {path_key}")
            if path == None:
                path = ctxt.machine.fkvstore.get_path(path_key)
                # TODO: make it executable in a different call?
                os.chmod(path, 0o700)

            if None in [path]:
                return ErrorResponse(
                    "path not provided",
                    refmsg=req,
                )
            elif not os.path.exists(path):
                return ErrorResponse(
                    f"file ({path}) not found",
                    refmsg=req,
                )
            else:
                kwargs = {
                    "cwd": cwd,
                    "text": text,
                }
                if capture_output:
                    kwargs["stdout"] = subprocess.PIPE
                    kwargs["stderr"] = subprocess.PIPE
                if env:
                    kwargs["env"] = env

                if 0:
                    # no buffering, allows short reads
                    kwargs["bufsize"] = 0
                if 1:
                    # line buffered
                    kwargs["bufsize"] = 1

                try:
                    sp = subprocess.Popen(
                        [path] + args,
                        **kwargs,
                    )

                    POLLIN = select.POLLIN
                    POLLOUT = select.POLLOUT
                    POLLHEN = select.POLLHUP | select.POLLERR | select.POLLNVAL

                    stdout_fd = sp.stdout.fileno()
                    stderr_fd = sp.stderr.fileno()

                    poll = select.poll()
                    poll.register(stdout_fd, select.POLLIN)
                    poll.register(stderr_fd, select.POLLIN)

                    self.logger.debug(f"{sp.stdout=} {sp.stdout.line_buffering=} {sp.stderr=}")
                    sp.stdout.reconfigure(line_buffering=True)
                    sp.stderr.reconfigure(line_buffering=True)

                    self.logger.debug(f"starting ...")

                    nfds = 2
                    while nfds:
                        populated = False
                        resp = OkResponse(
                            refmsg=req,
                        )
                        for fd, event in poll.poll():
                            self.logger.debug(f"{fd=} {event=}")
                            if event & POLLIN:
                                if fd == stdout_fd:
                                    populated = True
                                    resp.payload["stdout"] = sp.stdout.read(128)
                                else:
                                    # assume stderr
                                    populated = True
                                    resp.payload["stderr"] = sp.stderr.read(128)
                            if event & POLLHEN:
                                poll.unregister(fd)
                                nfds -= 1
                        if populated:
                            ctxt.session.omq.put(resp)

                    sp.wait()
                    self.logger.debug(f"final")
                    resp = OkResponse(
                        refmsg=req,
                    )
                    resp.payload["returncode"] = sp.returncode
                    return resp
                except Exception as e:
                    # TODO: cleanup? kill?
                    sp.kill()
                    raise
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                f"{e}",
                refmsg=req,
            )


class RunPython(Op):
    """Run python code snippet with typed args and typed kwargs.

    The code is executed in a thread of the `machine`.

    Danger:
        Running in the machine process can cause problems.

    See [penvm.kernels.default.server.RunExec][].

    Attributes: Request:
        args (typed list): Arguments.
        code (str): Code snippet.
        globals (typed dict): Dictionary of "globals" to use. Defaults
            to current "globals".
        kwargs (typed dict): Keyword arguments.
        locals (typed dict): Dictionary of "locals" to use. Defaults
            to empty dict.
        path-key (str): Path of code snippet in file kvstore. See also
            `code`.

    Attributes: Response:
        return-value (typed value): Taken from `returnvalue` in locals.
    """

    def run(self, ctxt, req):
        """Run operation."""
        try:
            self.logger.debug("run")
            payload = req.payload

            code = payload.get("code")
            path_key = payload.get("path-key")

            if code == None:
                code = ctxt.machine.fkvstore.get(path_key)

            # TODO: should args and kwargs be distinct or in one of locals or globals
            args = payload.get("args", [])
            _locals = payload.get("locals", {})
            _globals = payload.get("globals", None)
            kwargs = payload.get("kwargs", {})

            if None in [code]:
                return ErrorResponse(
                    "code not provided",
                    refmsg=req,
                )
            else:
                try:
                    co = compile(code, "<string>", "exec")
                except Exception as e:
                    self.logger.warning(f"EXCEPTION ({e})")
                    co = None

                if co == None:
                    return ErrorResponse(
                        "code does not compile",
                        refmsg=req,
                    )
                else:
                    try:
                        exec(code, _globals, _locals)
                        return OkResponse(
                            payload=Payload({"return-value": _locals.get("returnvalue")}),
                            refmsg=req,
                        )
                    except Exception as e:
                        self.logger.warning(f"EXCEPTION ({e})")
                        resp = ErrorResponse(
                            f"{e}",
                            refmsg=req,
                        )
        except Exception as e:
            self.logger.error(f"{e}")


class RunPythonFunction(Op):
    """Run function from python code snippet with typed args and typed
    kwargs.

    The code is executed in a thread of the `machine`.

    Danger:
        Running in the machine process can cause problems.

    See [penvm.kernels.default.server.RunExec][].

    Attributes: Request:
        args (typed list): Arguments.
        code (str): Code snippet.
        code-key (str): Code snippet in file kvstore. See also `code`.
        globals (typed dict): Dictionary of "globals" to use. Defaults
            to current "globals"
        kwargs (typed dict): Keyword arguments.
        locals (typed dict): Dictionary of "locals" to use. Defaults
            to empty dict
        path-key (str): Path of code snippet in file kvstore. See also
            `code`.

    Attributes: OkResponse:
        return-value (typed): Return value from function call.
    """

    code_template = """\
fn = globals().get(fnname)
__penvm_returnvalue__ = fn(*args, **kwargs)
"""

    def run(self, ctxt, req):
        """Run operation."""
        try:
            self.logger.debug("run")
            payload = req.payload

            # TODO: tweak args and kwargs handling
            args = payload.get("args", [])
            code = payload.get("code")
            path_key = payload.get("path-key")
            if code == None:
                code = ctxt.machine.fkvstore.get(path_key)
            fnname = payload.get("fn-name")
            kwargs = payload.get("kwargs") or {}

            _globals = payload.get("globals", {})
            _locals = payload.get(
                "locals",
                {
                    "args": args,
                    "fnname": fnname,
                    "kwargs": kwargs,
                },
            )

            if code == None:
                return ErrorResponse(
                    "code not provided",
                    refmsg=req,
                )
            elif fnname == None:
                return ErrorResponse(
                    "fnname not provided",
                    refmsg=req,
                )
            else:
                try:
                    co = compile(code, "<string>", "exec")
                except Exception as e:
                    self.logger.warning(f"EXCEPTION ({e})")
                    co = None

                if co == None:
                    return ErrorResponse(
                        "code does not compile",
                        refmsg=req,
                    )
                else:
                    try:
                        # load provided code
                        exec(code, _globals)
                        # run fn
                        exec(self.code_template, _globals, _locals)
                        resp = OkResponse(
                            payload=Payload(
                                {"return-value": _locals.get("__penvm_returnvalue__")}
                            ),
                            refmsg=req,
                        )
                        return resp
                    except Exception as e:
                        self.logger.warning(f"EXCEPTION ({e})")
                        resp = ErrorResponse(
                            f"{e}",
                            refmsg=req,
                        )
        except Exception as e:
            self.logger.error(f"{e}")


class StoreDrop(Op):
    """Drop item from store.

    Attributes: Request:
        name: Item name.
    TODO: should an reponse be sent?
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            name = req.payload.get("name")
            if name != None:
                ctxt.machine.fkvstore.drop(name)
        except Exception as e:
            self.logger.error(f"{e}")


class StoreGet(Op):
    """Get item from store.

    Attributes: Request:
        name: Item name.

    Attributes: Response:
        data: Data as text or binary.
        name: Item name.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            name = req.payload.get("name")
            data = ctxt.machine.fkvstore.get(name)
            if data == None:
                return ErrorResponse(
                    f"name ({name}) not in store",
                    refmsg=req,
                )
            else:
                return OkResponse(
                    payload=Payload(
                        {
                            "name": name,
                            "data": data,
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


class StoreGetState(Op):
    """Get store state.

    Attributes: Request:
        : Unused.

    Attributes: OkResponse:
        : Dictionary of state.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")
            return OkResponse(
                payload=Payload(ctxt.machine.fkvstore.state()),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                "store not found",
                refmsg=req,
            )


class StoreList(Op):
    """Get item from store.

    Attributes: Request:
        -: Unused.

    Attributes: Response:
        names: Item name.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            fkvstore = ctxt.machine.fkvstore
            return OkResponse(
                payload=Payload({"names": list(fkvstore.keys())}),
                refmsg=req,
            )
        except Exception as e:
            self.logger.error(f"{e}")
            return ErrorResponse(
                f"{e}",
                refmsg=req,
            )


class StorePop(Op):
    """Get item from store and remove from store.

    Attributes: Request:
        name: Item name.

    Attributes: Response:
        data: Data as text or binary.
        name: Item name.
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            name = req.payload.get("name")
            resp = StoreGet().run(ctxt, req)
            if name != None:
                ctxt.machine.fkvstore.drop(name)
            return resp
        except Exception as e:
            self.logger.error(f"{e}")


class StorePut(Op):
    """Put item into store.

    Attributes: Request:
        data: Data as text or binary.
        name: Item name.
    TODO: should an reponse be sent?
    """

    def run(self, ctxt, req):
        try:
            self.logger.debug("run")

            ftype = req.payload.get("type", "text")
            name = req.payload.get("name")
            data = req.payload.get("data")
            if None in [name, data]:
                # DROP!
                self.logger.debug(f"drop for bad 'name' or 'data'")
                return
            if data:
                self.logger.debug(f"putting {name=} {ftype=} {ctxt.machine.fkvstore.dirpath=} ...")
                ctxt.machine.fkvstore.put(name, data)
        except Exception as e:
            self.logger.error(f"{e}")


class Kernel(_Kernel):
    name = "default"

    def __init__(self):
        super().__init__()
        self.ops.update(
            {
                "file-get-block": FileGetBlock(),
                "file-get-stat": FileGetStatus(),
                "file-list": FileList(),
                "file-put-block": FilePutBlock(),
                "file-set-stat": FileSetStatus(),
                "run-exec": RunExec(),
                "run-exec-stream": RunExecStream(),
                "run-python": RunPython(),
                "run-python-function": RunPythonFunction(),
                "store-drop": StoreDrop(),
                "store-get": StoreGet(),
                "store-get-state": StoreGetState(),
                "store-list": StoreList(),
                "store-pop": StorePop(),
                "store-put": StorePut(),
            }
        )
