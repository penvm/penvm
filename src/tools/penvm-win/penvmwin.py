#! /usr/bin/env python3
#
# tools/penvmwin.py

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

"""Set up (local) windows/terminals for a PENVM network.
"""

import os
import os.path
import subprocess
import sys
import tempfile
import time
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.client.world import World

HEREFILE = os.path.abspath(sys.argv[0])
TERMINAL_EXEC = os.environ.get("PENVM_WIN_TERMINAL", "xterm")


def bytetail_main():
    import codecs
    import fcntl

    def bytetail_open():
        try:
            filename = sys.argv[1]
            # print(f"{filename=}")
            if filename == "-":
                f = os.fdopen(sys.stdin.fileno(), "rb", buffering=0)
            else:
                f = open(filename, "rb", buffering=0)
            # print(f"opening file ({filename=}) ({f=})")
            return f
        except Exception as e:
            traceback.print_exc()
            print(f"error: {e}", file=sys.stderr)
            sys.exit(1)

    def bytetail_read(f):
        try:
            dec = codecs.getincrementaldecoder("utf-8")()

            flags = fcntl.fcntl(f.fileno(), fcntl.F_GETFL)
            flags_block = flags
            flags_nonblock = flags | os.O_NONBLOCK
            while True:
                # read one byte as soon as available
                fcntl.fcntl(f.fileno(), fcntl.F_SETFL, flags_block)
                buf = f.read(1)
                if buf in [b"", None]:
                    # EOF
                    break
                s = dec.decode(buf)
                if s:
                    print(s, end="", flush=True)

                # read chunks of bytes in non-blocking until nothing
                fcntl.fcntl(f.fileno(), fcntl.F_SETFL, flags_nonblock)
                while True:
                    buf = f.read(1024)
                    if buf in [b"", None]:
                        # EOF or timeout
                        break
                    s = dec.decode(buf)
                    if s:
                        print(s, end="", flush=True)
        except Exception as e:
            traceback.print_exc()
            pass

    try:
        while True:
            # print("starting bytetail loop ...")
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f"---- [{timestamp}] ------ ↑ ↑ ↑ ↑ ----------")
            f = bytetail_open()
            bytetail_read(f)
    finally:
        windows.close()

    sys.exit(0)


class Window:
    """Log window which creates and monitors a log/fifo file."""

    tempd = tempfile.TemporaryDirectory()

    def __init__(self, name, title, usefifo=True, cleanlog=True):
        self.name = name
        self.title = title
        self.path = f"{self.tempd.name}/{self.name}"
        self.usefifo = usefifo
        self.cleanlog = cleanlog
        if usefifo:
            os.mkfifo(self.path, mode=0o600)
        else:
            # touch
            self.fd = os.open(self.path, os.O_CREAT, mode=0o600)
            os.close(self.fd)
        self.fd = None
        self.p = None

    def __del__(self):
        """Clean up."""
        try:
            if self.fd != None:
                os.close(self.fd)
                self.fd = None
        except:
            pass

        try:
            if os.path.exists(self.path):
                if self.cleanlog or self.usefifo:
                    os.remove(self.path)
        except:
            pass

    def kill(self):
        try:
            self.p.kill()
            self.p = None
        except:
            pass

    def write(self, buf):
        if self.p == None:
            return
        if self.fd == None:
            self.fd = os.open(self.path, os.O_RDWR)
        os.write(self.fd, buf.encode("utf-8"))

    def terminal(self):
        filename = os.path.basename(TERMINAL_EXEC)
        if filename in ["xterm"]:
            title_opt = "-title"
            execute_opt = "-e"
        elif filename in ["gnome-terminal"]:
            title_opt = "--title"
            execute_opt = "--"
        else:
            title_opt = "--title"
            execute_opt = "-x"

        self.p = subprocess.Popen(
            [TERMINAL_EXEC, title_opt, self.name, execute_opt, HEREFILE, "--bytetail", self.path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            close_fds=True,
        )


class Windows:
    """Manage multiple log windows."""

    def __init__(self, usefifo=True, cleanlog=True):
        self.usefifo = usefifo
        self.cleanlog = cleanlog
        self.windows = {}

    def add(self, name):
        self.windows[name] = Window(
            name,
            f"Machine: {name}",
            usefifo=self.usefifo,
            cleanlog=self.cleanlog,
        )

    def close(self):
        for w in self.windows.values():
            w.kill()

    def get_spec(self):
        # TODO: change method name
        return " ".join([f"{w.name}:{w.path}" for w in self.windows.values()])

    def terminal(self):
        for w in self.windows.values():
            w.terminal()

    def write(self, name, out):
        w = self.windows.get(name)
        if w:
            w.write(str(out))

    def write_all(self, out):
        for w in self.windows.values():
            w.write(out)

    def write_named(self, d):
        for name, out in d.items():
            self.write(name, out)

    def write_sep(self, name, sep=None):
        sep = sep or "-" * 40
        self.write(name, sep)


def print_usage():
    progname = os.path.basename(sys.argv[0])
    print(
        f"""\
usage: {progname} [--shell <path>] [<networkstr]
       {progname} -h|--help

Start log windows for a running PENVM network (at
PENVM_AUTO_NETWORK). Optionally start a shell, else wait
indefinitely (until killed).

Arguments:
<networkstr>        PENVM network of running machine instances.
--shell <path>      Run a shell.
""",
        end="",
    )


class ArgOpts:
    pass


def main():
    try:
        argopts = ArgOpts()
        argopts.networkstr = os.environ.get("PENVM_AUTO_NETWORK")
        argopts.shell = None

        args = sys.argv[1:]

        # special/unadvertised
        if args and args[0] == "--bytetail":
            del sys.argv[1]
            bytetail_main()
            sys.exit(0)

        while args:
            arg = args.pop(0)
            if arg in ["-h", "--help"]:
                print_usage()
                sys.exit(0)
            elif arg == "--shell" and args:
                argopts.shell = args.pop(0)
            elif not args:
                argopts.networkstr = arg
            else:
                raise Exception("bad/missing argument")

        if argopts.networkstr in [None, ""]:
            raise Exception("network string not found")
    except SystemExit:
        raise
    except Exception as e:
        print("error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        world = World()
        network = World.network()

        windows = Windows()

        machidhostports = argopts.machidhostports.split()
        for machidhostport in machidhostports:
            machid, sslprofile, host, port = machidhostport.split(":", 3)
            # print(f"adding machine ({machid})")
            windows.add(machid)
        windows.terminal()

        env = os.environ.copy()
        env["PENVM_WINDOWS"] = windows.get_spec()
        print(f"""export PENVM_WINDOWS="{env.get('PENVM_WINDOWS')}" """)

        if not argopts.shell:
            print("sleeping ...")
            time.sleep(10000000)
        else:
            print(f"starting shell ({argopts.shell}) ...")
            try:
                sp = subprocess.Popen(
                    [argopts.shell],
                    pass_fds=[0, 1, 2],
                    env=env,
                )
            except Exception as e:
                raise Exception(f"failed to start shell ({argopts.shell})")

            try:
                # close streams
                sys.stdin.close()
                sys.stdout.close()
                sys.stderr.close()

                # close fds
                os.close(0)
                os.close(1)
                os.close(2)
            except Exception as e:
                raise Exception("failed to close streams/fds")

            os.waitpid(sp.pid, 0)

        print("exiting")
    except Exception as e:
        # traceback.print_exc()
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
