#! /usr/bin/env python3
#
# tools/penvmserver.py

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

"""PENVM server.
"""

import logging
import os
import os.path
import subprocess
import sys
from sys import stderr
import tempfile
import time
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.server.machine import Machine


# set in main()
logger = None


class Server(BaseObject):
    def __init__(self):
        try:
            super().__init__(None, logger)
            self.logger.debug("init")
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            pass

    def background(self):
        """Put in background.

        Steps:
        1. Forks (multistep).
        2. Closes stdin, stdout, stderr."""

        self.logger.debug("backgrounding ...")

        # fork 1
        pid = os.fork()
        if pid > 0:
            # os._exit(0)
            sys.exit(0)

        os.setsid()
        os.chdir("/")

        # fork 2
        pid = os.fork()
        if pid > 0:
            # os._exit(0)
            sys.exit(0)

        # close fds
        sys.stdout.flush()
        sys.stderr.flush()
        si = open("/dev/null", "r")
        so = open("/dev/null", "a+")
        se = open("/dev/null", "a+")
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        self.logger.debug("background exit")

    def respawn(self, argopts):
        self.logger.debug("respawning ...")

        args = sys.argv[:]

        # force full path
        args[0] = os.path.realpath(args[0])

        f = tempfile.NamedTemporaryFile("rt")
        # print(f"{f.name=} {f.file=}")
        args.insert(1, "--announce-file")
        args.insert(2, f.name)

        # force python3
        if args[0] != "python3":
            args.insert(0, "python3")

        self.logger.debug(f"{args=}")
        cp = subprocess.run(
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            # capture_output=True,
            # text=True,
            cwd="/",
            start_new_session=True,
        )
        # print(f"{cp=}")

        if cp.returncode != 0:
            # print(f"error: failed to respawn", file=sys.stderr, flush=True)
            self.logger.debug(f"failed to respawn {cp.returncode=} {cp.stdout=} {cp.stderr=}")
            sys.exit(cp.returncode)

        if argopts.announce:
            # TODO: change this to a pipe!
            s = "announce::"
            while True:
                s += f.file.read()
                # print(f"{s=}")
                if "\n" in s:
                    break
                time.sleep(0.2)
            print(s, flush=True)
        sys.exit(0)

    def run(self, argopts):
        self.logger.debug("running ...")

        if argopts.background:
            self.background()

        self.logger.debug(
            f"machine configuration: id={argopts.machineid} host={argopts.host} port={argopts.port}"
        )
        self.logger.debug("creating machine ...")

        try:
            machine = Machine(
                argopts.host,
                argopts.port,
                sslprofile=argopts.sslprofile,
                machineid=argopts.machineid,
            )
            time.sleep(0.2)
            if argopts.announce and argopts.announce_filename:
                lhost, lport = machine.get_addr_port()
                self.logger.debug(
                    f"announcement machineid={argopts.machineid} addr={lhost} port={lport} ssl-profile={argopts.sslprofile}"
                )
                print(
                    f"{argopts.machineid}:{argopts.sslprofile or ''}:{lhost}:{lport}",
                    file=open(argopts.announce_filename, "a"),
                    flush=True,
                )
        except Exception as e:
            self.logger.debug(f"failed to start machine {traceback.format_exc()}")

        # sys.exit(0)
        self.logger.debug("starting/running machine ...")
        # TODO: move `ltimeout`` setting elsewhere
        machine.connmgr.ltimeout = argopts.firstwait
        if 1:
            machine.start()
            machine.wait()

        if 0:
            machine.run()
        self.logger.debug("exiting ...")


def print_usage():
    progname = os.path.basename(sys.argv[0])
    print(
        f"""\
usage: {progname} [<options>] <host> [<port>]
       {progname} -h|--help

Start penvm server.

Arguments:
<host>          Address to listen on.
<port>          Port to listen on. Default is 0, which autoselects an
                available port.
--announce      Write <host>:<port> of the listening socket to the
                announce file.
--announce-file <filename>
                Filename to hold the announce info.
--background    Put server into background: detach from session and
                redirect I/O to `/dev/null`.
--firstwait <seconds>
                Time to wait for first connection before exiting.
                Default is 30 seconds.
--machineid <id>
                Id to use for machine.
--ssl-profile <name>
                SSL profile name.
""",
        end="",
    )


class ArgOpts:
    pass


def main():
    global logger

    try:
        argopts = ArgOpts()
        argopts.announce = False
        argopts.announce_filename = None
        argopts.background = False
        argopts.firstwait = 30
        argopts.host = None
        argopts.machineid = None
        argopts.port = 0
        argopts.sslprofile = None

        args = sys.argv[1:]
        while args:
            arg = args.pop(0)
            if arg == "--announce":
                argopts.announce = True
            elif arg == "--announce-file" and args:
                argopts.announce_filename = args.pop(0)
            elif arg == "--background":
                argopts.background = True
            elif arg == "--firstwait" and args:
                argopts.firstwait = int(args.pop(0))
            elif arg in ["-h", "--help"]:
                print_usage()
                sys.exit(0)
            elif arg == "--machineid" and args:
                argopts.machineid = args.pop(0)
            elif arg == "--ssl-profile" and args:
                argopts.sslprofile = args.pop(0)
            else:
                argopts.host = arg
                if args:
                    argopts.port = int(args.pop(0))
                if args:
                    raise Exception()

        if argopts.host == None:
            raise Exception()
    except SystemExit:
        raise
    except Exception as e:
        print("error: bad/missing argument", file=sys.stderr)
        sys.exit(1)

    # set up logger
    username = os.environ.get("USER", os.environ.get("LOGNAME", os.getpid()))
    logging.basicConfig(
        filename=f"/tmp/penvmserver-{username}.log",
        format="%(asctime)s:%(levelname)s:%(process)s:%(name)s:%(lineno)d:%(funcName)s:%(msg)s",
    )
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    logger = logging.getLogger(__name__)

    # create server
    server = Server()

    if not argopts.announce_filename:
        # patch sys.argv for respawn
        sys.argv[0] = os.path.abspath(sys.argv[0])
        server.respawn(argopts)
    else:
        server.run(argopts)


if __name__ == "__main__":
    main()
