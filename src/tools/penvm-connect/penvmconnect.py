#! /usr/bin/env python3
#
# tools/penvmconnect.py

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

"""Connect to the servers running on a PENVM network/machine.
"""

import code
import os.path
import sys
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.client.world import World


def print_usage():
    progname = os.path.basename(sys.argv[0])
    print(
        f"""\
usage: {progname} [<networkstr>]
       {progname} -h|--help

Connect to a PENVM network of running machine instances and interact
in a Python shell.

Arguments:
<networkstr>        Network string consisting of one or more,
                    space-separated, machine connection strings
                    (format <machineid>:<sslprofile>:<host>:<port>)
                    for each running machine. Defaults to
                    PENVM_AUTO_NETWORK.
""",
        end="",
    )


class ArgOpts:
    pass


def main():
    try:
        argopts = ArgOpts()
        argopts.networkstr = os.environ.get("PENVM_AUTO_NETWORK")

        args = sys.argv[1:]
        while args:
            arg = args.pop(0)
            if arg in ["-h", "--help"]:
                print_usage()
                sys.exit(1)
            elif not args:
                argopts.networkstr = arg
            else:
                raise Exception("bad/missing argument")

        if argopts.networkstr in [None, ""]:
            raise Exception("network string not found")
    except SystemExit:
        raise
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        world = World(networkstr=argopts.networkstr)
        network = world.get_network()
        network.boot()
        machines = network.get_machines()
        machconnspecs = [m.get_machconnspec() for m in machines]

        # TODO: restrict globals and locals
        code.interact(
            banner="""Ready.\n"""
            """Use "machines" to access machines.\n"""
            """Use "machconnspecs" to access machine connection specs.\n"""
            """Use "network" for network.""",
            local=dict(globals(), **locals()),
        )
    except SystemExit:
        raise
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
