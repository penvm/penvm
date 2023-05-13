#! /usr/bin/env python3
#
# tools/penvmboot.py

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

"""Set up a PENVM network.
"""

import os
import os.path
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.app.boot import Booter


def print_usage():
    progname = os.path.basename(sys.argv[0])
    print(
        f"""\
usage: {progname} [--shell <path>] [-N <network>] <config>
       {progname} -h|--help

Start PENVM network. Optionally start a shell, else wait indefinitely
(until killed).

Arguments:
<config>            Configuration file (.penvm).
-N <network>        Start named network. Default is "default".
--shell <path>      Run a shell.
--verbose           Output informative details.
""",
        end="",
    )


class ArgOpts:
    pass


def main():
    try:
        argopts = ArgOpts()
        argopts.filename = None
        argopts.network = "default"
        argopts.release = None
        argopts.shell = None
        argopts.verbose = False

        args = sys.argv[1:]
        while args:
            arg = args.pop(0)
            if arg in ["-h", "--help"]:
                print_usage()
                sys.exit(0)
            elif arg == "-N" and args:
                argopts.network = args.pop(0)
            elif arg == "-r" and args:
                argopts.release = args.pop(0)
            elif arg == "--shell" and args:
                argopts.shell = args.pop(0)
            elif arg == "--verbose":
                argopts.verbose = True
            elif not args:
                argopts.filename = arg
            else:
                raise Exception("bad/missing argument")

        if None in [argopts.filename]:
            raise Exception()
    except SystemExit:
        raise
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        booter = Booter(
            argopts.filename,
            argopts.network,
            argopts.release,
        )
        booter.boot()

        if argopts.verbose:
            print(f"machines available ({len(booter.network.get_machines())})")

        if argopts.shell:
            PS1 = f"""[pvshell] {booter.env.get("PS1", "")}"""
            booter.env["PS1"] = PS1
            booter.env["XPS1"] = PS1

        # print env info
        if argopts.verbose:
            print("environment settings:")
            print(f"""    export PENVM_AUTO_NETWORK="{booter.env['PENVM_AUTO_NETWORK']}" """)
            print(f"""    export PENVM_RELEASE={booter.release}""")
            print(f"""    export PYTHONPATH="{booter.env['PYTHONPATH']}" """)
            # print(f"""    export PS1="{PS1}" """)

        if not argopts.shell:
            if argopts.verbose:
                print("sleeping ...")
            booter.wait()
        else:
            if argopts.verbose:
                print(f"starting shell ({argopts.shell}) ...")
            booter.shell(argopts.shell)

        if 0:
            world = World(filename=argopts.filename)
            network = world.get_network(argopts.network)
            # print(f"{network=}")
            if network == None or network.get_targets() in [None, []]:
                raise Exception(f"network ({argopts.network}) not found")
            network.boot()

            machines = network.get_machines()
            if machines in [None, []]:
                raise Exception("no machines")
            if None in machines:
                raise Exception("not all machines found")

            if argopts.verbose:
                print(f"machines available ({len(machines)})")

            machconnspecs = [MachineConnectionSpec(machine=machine) for machine in machines]

            release = argopts.release or world.get_meta("release") or get_version_string()
            if release == None:
                raise Exception(f"could not determine release value")

            releasepath = os.path.expanduser(f"~/.penvm/releases/{release}")
            releaselibs = ":".join([f"{releasepath}/{name}" for name in ["penvmlib-client"]])

            # patch env
            env = os.environ.copy()
            PENVM_AUTO_NETWORK = " ".join([str(machconnspec) for machconnspec in machconnspecs])
            PYTHONPATH = env.get("PYTHONPATH")
            PYTHONPATH = f":{PYTHONPATH}" if PYTHONPATH != None else ""
            PS1 = f"""[pvshell] {env["PS1"]}"""
            env["PENVM_AUTO_NETWORK"] = PENVM_AUTO_NETWORK
            env["PYTHONPATH"] = f"{releaselibs}{PYTHONPATH}"
            env["PENVM_RELEASE"] = release
            env["PS1"] = PS1
            env["XPS1"] = PS1

            # print env info
            if argopts.verbose:
                print("environment settings:")
                print(f"""    export PENVM_AUTO_NETWORK="{PENVM_AUTO_NETWORK}" """)
                print(f"""    export PENVM_RELEASE={release}""")
                print(f"""    export PYTHONPATH="{releaselibs}:$PYTHONPATH" """)
                print(f"""    export PS1="{PS1}" """)

            if not argopts.shell:
                if argopts.verbose:
                    print("sleeping ...")
                time.sleep(10000000)
            else:
                if argopts.verbose:
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
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)

    if argopts.verbose:
        print("exiting ...")


if __name__ == "__main__":
    main()
