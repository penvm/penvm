#! /usr/bin/env -S python3 -B
#
# tools/penvmdeploy.py

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

"""PENVM library and server deployment tool.

Using a .penvm configration, deploys the zipped penvm libraries and
zipapp penvmserver to a set of targets.
"""

import os.path
import subprocess
import sys
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union

HEREFILE = os.path.abspath(sys.argv[0])
HEREDIR = os.path.dirname(HEREFILE)
TOPDIR = os.path.dirname(HEREDIR)

BINDIR = f"{TOPDIR}/bin"
LIBDIR = f"{TOPDIR}/lib"

PENVMLIBS = [
    f"{LIBDIR}/penvmlib-client",
    # f"{LIBDIR}/penvmlib-kernels",
    # f"{LIBDIR}/penvmlib-lib",
    f"{LIBDIR}/penvmlib-server",
]
PENVMSERVER = f"{BINDIR}/penvm-server"

# access local libs
sys.path.insert(0, f"{LIBDIR}/penvmlib-lib")
sys.path.insert(0, f"{LIBDIR}/penvmlib-client")

from penvm.client.world import World
from penvm.lib.misc import get_version_string


def deploy_penvmfiles(release, fshomeid, user, host, filenames, dryrun=False):
    """Install file(s) under ~/.penvm/releases/<release>."""

    dstdir = f".penvm/releases/{release}"

    for filename in filenames:
        print(f"{filename} -> {user or ''}@{host} fshomeid ({fshomeid}) release ({release}) ... ")

        if dryrun:
            continue

        cp = remote_mkdir(user, host, dstdir)
        if cp.returncode != 0:
            print(f"failed ({cp.stderr})")
            continue

        dstpath = f"{dstdir}/{os.path.basename(filename)}"
        cp = remote_copyfile(user, host, filename, dstpath)
        if cp.returncode != 0:
            print(f"failed ({cp.stderr})")


def remote_copyfile(user, host, srcpath, dstpath):
    """Copy local file to remote."""

    userhost = f"{user}@{host}" if user != None else host
    userhostpath = f"{userhost}:{dstpath}"
    spargs = [
        "scp",
        srcpath,
        userhostpath,
    ]
    cp = subprocess.run(spargs, capture_output=True)
    return cp


def remote_mkdir(user, host, path):
    """Create remote directory."""

    userhost = f"{user}@{host}" if user != None else host
    spargs = [
        "ssh",
        userhost,
        "python3",
        "-c",
        f"""'import os; os.makedirs("{path}", exist_ok=True)'""",
    ]
    cp = subprocess.run(spargs, capture_output=True)
    return cp


def print_usage():
    progname = os.path.basename(sys.argv[0])
    print(
        f"""\
usage: {progname} [--dry] [-N <network>[,...] [-l <libfile>] [-s <serverfile>] <config>
       {progname} [--list] <config>
       {progname} -h|--help

Deploy libraries and penvmserver to targets taken from a .penvm
configuration file.

Arguments:
<config>            Configuration file (.penvm suffix).
--dry               Run without making any changes.
-l <libfile>        Libraries. Default is from lib/.
-N <network>[,...]  Apply to targets in named networks.
-r <release>        Deploy to alternate release name.
-s <serverfile>     Server file. Default is from bin/.
""",
        end="",
    )


class ArgOpts:
    pass


def main():
    try:
        argopts = ArgOpts()
        argopts.dryrun = False
        argopts.list = False
        argopts.networks = None
        argopts.penvmlib_filenames = PENVMLIBS
        argopts.penvmserver_filename = PENVMSERVER
        argopts.release = None
        argopts.world_filename = None

        args = sys.argv[1:]
        while args:
            arg = args.pop(0)

            if arg in ["-h", "--help"]:
                print_usage()
                sys.exit(0)
            elif arg == "--dry":
                argopts.dryrun = True
            elif arg == "-l" and args:
                argopts.penvmlib_filename = args.pop(0)
            elif arg == "--list":
                argopts.list = True
            elif arg == "-N":
                argopts.networks = args.pop(0).split(",")
            elif arg == "-r" and args:
                argopts.release = args.pop(0)
            elif arg == "-s" and args:
                argopts.penvmserver_filename = args.pop(0)
            elif not args:
                argopts.world_filename = arg
            else:
                raise Exception()

        if argopts.list:
            if None in [argopts.world_filename]:
                raise Exception()
        else:
            if None in [argopts.penvmserver_filename, argopts.world_filename]:
                raise Exception()
    except SystemExit:
        raise
    except Exception as e:
        print(f"error: bad/missing arguments ({e})", file=sys.stderr)
        sys.exit(1)

    try:
        try:
            world = World(filename=argopts.world_filename)
        except Exception as e:
            print(e)
            print(
                f"error: bad or nonexistent config file ({argopts.world_filename})",
                file=sys.stderr,
            )
            sys.exit(1)

        if argopts.list == True:
            network_names = world.get_network_names()
            print(f"networks: {list(network_names)}")

            for network_name in network_names:
                network = world.get_network(network_name)
                if network != None:
                    print(f"targets[{network_name}]: {list(network.get_target_names())}")

            print(f"targets[*]: {list(world.get_target_names())}")
            sys.exit(0)

        if argopts.networks:
            target_names = set()
            for network_name in argopts.networks:
                network = world.get_network(network_name)
                if network != None:
                    target_names.update(network.get_target_names())
        else:
            target_names = world.get_target_names()

        # print(f"{target_names=}")

        userfshomeid2hosts = {}
        for target_name in target_names:
            target = world.get_target(target_name)
            # print(f"{target=}")
            fshomeid = target.config.get("fshome-id")
            user = target.config.get("user")
            host = target.config.get("host")
            hosts = userfshomeid2hosts.setdefault((user, fshomeid), set())
            hosts.add(host)

        # print(f"{userfshomeid2hosts=}")

        filenames = argopts.penvmlib_filenames[:]
        filenames.append(argopts.penvmserver_filename)

        release = argopts.release or world.get_meta("release") or get_version_string()

        for userfshomeid, hosts in userfshomeid2hosts.items():
            user, fshomeid = userfshomeid
            if fshomeid != None:
                hosts = [list(hosts)[0]]
            for host in hosts:
                deploy_penvmfiles(
                    release,
                    fshomeid,
                    user,
                    host,
                    filenames,
                    dryrun=argopts.dryrun,
                )

    except SystemExit:
        raise
    except Exception as e:
        traceback.print_exc()
        print(f"unexpected error ({e})", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
