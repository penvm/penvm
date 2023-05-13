#! /usr/bin/env -S python3 -B
#
# penvmbuild.py

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

"""Build and install PENVM.

Most items are taken from src/ to populate the install directory as:
    bin/
    lib/
    share/penvm

All executables are single zipapp applications. The penvmlib is a
zip archive of all the source. Documents, etc. are under the share
directory.
"""

import os
import os.path
import shutil
import subprocess
import sys
import traceback

HEREFILE = os.path.abspath(sys.argv[0])
HEREDIR = os.path.dirname(HEREFILE)
TOPDIR = HEREDIR

SRCDIR = f"{TOPDIR}/src"
LIBDIR = f"{SRCDIR}/lib"
TESTSDIR = f"{SRCDIR}/tests"
TOOLSDIR = f"{SRCDIR}/tools"

PENVMZIP = f"{TOOLSDIR}/penvm-zip/penvmzip.py"

PENVM_CLIENT_LIBS = [
    "penvmlib-client",
]

PENVM_SERVER_LIBS = [
    "penvmlib-server",
]

APPS = [
    # (appname, appdir, modname, penvmlibs, extralibs)
    ("penvm-connect", f"{TOOLSDIR}/penvm-connect", "penvmconnect", PENVM_CLIENT_LIBS, None),
    ("penvm-boot", f"{TOOLSDIR}/penvm-boot", "penvmboot", PENVM_CLIENT_LIBS, None),
    ("penvm-deploy", f"{TOOLSDIR}/penvm-deploy", "penvmdeploy", PENVM_CLIENT_LIBS, None),
    ("penvm-win", f"{TOOLSDIR}/penvm-win", "penvmwin", PENVM_CLIENT_LIBS, None),
    ("penvm-zip", f"{TOOLSDIR}/penvm-zip", "penvmzip", PENVM_CLIENT_LIBS, None),
    ("penvm-server", f"{TOOLSDIR}/penvm-server", "penvmserver", PENVM_SERVER_LIBS, None),
]

LIBS = [
    # (libname, libdirs, extralibs)
    (
        "penvmlib-client",
        [
            f"{SRCDIR}/client",
            f"{SRCDIR}/lib",
            f"{SRCDIR}/kernels",
            f"{SRCDIR}/app",
            f"{SRCDIR}/ext",
        ],
        None,
    ),
    # ("penvmlib-lib", [f"{SRCDIR}/lib"], None),
    # ("penvmlib-kernels", [f"{SRCDIR}/kernels"], None),
    ("penvmlib-server", [f"{SRCDIR}/server", f"{SRCDIR}/lib", f"{SRCDIR}/kernels"], None),
]

TESTS = [
    ("queue_test", f"{TESTSDIR}/queue_test.py", "queue_test", None, None),
    ("semaphore_test", f"{TESTSDIR}/semaphore_test.py", "semaphore_test", None, None),
    ("session_test", f"{TESTSDIR}/session_test.py", "session_test", None, None),
]


def build_apps(apps, libdir, dstdir):
    """Build a zip application with app file and libs then store in a
    directory."""
    for appname, appdir, modname, libs, extralibs in apps:
        print()
        extralibs = extralibs or []
        dstfilename = f"{dstdir}/{appname}"
        pargs = [
            PENVMZIP,
            "app",
            "-o",
            dstfilename,
            "-m",
            modname,
        ]
        if libs:
            for lib in libs:
                pargs.extend(["-l", f"{libdir}/{lib}"])
        if extralibs:
            for extralib in extralibs:
                pargs.extend(["-l", extralib])
        pargs.append(appdir)
        subprocess.run(pargs)
        os.chmod(dstfilename, 0o755)


def build_libs(libs, dstdir):
    """Build a zip library with libs then store in a directory."""
    for libname, libdirs, extralibs in libs:
        print()
        extralibs = extralibs or []
        dstfilename = f"{dstdir}/{libname}"
        pargs = [
            PENVMZIP,
            "lib",
            "-o",
            dstfilename,
        ]
        for extralib in extralibs:
            pargs.extend(["-l", extralib])
        if libdirs:
            pargs.extend(libdirs)
        subprocess.run(pargs)
        os.chmod(dstfilename, 0o644)


def print_usage():
    progname = os.path.basename(sys.argv[0])
    print(
        f"""\
usage: {progname} <installdir>
       {progname} -h|--help

Build files and install.

Arguments:
<installdir>            Installation directory.
""",
        end="",
    )


class ArgOpts:
    pass


def main():
    try:
        argopts = ArgOpts()
        argopts.installdir = None

        args = sys.argv[1:]
        while args:
            arg = args.pop(0)
            if arg in ["-h", "--help"]:
                print_usage()
                sys.exit(0)
            elif not args:
                argopts.installdir = arg
            else:
                raise Exception()

        if argopts.installdir == None:
            raise Exception()
    except SystemExit:
        raise
    except Exception as e:
        print(e)
        msg = e.msg if e and e.msg != "" else "bad/missing arguments"
        print(f"error: {msg}", file=sys.stderr)
        sys.exit(1)

    try:
        bindir = f"{argopts.installdir}/bin"
        libdir = f"{argopts.installdir}/lib"
        testsdir = f"{argopts.installdir}/tests"
        sharedir = f"{argopts.installdir}/share/penvm"

        os.makedirs(bindir, exist_ok=True)
        os.makedirs(libdir, exist_ok=True)
        os.makedirs(sharedir, exist_ok=True)
        os.makedirs(testsdir, exist_ok=True)

        # this tool: penvmzip
        # shutil.copy(PENVMZIP, f"{bindir}/penvm-zip")

        # library (FIRST!)
        build_libs(LIBS, libdir)

        # apps
        build_apps(APPS, libdir, bindir)
        build_apps(TESTS, libdir, testsdir)

        # miscellaneous
        shutil.copy(f"{TOPDIR}/README.md", f"{sharedir}/README.md")
        shutil.copy(f"{TOPDIR}/LICENSE", f"{sharedir}/LICENSE")
    except Exception as e:
        traceback.print_exc()
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
