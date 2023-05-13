#! /usr/bin/env -S python3 -B
#
# tools/penvmzip.py

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

"""Build penvm app or lib file.
"""

import os
import os.path
import sys
import traceback
import zipapp
import zipfile
from typing import Any, Callable, Dict, List, Tuple, Type, Union

HEREFILE = os.path.realpath(sys.argv[0])
HEREDIR = os.path.dirname(HEREFILE)
PENVMLIBFILE = os.path.abspath(f"{HEREDIR}/../../lib/penvmlib")
PENVMLIBDIR = os.path.abspath(f"{HEREDIR}/../../lib")

if 0:
    if os.path.exists(PENVMLIBFILE):
        sys.path.insert(0, PENVMLIBFILE)
        print("info: using penvmlib file")
    elif os.path.exists(PENVMLIBDIR):
        sys.path.insert(0, PENVMLIBDIR)
        print("info: using penvm source")
        PENVMLIBFILE = None
    else:
        print("fatal: cannot find PENVM library file or directory", file=sys.stderr)
        sys.exit(1)

    # from penvm.lib.misc import get_version_string


def penvmappzip(appname, appfilename, appdir, extralibs, modname, fnname):
    """Create a zipapp (application zip archive) application file.
    It will include the PENVM library, the application file, and
    optional extra, non-PENVM, library files.
    """
    try:
        print(
            f"building {appname} ...\n"
            f"    appfile:  {appfilename}\n"
            f"    module:   {modname}\n"
            f"    function: {fnname}\n",
            end="",
        )
        print("    creating archive ...")
        zipapp.create_archive(
            source=appdir,
            target=appfilename,
            interpreter="/usr/bin/env python3",
            main=f"{modname}:{fnname}",
            compressed=True,
        )

        with zipfile.ZipFile(appfilename, "a") as zipf:
            if extralibs:
                for name in extralibs:
                    print(f"    adding extralib ({name}) ...")
                    if not os.path.exists(name):
                        raise Exception(f"cannot find extralib ({name})")
                    zipfileadd(zipf, name, stripbase=True)
        os.chmod(appfilename, 0o755)
    except Exception as e:
        traceback.print_exc()
        raise Exception("failed to build penvm app")


def penvmlibzip(libfilename, penvmlibs, extralibs):
    """Create a zip archive for the PENVM library. Optionally, add
    extra, non-PENVM, library files.
    """
    try:
        print(f"building library {libfilename} ...")
        zipf = zipfile.ZipFile(libfilename, "w", compression=zipfile.ZIP_DEFLATED)
        if penvmlibs:
            for name in penvmlibs:
                print(f"    adding library ({name}) ...")
                if not os.path.exists(name):
                    raise Exception(f"cannot find penvm lib ({name})")
                zipfileadd(zipf, name, stripbase=True)

        if extralibs:
            for name in extralibs:
                print(f"    adding extralib ({name}) ...")
                if not os.path.exists(name):
                    raise Exception(f"cannot find extralib ({name})")
                zipfileadd(zipf, name, stripbase=True)
        zipf.close()
    except Exception as e:
        traceback.print_exc()
        raise Exception("failed to build penvm library")


def zipfileadd(zipf, path, stripbase=False):
    try:
        zipf2 = zipfile.ZipFile(path, "r")
    except:
        zipf2 = None

    if zipf2 != None:
        zipfilemerge(zipf, path)
    else:
        zipfiledirfile(zipf, path, stripbase)


def zipfiledirfile(zipf, path, stripbase=False):
    arcname = None
    if os.path.isfile(path):
        if stripbase:
            arcname = os.path.basename(path)
        zipf.write(path, arcname=arcname)
    else:
        for root, dirnames, filenames in os.walk(path):
            if stripbase:
                _root = root[len(path) + 1 :]
            else:
                _root = root

            # ensure dirs are added (for non-__init__.py support)
            allnames = dirnames + filenames
            for name in allnames:
                _path = os.path.join(root, name)
                arcname = os.path.join(_root, name)
                # print(f"{path=} {_path=} {arcname=}")
                zipf.write(_path, arcname=arcname)


def zipfilemerge(zipf, path):
    with zipfile.ZipFile(path, "r") as zipf2:
        for name in zipf2.namelist():
            zi = zipf2.open(name)
            zipf.writestr(name, zi.read())


def print_usage():
    progname = os.path.basename(sys.argv[0])
    print(
        f"""\
usage: {progname} app [-f <fnname>] [-l <extralibs>] [-m <modname>] [-o <appfile>] <appdir>
       {progname} lib [-l <extralibs>] -o <libfile> [<libdir> ...]
       {progname} -h|--help

Build single file PENVM application or library file.

Arguments:
<appdir>            Application directory with same named file containing "main()".
<libdir>            Library directory (containing content).
--dry               Dry run with making changes.
-f <fnname>         Function name. Default is "main".
-l <extralibs>      Addtitional files/dirs to add to the zip file.
                    Multiple uses supported.
-m <modname>        Module name.
-o <appfile>|<libfile>
                    Destination for single file application or library.
""",
        end="",
    )


class ArgOpts:
    pass


def main():
    try:
        argopts = ArgOpts()
        argopts.appdir = None
        argopts.buildfilename = None
        argopts.buildtype = None
        argopts.dryrun = False
        argopts.extralibs = []
        argopts.fnname = "main"
        argopts.libdirs = []
        argopts.modname = None

        args = sys.argv[1:]

        if args:
            arg = args.pop(0)
            if arg in ["-h", "--help"]:
                print_usage()
                sys.exit(0)
            elif arg in ["app", "lib"]:
                argopts.buildtype = arg
            else:
                raise Exception()
        else:
            raise Exception("bad/missing build type")

        if argopts.buildtype == "app":
            while args:
                arg = args.pop(0)
                if arg == "--dry":
                    argopts.dryrun = True
                elif arg == "-f" and args:
                    argopts.fnname = args.pop(0)
                elif arg == "-l" and args:
                    argopts.extralibs.append(args.pop(0))
                elif arg == "-m" and args:
                    argopts.modname = args.pop(0)
                elif arg == "-o" and args:
                    argopts.buildfilename = args.pop(0)
                elif not args:
                    argopts.appdir = arg
                else:
                    raise Exception()

            if None in [argopts.appdir]:
                raise Exception()
        elif argopts.buildtype == "lib":
            while args:
                arg = args.pop(0)
                if arg == "--dry":
                    argopts.dryrun = True
                elif arg == "-l" and args:
                    argopts.extralibs.append(args.pop(0))
                elif arg == "-o" and args:
                    argopts.buildfilename = args.pop(0)
                else:
                    argopts.libdirs = [arg] + args
                    del args[:]

            if None in [argopts.buildfilename]:
                raise Exception()
    except SystemExit:
        raise
    except Exception as e:
        traceback.print_exc()
        print("error: bad/missing argument", file=sys.stderr)
        sys.exit(1)

    try:
        if argopts.buildtype == "app":
            if argopts.buildfilename:
                buildfilename = argopts.buildfilename
            else:
                buildfilename = os.path.basename(argopts.appdir)
                if buildfilename.endswith(".py"):
                    buildfilename = buildfilename[:-3]
            argopts.buildfilename = buildfilename

        if argopts.dryrun:
            print(f"appdir:        {argopts.appdir}")
            print(f"buildtype:     {argopts.buildtype}")
            print(f"buildfilename: {argopts.buildfilename}")
            print(f"extralibs:     {argopts.extralibs}")
            print(f"fnname:        {argopts.fnname}")
            print(f"libdirs:       {argopts.libdirs}")
            print(f"modname:       {argopts.modname}")
            print(f"penvmlibdir:   {PENVMLIBDIR}")
            print(f"penvmlibfile:  {PENVMLIBFILE}")
        else:
            if argopts.buildtype == "app":
                appname = os.path.basename(buildfilename)
                penvmappzip(
                    appname,
                    argopts.buildfilename,
                    argopts.appdir,
                    argopts.extralibs,
                    argopts.modname or appname,
                    argopts.fnname,
                )
            elif argopts.buildtype == "lib":
                penvmlibzip(
                    argopts.buildfilename,
                    argopts.libdirs,
                    argopts.extralibs,
                )

    except Exception as e:
        print(f"{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
