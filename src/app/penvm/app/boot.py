#
# penvm/app/boot.py

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

"""Boot support for PENVM world/network.
"""

import os
import os.path
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.client.world import World
from penvm.lib.misc import MachineConnectionSpec, get_version_string


class Booter:
    """Provides PENVM network boot support from configuration file."""

    def __init__(
        self,
        configfilename: str,
        networkname: str,
        release: str,
    ):
        """Initialize.

        Args:
            configfilename: Configuration file name.
            networkname: Network name.
            release: PENVM release.
        """
        self.configfilename = configfilename
        self.networkname = networkname
        self.release = release

        self.world = None
        self.network = None
        self.env = None

    def boot(self):
        """Boot."""
        try:
            self.world = World(filename=self.configfilename)
            self.network = self.world.get_network(self.networkname)
            # print(f"{self.networkname=}")
            if self.network == None or self.network.get_targets() in [None, []]:
                raise Exception(f"network ({self.networkname}) not found")
            self.network.boot()

            machines = self.network.get_machines()
            if machines in [None, []]:
                raise Exception("no machines")
            if None in machines:
                raise Exception("not all machines found")

            self.release = self.release or self.world.get_meta("release") or get_version_string()
            if self.release == None:
                raise Exception(f"could not determine release value")

            releasepath = os.path.expanduser(f"~/.penvm/releases/{self.release}")
            releaselibs = ":".join([f"{releasepath}/{name}" for name in ["penvmlib-client"]])

            # patch env
            self.env = os.environ.copy()
            PENVM_AUTO_NETWORK = str(self.network)
            PYTHONPATH = self.env.get("PYTHONPATH")
            if PYTHONPATH != None:
                PYTHONPATH = f"{releaselibs}:{PYTHONPATH}"
            else:
                PYTHONPATH = releaselibs

            self.env["PENVM_AUTO_NETWORK"] = PENVM_AUTO_NETWORK
            self.env["PENVM_RELEASE"] = self.release
            self.env["PYTHONPATH"] = PYTHONPATH
        except Exception as e:
            raise
        finally:
            pass

    def shell(
        self,
        path: str,
    ):
        """Start shell with environment."""
        try:
            try:
                sp = subprocess.Popen(
                    [path],
                    pass_fds=[0, 1, 2],
                    env=self.env,
                )
            except Exception as e:
                raise Exception(f"failed to start shell ({path})")

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
        except Exception as e:
            raise

    def wait(self, secs: int = 1000000):
        """Wait indefinitely."""
        time.sleep(secs)
