#
# penvm/client/world.py

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

"""
The World object is foundational for defining the set of networks
and machines available to an application. It can be set up in various
ways: configuration, configuration file, network string, environment
variables. All of these provide flexibility for many use cases.

An application is associated with a world configuration explicitly
(specific configuration or named configuration file) or implicitly
(environment variable or related .penvm file).

Each running machine instance is defined by a `MachineConnectionSpec`
which can be represented as a Machine Connection string. A machine
connection consists of:

* machine id (unique)
* ssl profile (name)
* hostname/address
* port

A network string consists of one or more machine connection strings.
"""

import concurrent.futures
import os
import os.path
import logging
import subprocess
import sys
import threading
import time
import traceback
from typing import Any, Callable, Dict, List, Tuple, Type, Union


from penvm.client.config import WorldConfig
from penvm.client.machine import Machine
from penvm.lib.base import BaseObject
from penvm.lib.misc import MachineConnectionSpec, get_uuid, get_version_string

logger = logging.getLogger(__name__)


class World(BaseObject):
    """World of networks and hosts.

    Configuration is taken from:

    1. `config` keyword argument
    1. `networkstr` keyword argument
    1. `filename` keyword argument
    1. `PENVM_AUTO_NETWORK` environment variable
    1. `PENVM_WORLD_FILENAME` environment variable
    1. `<binname>.penvm` found in directory containing executable
    """

    def __init__(self, **kwargs):
        """Initialize.

        Keyword Args:
            config (dict): Dictionary configuration.
            filename (str): Configuration filename.
            networkstr (str): Network string of machine connection
                strings.
        """
        try:
            super().__init__(kwargs.get("filename"), logger)
            tlogger = self.logger.enter()

            self.config = WorldConfig()
            self.networks = {}
            self.filename = None

            # print(f"{kwargs=}")
            if kwargs.get("config") != None:
                # print("World config")
                self.config.load_config(kwargs["config"])
            elif kwargs.get("networkstr") != None:
                # print("World networkstr")
                self.load_auto_network("default", kwargs["networkstr"])
            elif kwargs.get("filename") != None:
                # print("World filename")
                self.filename = kwargs["filename"]
                self.config.load(self.filename)
            elif os.environ.get("PENVM_AUTO_NETWORK") != None:
                # print("World PENVM_AUTO_NETWORK")
                self.load_auto_network("default", os.environ.get("PENVM_AUTO_NETWORK"))
            elif os.environ.get("PENVM_WORLD_FILENAME") != None:
                # print("World PENVM_WORLD_FILENAME")
                self.filename = os.environ.get("PENVM_WORLD_FILENAME")
                self.config.load(self.filename)
            else:
                # print("World .penvm")
                filename, ext = os.path.splitext(os.path.basename(sys.argv[0]))
                filename = f"{os.path.dirname(sys.argv[0])}/{filename}.penvm"
                # TODO: abspath(filename)?
                if filename and os.path.exists(filename):
                    self.config.load(filename)
                    self.filename = filename

            if self.filename:
                # TODO: is this the right place for this or way to do this?
                self.oid = self.filename
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def get_group_names(self):
        return self.config.get_groups()

    def get_meta(self, name: str) -> Any:
        """Get meta(data) value.

        Returns:
            Value.
        """
        return self.config.get_meta(name)

    def get_network(self, network_name: str = "default") -> "Network":
        """Get network object by name.

        Args:
            network_name: Network name.

        Returns:
            Network.
        """
        network = self.networks.get(network_name)
        if not network:
            netconfig = self.config.get_network(network_name)
            if not netconfig:
                return None
            targets = []
            for target_name in netconfig.get_targets():
                target = self.get_target(target_name)
                if target:
                    targets.append(target)
            network = Network(network_name, targets, netconfig)
            self.networks[network_name] = network
        return network

    def get_networks(self) -> List["Network"]:
        """Get networks.

        Return:
            List of networks.
        """
        return list(self.networks.values())

    def get_network_names(self) -> List[str]:
        """Get network names.

        Return:
            Network names.
        """
        return self.config.get_networks()

    def get_target(self, target_name: str) -> "Target":
        """Get target by name.

        Args:
            target_name: Target name.

        Returns:
            Target.
        """
        return Target(target_name, self.config.get_target(target_name))

    def get_target_names(self) -> List[str]:
        """Get target names.

        Returns:
            List of target names.
        """
        return self.config.get_targets()

    def load_auto_network(self, name: str, networkstr: str):
        """Load auto-scheme (predefined, running) network

        The machine connection strings are used to generate a
        configuration for targets (by machid) and a network.

        Args:
            name: Network name.
            networkstr: Network string.
        """
        try:
            targets = []
            for machconnstr in networkstr.split():
                mcs = MachineConnectionSpec(machconnstr=machconnstr)
                target_config = {
                    "machine-id": mcs.machid,
                    "scheme": "auto",
                    "host": mcs.host,
                    "port": mcs.port,
                    "ssl-profile": mcs.sslprofile,
                }
                targets.append(mcs.machid)
                self.config.add_target(mcs.machid, target_config)
            net_config = {
                "targets": " ".join(targets),
            }
            self.config.add_network(name, net_config)
        except Exception as e:
            traceback.print_exc()

    def shutdown(self):
        """Shut down network.

        Results in machine instances being shut down.
        """
        for network in self.get_networks():
            try:
                network.shutdown()
            except Exception as e:
                pass


class Network(BaseObject):
    """Network.

    Encapsulates references to targets and booted machine instances.
    """

    def __init__(self, name: str, targets: List["Target"], config: "NetworkConfig"):
        """Initialize.

        Args:
            name: Network name.
            targets: List of network targets.
            config: Network configuration.
        """
        try:
            super().__init__(name, logger)
            tlogger = self.logger.enter()

            self.name = name
            self.targets = targets
            self.config = config

            # augment from net config
            overrides = self.config.get("overrides")
            if overrides:
                for target in self.targets:
                    target.update(overrides)

            self.machines = {}
        finally:
            tlogger.exit()

    def __len__(self):
        return len(self.machines)

    def __str__(self):
        return " ".join(self.get_machconnstrs())

    def boot(
        self,
        concurrency: Union[int, None] = None,
    ):
        """Boot (machines in) network.

        Boots machine instances using target information.

        Boot concurrency ("boot-concurrency" configuration
        setting) is: 0: number of targets, 1: serial, > 1
        concurrent. Final minimum is 1. Default is 1.

        Args:
            concurrency: Boot concurrency. Overrides configuration if
                provided.
        """
        # TODO: run threaded to improve startup/setup times and smooth
        # out wait/delay/connect times
        try:
            tlogger = self.logger.enter()

            if self.machines:
                # already booted
                return

            if concurrency == None:
                concurrency = self.config.get("boot-concurrency", 1)
                if concurrency == 0:
                    concurrency = len(self.targets)
            concurrency = max(1, concurrency)

            # print(f"{concurrency=}")
            if concurrency == 1:
                # sequential boot
                for target in self.targets:
                    self.machines[target.name] = target.boot()
            else:
                # TODO: figure out why terminal gets messed up
                # concurrent boot
                def _boot(target):
                    machine = target.boot()
                    lock.acquire()
                    self.machines[target.name] = machine
                    lock.release()
                    self.logger.debug(f"booted target ({target.name}) machine ({machine.oid})")
                    # print(f"{machine=}")
                    return machine

                lock = threading.Lock()

                with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                    # with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    results = {executor.submit(_boot, target) for target in self.targets}
                    for future in concurrent.futures.as_completed(results):
                        # print(f"{future.result()=}")
                        pass

            # TODO: launch penvm-server
            # TODO: capture host/addr and port
            # TODO: set up Machines for each
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def get_machconnspecs(self) -> List["MachineConnectionSpec"]:
        """Get `MachineConnectionSpec`s for machines.

        Returns:
            List of `MachineConnectionSpec`s.
        """
        return [m.get_machconnspec() for m in self.machines.values()]

    def get_machconnstrs(self) -> List[str]:
        """Get machine connection strings for machines.

        Returns:
            List of machine connection strings.
        """
        return [m.get_machconnstr() for m in self.machines.values()]

    def get_machine(self, target_name: str) -> "Machine":
        """Get machine.

        Args:
            target_name (str): Target name.

        Returns:
            Machine for named target.
        """
        return self.machines.get(target_name)

    def get_machines(self) -> List["Machine"]:
        """Get list of machines for network.

        Returns:
            List of `Machine`s.
        """
        return list(self.machines.values())

    def get_machine_names(self) -> List[str]:
        """Get list of machine names for network.

        Returns:
            List of machine names.
        """
        return list(self.machines.keys())

    def get_targets(self) -> List["Target"]:
        """Get targets for network.

        Returns:
            List of Targets.
        """
        return self.targets

    def get_target_names(self) -> List[str]:
        """Get target names for network.

        Returns:
            List of target names.
        """
        return [target.name for target in self.targets]

    def is_alive(self) -> bool:
        """Indicate if network is alive, or not.

        Returns:
            `True` or `False`.
        """
        return self.machines and True or False

    def shutdown(self):
        """Shut down network.

        Request for machines to be shutdown. Clear local tracking
        of machines by network.
        """
        self.logger.debug("shutdown")
        for k, machine in self.machines.items()[:]:
            s = machine.get_session()
            s.kernel.machine_shutdown()
            self.machines.pop(k, None)


class Target(BaseObject):
    """Target."""

    def __init__(self, name: str, config: Union["TargetConfiguration", None] = None):
        """Initialize.

        Args:
            name: Target name.
            config: Target configuration.
        """
        try:
            super().__init__(name, logger)
            tlogger = self.logger.enter()

            self.name = name
            self.config = config or {}
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def __repr__(self):
        # url = self.config.get("url") if self.config else ""
        scheme = self.config.get("scheme")
        host = self.config.get("host")
        port = self.config.get("port")
        user = self.config.get("user")
        return f"<Target name={self.name} scheme={scheme} host={host} port={port} user={user}>"

    def boot(self):
        """Boot machine on target.

        Spawns `penvm-server` processes for each target.
        """
        try:
            tlogger = self.logger.enter()

            scheme = self.config.get("scheme")
            host = self.config.get("host")
            port = self.config.get("port")
            user = self.config.get("user")
            sslprofile = self.config.get("ssl-profile")

            # assign unique machine id
            machineid = self.config.get("machine-id", get_uuid())
            serverpath = f".penvm/releases/{get_version_string()}/penvm-server"

            t0 = time.time()
            if scheme == "auto":
                pass
            else:
                if scheme == "ssh":
                    spargs = [
                        "ssh",
                        host,
                        # "-T",
                        "-tt",
                        "-x",
                    ]
                    if user:
                        spargs.extend(["-l", user])
                    if port:
                        spargs.extend(["-p", port])

                elif scheme == "local":
                    serverpath = os.path.expanduser(f"~/{serverpath}")
                    spargs = []
                else:
                    return

                spargs.extend(
                    [
                        "python3",
                        serverpath,
                        "--machineid",
                        machineid,
                        "--announce",
                        "--background",
                        "--firstwait",
                        # "15",
                        "5",
                    ]
                )

                if sslprofile:
                    spargs.extend(["--ssl-profile", sslprofile])

                spargs.append(host)

                tlogger.debug("booting server ...")
                tlogger.debug(f"boot args={spargs}")
                # print(f"boot args={spargs}")
                cp = subprocess.run(
                    spargs,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    cwd="/",
                    timeout=5,
                )

                if cp.returncode != 0:
                    tlogger.debug(f"boot failed returncode={cp.returncode} stderr={cp.stderr}")

                t1 = time.time()
                # tlogger.debug(f"machine booted elapsed ({t1-t0})")

            # set up Machine info
            try:
                # print(f"{scheme=}")
                # print(f"{self.config=}")
                if scheme == "auto":
                    mcs = MachineConnectionSpec(config=self.config)
                else:
                    # print(f"{cp.stdout=}")
                    # print(f"{cp.stderr=}")
                    if not cp.stdout.startswith("announce::"):
                        raise Exception(
                            f"could not get penvm-server information ({cp.stderr}). (is penvm-server deployed?)"
                        )

                    # trim announcement
                    s = cp.stdout[10:]
                    mcs = MachineConnectionSpec(machconnstr=s)

                for _ in range(10):
                    # TODO: be smart about `sleep`/waiting for remote readiness
                    if scheme != "auto":
                        time.sleep(0.05)
                    tlogger.debug(f"machine announcement received machineconnectionstr={mcs}")

                    tlogger.debug("creating Machine ...")
                    mach = Machine(
                        mcs.host,
                        mcs.port,
                        mcs.sslprofile,
                        mcs.machid,
                    )
                    if mach:
                        mach.start()
                        break

                t1 = time.time()
                tlogger.debug(f"machine created elapsed ({t1-t0})")

                if mach:
                    # set up sessions
                    sessions = self.config.get("sessions")
                    if sessions:
                        sess = mach.get_session("_")
                        for sessionid, session in sessions.items():
                            kernelname = session.get("kernel")
                            maxthreads = session.get("max-threads")

                            if kernelname != None:
                                tlogger.debug(
                                    f"session ({sessionid}) setting kernel ({kernelname})"
                                )
                                sess.kernel.session_use_kernel(kernelname, sessionid=sessionid)
                            if maxthreads != None:
                                tlogger.debug(
                                    f"session ({sessionid}) setting max threads ({maxthreads})"
                                )
                                sess.kernel.session_set_max_threads(
                                    maxthreads, sessionid=sessionid
                                )

                    # load assets
                    pass

                return mach
            except Exception as e:
                raise
        finally:
            tlogger.exit()

    def update(self, config: dict):
        """Update configuration.

        Args:
            config: Configuration.
        """
        self.config.update(config)
