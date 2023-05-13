#
# penvm/client/config.py

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

"""Collection of configuration support, mostly used in
[penvm.client.world][].
"""

import copy
import itertools
import logging
from typing import Any, Callable, Dict, List, Tuple, Type, Union
import yaml

logger = logging.getLogger(__name__)


def expand_range(s: str) -> str:
    """Expand range settings in string.

    Uses Python style range: `<start>:<end>:<step>`.

    Deprecated.

    Args:
        s (str): Range string to expand.

    Returns:
        List of expanded ranges.
    """
    l = []
    # print(f"expand_range ({s=})")
    while s:
        if "[" in s:
            i = s.find("[")
            ii = s.find("]")
            l.append([s[:i]])
            start, end, *rest = s[i + 1 : ii].split(":")
            step = rest[0] if rest else "1"
            sz = min(map(len, [start, end]))
            start, end, step = int(start), int(end), int(step)
            l.append(srange(start, end, step, sz))
            s = s[ii + 1 :]
        else:
            l.append([s])
            s = ""

    return ["".join(t) for t in itertools.product(*l)]


def expand_range(s: str) -> List[str]:
    """Expand numbered, comma-separated ranges.

    Supported formats are:

    * &lt;start>-&lt;end>
    * &lt;single>

    Leading 0s set width of resulting numbers (e.g., 001 for a width
    of three digits).

    Args:
        s (str): Formatted string.

    Returns:
        List of expanded ranges.
    """
    l = []
    while s:
        if "[" in s:
            i = s.find("[")
            ii = s.find("]")
            l.append([s[:i]])
            ll = []
            segment = s[i + 1 : ii]
            chunks = segment.split(",")
            for chunk in chunks:
                chunk = chunk.strip()
                if "-" in chunk:
                    first, last = chunk.split("-")
                    sz = min(map(len, [first, last]))
                    first, last = int(first), int(last)
                    ll.extend(list(srange(first, last + 1, 1, sz)))
                else:
                    ll.append(chunk)
            l.append(ll)
            s = s[ii + 1 :]
        else:
            l.append([s])
            s = ""

    return ["".join(t) for t in itertools.product(*l)]


def listify_targets(s: str) -> List[str]:
    """Expand targets settings.

    Args:
        s: String of targets/target ranges.

    Returns:
        List of expanded ranges.
    """
    l = []
    for name in s.split():
        l.extend(expand_range(name))
    return l


def expand_value(v: str, d: dict) -> str:
    """Expand/resolve value using dictionary.

    Resolvable items are specified using the `str.format`.

    Args:
        v: Regular or format string.
        d: Dictionary used to resolve format string.

    Returns:
        Resolved "value".
    """
    if type(v) == str:
        return v.format(**d)
    return v


def srange(start: int, end: int, step: int, sz: str):
    """
    Args:
        start: Start value.
        end: End value.
        step: Step value.
        sz: Field width.

    Yields:
        (str): Range value.
    """
    fmt = "%%0.%dd" % (sz,)
    for v in range(start, end, step):
        yield fmt % v


class WorldConfig:
    """World configuation.

    Provide high-level methods for working with the world
    configuration file. This is normally used only by
    [penvm.client.world.World][].
    """

    def __init__(self):
        """Initialize."""
        self.groups = {}
        self.meta = {}
        self.networks = {}
        self.targets = {}
        self.templates = {}

    def __repr__(self):
        return f"<WorldConfig nnetworks={len(self.networks)}  ngroups={len(self.groups)} ntargets={len(self.targets)}>"

    def add_group(self, name: str, config: dict):
        """Add group.

        Args:
            name: Group name.
            config: Configuration.
        """
        self.groups[name] = GroupConfig(self, name, config)

    def add_meta(self, config: dict):
        """Add meta(data).

        Args:
            config: Configuration.
        """
        self.meta.update(config)

    def add_network(self, name: str, config: dict):
        """Add network.

        Args:
            name: Network name.
            config: Configuration.
        """
        self.networks[name] = NetworkConfig(self, name, config)

    def add_target(self, name: str, config: dict):
        """Add target.

        Args:
            name: Target name.
            config: Configuration.
        """
        self.targets[name] = TargetConfig(self, name, config)

    def add_template(self, name: str, config: dict):
        """Add template.

        Args:
            name: Template name.
            config: Configuration.
        """
        template = self.templates[name] = TemplateConfig(self, name, config)
        template.run()

    def clear(self):
        """Clear configuration."""
        self.networks = {}
        self.groups = {}
        self.targets = {}
        self.templates = {}
        self.meta = {}

    def load(self, path: str):
        """Load configuration from a file.

        Args:
            path: File path.
        """
        try:
            d = yaml.safe_load(open(path, "r"))
        except Exception as e:
            print(f"failed to load world file ({e})")
            raise

        self.load_config(d)

    def load_config(self, d: dict):
        """Load configuration from a dictionary.

        Args:
            d: Configuration.
        """
        self.add_meta(d.get("meta", {}))

        for name, config in d.get("templates", {}).items():
            self.add_template(name, config)

        for name, config in d.get("targets", {}).items():
            self.add_target(name, config)

        for name, config in d.get("groups", {}).items():
            self.add_group(name, config)

        for name, config in d.get("networks", {}).items():
            self.add_network(name, config)

    def get_group(self, name: str) -> "GroupConfig":
        """Get group.

        Args:
            name: Group name.

        Returns:
            Group confguration object.
        """
        return self.groups.get(name)

    def get_groups(self) -> List[str]:
        """Get group names.

        Returns:
            List of group names.
        """
        return self.groups.keys()

    def get_meta(self, name: str) -> Any:
        """Get meta(data).

        Args:
            name: Metadata item name.

        Returns:
            Metadata item value.
        """
        return self.meta.get(name)

    def get_network(self, name: str) -> "NetworkConfig":
        """Get network configuration.

        Args:
            name: Network name.

        Returns:
            Network configuration object.
        """
        return self.networks.get(name)

    def get_networks(self) -> List[str]:
        """Get network names.

        Returns:
            List of network names.
        """
        return self.networks.keys()

    def get_target(self, name: str) -> "TargetConfig":
        """Get target.

        Args:
            name (str): Target name.

        Returns:
            Target configuration object.
        """
        return self.targets.get(name)

    def get_targets(self) -> List[str]:
        """Get target names.

        Returns:
            List of target names.
        """
        return self.targets.keys()


class BaseConfig:
    """Base configuration.

    Provides standard methods: `add`, `get`, `update`."""

    def __init__(self, world: "WorldConfig", name: str, config: Union[dict, None] = None):
        """Initialize.

        Args:
            world: World configuration object.
            name: Configuration name.
            config: Configuration.
        """
        self.world = world
        self.config = {"name": name}
        if config:
            for k, v in config.items():
                self.add(k, v)

    def add(self, k: Any, v: Any):
        """Add key+value.

        Args:
            k: Key.
            v: Value.
        """
        if k == "targets":
            targets = listify_targets(v)
            v = " ".join(targets)
        self.config[k] = v

    def get(self, k: Any, default: Any = None) -> Any:
        """Get value for key.

        Args:
            k: Key.
            default: Default value if key is not found.

        Returns:
            Value for key.
        """
        return self.config.get(k, default)

    def update(self, config: dict):
        """Update configuration.

        Args:
            config: Configuration.
        """
        self.config.update(config)


class GroupConfig(BaseConfig):
    """Group configuration."""

    def __repr__(self):
        return f"<GroupConfig name={self.config['name']} config={self.config}>"

    def get_targets(self) -> List["TargetConfig"]:
        """Get target configuration objects.

        Returns:
            List of Target configuration objects.
        """
        targets = []
        for target in listify_targets(self.config.get("targets", "")):
            targets.extend(target)
        return targets


class NetworkConfig(BaseConfig):
    """Network configuration."""

    def __repr__(self):
        return f"<NetworkConfig name={self.config['name']} config={self.config}>"

    def get_targets(self) -> List["TargetConfig"]:
        """Get target configuration objects.

        Returns:
            List of Target configuration objects.
        """
        targets = []
        for target in listify_targets(self.config.get("targets", "")):
            if target.startswith("@"):
                group = self.world.groups.get(target[1:])
                if group:
                    targets.extend(group.get_targets())
            else:
                targets.append(target)
        return targets


class TargetConfig(BaseConfig):
    """Target configuration."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        super().__init__(*args, **kwargs)
        self.config["host"] = self.config.get("host", self.config.get("name"))

    def __repr__(self):
        return f"<TargetConfig name={self.config['name']} config={self.config}>"


class TemplateConfig(BaseConfig):
    """Template configuration."""

    def run(self):
        """Add target configurations based on template.

        Automatic substitution for `$target` is done for`host`,
        `machine-id` with target name.
        """
        targets = self.config.get("targets")
        for name in listify_targets(targets):
            config = copy.deepcopy(self.config)
            config["name"] = name
            if config.get("host") == "$target":
                config["host"] = name
            if config.get("machine-id") == "$target":
                config["machine-id"] = name
            config.pop("targets")
            self.world.add_target(name, config)
