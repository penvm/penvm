#
# penvm/server/kernelmanager.py

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
All server-side kernels are registered with the manager and accessible
by name.
"""

import logging
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject
from penvm.lib.misc import State


logger = logging.getLogger(__name__)


class KernelManager(BaseObject):
    """Manage server-side kernels."""

    def __init__(self, machine: "Machine"):
        """Initialize.

        Args:
            machine: Machine.
        """
        super().__init__(None, logger)
        self.machine = machine
        self.kernels = {}

    def drop(self, name: str):
        """Drop kernel by name.

        Args:
            name: Kernel name.
        """
        self.kernels.pop(name)

    def get(self, name: str) -> "Kernel":
        """Get kernel by name.

        Args:
            name: Kernel name.
        """
        return self.kernels.get(name)

    def list(self) -> List[str]:
        """Get list of kernel names.

        Returns:
            Kernel names.
        """
        return list(self.kernels.keys())

    def set(self, name: str, kernel: "Kernel"):
        """Register kernel by name.

        Args:
            name: Kernel name.
            kernel: Kernel.
        """
        self.kernels[name] = kernel

    def start(self):
        """Start.

        No background threads."""
        pass

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` object.
        """
        try:
            return State(
                "kernel-manager",
                None,
                {
                    "kernel-ids": self.list(),
                    "nkernels": len(self.kernels),
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
