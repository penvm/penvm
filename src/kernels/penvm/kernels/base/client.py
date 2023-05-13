#
# penvm/kernels/base/client.py

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

"""Client-side base kernel support."""

import logging
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject

logger = logging.getLogger(__name__)


class KernelClient(BaseObject):
    """Base kernel client.

    Provides name/oid and logger setup.
    """

    name = "base"

    def __init__(
        self,
        session: "Session",
    ):
        """Initialize.

        Args:
            session: Owning session.
        """
        try:
            super().__init__(self.name, logger)
            tlogger = self.logger.enter()

            self.session = session
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def __repr__(self):
        return f"<KernelClient name={self.name}>"
