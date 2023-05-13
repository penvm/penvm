#
# penvm/server/storage.py

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

import logging
from typing import Any, Callable, Dict, List, Tuple, Type, Union

logger = logging.getLogger(__name__)

from penvm.lib.base import BaseObject


class StorageManager(BaseObject):
    """Storage manager.

    NIY.
    """

    def __init__(self, machine: "Machine"):
        """Initialize.

        Args:
            machine: Owning machine.
        """
        try:
            super().__init__(None, logger)
            tlogger = self.logger.enter()

            self.machine = machine
            self.stores = {}
        finally:
            tlogger.exit()

    def create(self):
        pass

    def delete(
        self,
        storeid: str,
    ):
        """Delete by id.

        Args:
            storeid: Store id.
        """
        store = self.stores.pop(storeid)
        if store:
            # delete
            pass

    def get(self, storeid: str) -> Any:
        """Get object by id.

        Args:
            storeid: Store id.

        Returns:
            Store.
        """
        return self.store.get(storeid)

    def list(self) -> List[str]:
        """List store ids.

        Returns:
            Store ids.
        """
        return self.store.keys()

    def run(self):
        pass
