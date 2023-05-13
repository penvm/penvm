#
# penvm/lib/kvstore.py

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

import fnmatch
import logging
import os
import os.path
from typing import Any, Callable, Dict, List, Tuple, Type, Union

logger = logging.getLogger(__name__)

from penvm.lib.base import BaseObject
from penvm.lib.misc import State


class KVStore(BaseObject):
    """Base key+value store."""

    def __init__(self):
        """Initialize."""
        super().__init__(None, logger)

    def drop(
        self,
        k: Any,
    ):
        """Drop value for k.

        Args:
            k: Key.
        """
        self.pop(k)

    def exists(
        self,
        k: Any,
    ) -> bool:
        """Indicate if key exists or not.

        Args:
            k: Key.

        Returns:
            Status of k in store.
        """
        pass

    def get(
        self,
        k: Any,
        default: Any = None,
    ) -> Any:
        """Get value for k.

        Args:
            k: Any.
            default: Any.

        Returns:
            Value for k.
        """
        pass

    def keys(
        self,
        pattern: Union[str, None] = None,
    ) -> List[Any]:
        """List of keys in store.

        Args:
            pattern: Filter for keys to return.

        Returns:
            Keys matching pattern.
        """
        return []

    def pop(
        self,
        k: Any,
        default: Any = None,
    ) -> Any:
        """Pop value from store for k.

        Args:
            k: Key.

        Returns:
            Value for k or `default` otherwise.
        """
        pass

    def put(
        self,
        k: Any,
        v: Any,
    ):
        """Put value in store.

        Args:
            k: Key.
            v: Value.
        """
        pass

    def state(self) -> "State":
        """Get object state.

        Returns:
            `State` of store.
        """
        try:
            return State(
                "kvstore",
                self.oid,
                {
                    "names": list(self.keys()),
                },
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")


class MemoryKVStore(KVStore):
    """Memory based [penvm.lib.kvstore.KVStore][]."""

    def __init__(self):
        """Initialize."""
        super().__init__()
        self.oid = "memory"
        self.d = {}

    def exists(self, k: str) -> bool:
        """See [penvm.lib.kvstore.KVStore.exists][]."""
        return k in self.d

    def get(
        self,
        k: str,
        default: Any = None,
    ) -> Any:
        """See [penvm.lib.kvstore.KVStore.get][]."""
        return self.d.get(k, default)

    def keys(
        self,
        pattern: Union[str, None] = None,
    ) -> List[str]:
        """See See [penvm.lib.kvstore.KVStore.keys][]."""
        return fnmatch.filter(self.d.keys(), pattern)

    def pop(self, k: str) -> Any:
        """See [penvm.lib.kvstore.KVStore.pop][]."""
        self.d.pop(k)

    def put(self, k: str, v: Any):
        """See [penvm.lib.kvstore.KVStore.put][]."""
        self.d[k] = v


class FInfo:
    """File info for [penvm.lib.kvstore.FileKVStore][].

    Tracks value type (text, bytes).
    """

    def __init__(self):
        """Initialize."""
        self.type = None


class FileKVStore(KVStore):
    """File based [penvm.lib.kvstore.KVStore][]."""

    def __init__(
        self,
        dirpath: str,
    ):
        """Initialize.

        Args:
            dirpath: Directory path to store files.

        See See [penvm.lib.kvstore.KVStore.__init__][].
        """
        try:
            super().__init__()
            self.oid = "file"

            self.dirpath = dirpath

            if not self.dirpath.startswith("/tmp/"):
                raise Exception("FileKVStore must be under allowed directory")
            self.d = {}
            os.mkdir(self.dirpath, 0o700)
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            pass

    def __del__(self):
        if not self.dirpath.startswith("/tmp/"):
            return

        for name in self.d.keys():
            self.drop(name)

        try:
            os.rmdir(self.dirpath)
        except Exception as e:
            pass

    def get(
        self,
        k: str,
        default: Union[str, bytes, None] = None,
    ) -> Union[str, bytes, None]:
        """See [penvm.lib.kvstore.KVStore.get][]."""
        try:
            k = k.replace("/", "__")
            finfo = self.d.get(k)
            if not finfo:
                return None

            if finfo.type == "text":
                mode = "r+t"
            elif finfo.type == "binary":
                mode = "r+b"
            path = f"{self.dirpath}/{k}"
            if os.path.exists(path):
                v = open(path, mode).read()
        except Exception as e:
            self.logger.warning(f"put EXCEPTION ({e})")

        return v

    def get_path(
        self,
        k: str,
        default: Union[str, None] = None,
    ) -> str:
        """Return the path associated with the key.

        Args:
            k: Key.
            default: Default value.

        Returns:
            File path holding value.
        """
        try:
            path = f"{self.dirpath}/{k}"
            if not os.path.exists(path):
                return None
            return path
        except Exception as e:
            pass

    def keys(
        self,
        pattern=None,
    ) -> List[str]:
        """See [penvm.lib.kvstore.KVStore.keys][]."""
        try:
            names = list(self.d.keys())
            # names = os.listdir(f"{self.dirpath}")
        except Exception as e:
            names = []

        if pattern:
            names = fnmatch.filter(names, pattern)
        return names

    def pop(
        self,
        k: str,
    ) -> Union[str, bytes, None]:
        """See [penvm.lib.kvstore.KVStore.pop][]."""
        try:
            v = self.get(k)
            k = k.replace("/", "__")
            path = f"{self.dirpath}/{k}"
            os.remove(path)
            self.d.pop(k)
            return v
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")

    def put(
        self,
        k: str,
        v: Union[str, bytes],
    ):
        """See [penvm.lib.kvstore.KVStore.put][]."""
        try:
            k = k.replace("/", "__")
            path = f"{self.dirpath}/{k}"
            self.logger.debug(f"put {k=} {path=}")

            finfo = FInfo()
            if type(v) == str:
                finfo.type = "text"
                mode = "w+t"
            else:
                finfo.type = "binary"
                mode = "w+b"
            open(path, mode).write(v)
            self.d[k] = finfo
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
