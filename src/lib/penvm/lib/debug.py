#
# penvm/lib/debug.py

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

"""Debugging tools."""

from threading import Lock
from typing import Any, Callable, Dict, List, Tuple, Type, Union


class DataDumper:
    """Dumps data to a destination (e.g., file) with support for
    serialization."""

    def __init__(self, path: str):
        """Initialize.

        Args:
            path: File path to dump data.
        """
        self.path = path
        self.lock = Lock()
        self.f = open(path, "ab")

    def __del__(self):
        try:
            self.f.close()
        except:
            pass

    def writebytes(
        self,
        b: bytes,
        flush: bool = True,
    ):
        """Write bytes.

        Args:
            b: Bytes to write
            flush: Flush stream.
        """
        try:
            self.lock.acquire()
            self.f.write(b)
            if flush:
                self.f.flush()
        except Exception as e:
            print(f"EXCEPTION {e}")
        finally:
            self.lock.release()

    def writetext(self, t: str, flush: bool = False):
        """Write text.

        Args:
            t: String to write.
            flush: Flush to stream.
        """
        self.writebytes(t.decode("utf-8"), flush)
