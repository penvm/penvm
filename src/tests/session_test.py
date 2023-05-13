#! /usr/bin/env python3
#
# tests/session_test.py

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

from threading import Thread
import time

from penvm.client.world import World


def main():
    w = World(filename="world.penvm")
    print(f"{w=}")
    t = w.get_target("localhost")
    print(f"{t=}")
    m = t.boot()

    print(f"{m=}")


if __name__ == "__main__":
    main()
