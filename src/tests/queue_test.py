#! /usr/bin/env python3
#
# tests/queue_test.py

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

from penvm.lib.mqueue import MessageQueue


def pop_wait(q):
    print("pop_wait ...")
    print(f"pop_wait {q.pop()=}")


def popwait_put_test():
    print("popwait_put_test")
    q = MessageQueue()
    Thread(target=pop_wait, args=(q,)).start()
    time.sleep(1)
    q.put(1)


def put_pop_test():
    print("put_pop_test")
    q = MessageQueue()
    q.put(1)
    print(f"{q.pop()=}")


def main():
    put_pop_test()
    print()
    popwait_put_test()


if __name__ == "__main__":
    main()
