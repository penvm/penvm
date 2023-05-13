#! /usr/bin/env python3
#
# tests/semaphore_test.py

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

from penvm.lib.semaphore import AdjustableSemaphore


def sem_acquire(sem):
    print("sem_acquire ...")
    sem.acquire()
    print("sem_acquired ...")
    print(f"{sem.count()=} {sem.max()=}")


def sem_2acquire_release_test():
    print("sem_2acquire_release_test")
    sem = AdjustableSemaphore()
    print(f"{sem.count()=} {sem.max()=}")
    sem.acquire()
    Thread(target=sem_acquire, args=(sem,)).start()
    time.sleep(1)
    print(f"{sem.count()=} {sem.max()=}")
    print("releasing ...")
    sem.release()
    print(f"{sem.count()=} {sem.max()=}")
    sem.release()
    print(f"{sem.count()=} {sem.max()=}")


def sem_acquire_release_test():
    print("sem_acquire_release_test")
    sem = AdjustableSemaphore(1)
    print(f"{sem.count()=} {sem.max()=}")
    sem.acquire()
    print(f"{sem.count()=} {sem.max()=}")
    sem.release()
    print(f"{sem.count()=} {sem.max()=}")


def main():
    sem_acquire_release_test()
    print()
    sem_2acquire_release_test()


if __name__ == "__main__":
    main()
