#
# penvm/ext/workers/__init__.py

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

"""A Worker is a convenience tool to help parcel out the work to the
network of machines. The steps are:

1. partition the problem space
1. spawn the work to the network
1. collect the results
1. process the results
1. combine (consolidate) the results into one
1. return the final result

Workers may be tailored to suit specific needs. This is espectially
required for the paritition and combine steps, where both
implementations cannot be known in advance.
"""

import io
import logging
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from penvm.lib.base import BaseObject

logger = logging.getLogger(__name__)


class WorkerException(Exception):
    pass


class Worker(BaseObject):
    """Base, mostly abstract, class."""

    def __init__(
        self,
        network: "Network",
        nworkers: Union[int, None] = None,
        auto_collect: bool = True,
    ):
        """Initialize.

        Args:
            network: Network object.
            nworkers: (Maximum) Number of workers to run with.
            auto_collect: Collect response objects."""
        try:
            super().__init__(None, logger)
            tlogger = self.logger.enter()

            self.network = network
            self._nworkers = nworkers
            self.enabled = True
            self.auto_collect = auto_collect
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def call_one(
        self,
        idx: int,
        session: "Session",
        *args: List,
        **kwargs: Dict,
    ):
        """Call one.

        Args:
            idx: Call index.
            session: Session for call.
            *args: Positional arguments for call.
            **kwargs: Keyword arguments for call.

        Note:
            Override.
        """
        pass

    def clean(
        self,
        sessions: List["Session"],
    ):
        """Clean/release all sessions.

        Args:
            sessions: List of sessions to clean.

        Note:
            Do not override.
        """
        try:
            tlogger = self.logger.enter()

            for idx, session in enumerate(sessions):
                session.kernel.machine_drop_session()
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def collect(
        self,
        sessions: List["Session"],
    ) -> List[Any]:
        """Collect all results.

        Results are collected using
        [Worker.collect_one][penvm.ext.workers.Worker.collect_one].

        Args:
            sessions: List of sessions to collect for.

        Returns:
            Results from calls.

        Note:
            Do not override.
        """
        try:
            tlogger = self.logger.enter()

            results = []
            if self.auto_collect:
                for idx, session in enumerate(sessions):
                    results.append(self.collect_one(idx, session))
            return results
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def collect_one(
        self,
        idx: int,
        session: "Session",
    ) -> Any:
        """Collect one result.

        Args:
            idx: Collect index.
            session: Session to collect for.

        Returns:
            Collected result.

        Note:
            Override.
        """
        pass

    def combine(
        self,
        results: List[Any],
    ) -> Any:
        """Combine all results into one and return it.

        Args:
            results: Collected results.

        Returns:
            Combined results.

        Note:
            Override.
        """
        pass

    def disable(self):
        """Disable worker.

        Will use fallback if available.
        """
        self.enabled = False

    def enable(self):
        """Enable worker.

        Will not force use of fallback.
        """
        self.enabled = True

    @property
    def machines(self) -> List["Machine"]:
        """Return the machines available for use, subject to the
        number of workers.

        Returns:
            List of machines available for use.
        """
        machines = self.network.get_machines()
        return machines[: self.nworkers]

    @property
    def nworkers(self) -> int:
        """Return the number of workers to use.

        Returns:
            Number of workers to use.
        """
        if self._nworkers != None:
            return self._nworkers
        else:
            return len(self.network.get_machines())

    def partition(
        self,
        *args: List,
        **kwargs: Dict,
    ):
        """Partition problem space. Yield for each.

        Args:
            *args: Positional arguments for call.
            **kwargs: Keyword arguments for call.

        Yields:
            (Tuple[List, Dict]): `args` and `kwargs` unchanged.

        Note:
            Override.
        """
        yield (args, kwargs)

    def process(
        self,
        results: List[Any],
    ) -> List[Any]:
        """Process and return all results.

        Args:
            results: Collected results.

        Returns:
            Results unchanged.

        Note:
            Override as needed.
        """
        return results

    def run(
        self,
        *args: List,
        **kwargs: Dict,
    ) -> Any:
        """Run work across network and return result.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Final result.

        Note:
            Do not override.
        """
        # ensure network is set up
        try:
            tlogger = self.logger.enter()

            try:
                self.network.boot()
            except Exception as e:
                raise WorkerException("cannot boot network")

            sessions = self.spawn(*args, **kwargs)
            results = self.collect(sessions)
            results = self.process(results)
            result = self.combine(results)
            self.clean(sessions)
            return result
        except WorkerException as e:
            raise
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def spawn(
        self,
        *args: List,
        **kwargs: Dict,
    ) -> List["Session"]:
        """Spawn partitioned work across machines.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            List of sessions used.

        Note:
            Do not override.
        """
        try:
            tlogger = self.logger.enter()

            sessions = []
            machines = self.machines
            nmachines = len(machines)
            for idx, part in enumerate(self.partition(*args, **kwargs)):
                _args, _kwargs = part
                _args, _kwargs = self.transform(*_args, **_kwargs)
                machine = machines[idx % nmachines]
                session = machine.get_session()
                sessions.append(session)
                self.call_one(idx, session, *_args, **_kwargs)

            return sessions
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def test(
        self,
        *args: List,
        **kwargs: Dict,
    ):
        """Test run.

        No actual calls or processing are made.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Yields:
            (Tuple[List, Dict]): `args` and `kwargs` after partitioning and transformation.
        """
        try:
            tlogger = self.logger.enter()

            # see spawn for calling pattern as below
            for idx, part in enumerate(self.partition(*args, **kwargs)):
                _args, _kwargs = part
                _args, _kwargs = self.transform(*_args, **_kwargs)
                yield (_args, _kwargs)
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def transform(
        self,
        *args: List,
        **kwargs: Dict,
    ) -> Tuple[List, Dict]:
        """Transform args and kwargs after partitioning and prior to
        running.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            `args` and `kwargs` unchanged.
        """
        return (args, kwargs)


class MirrorMixin:
    """Mixin to provide mirror support for Worker.

    *All* workers are given the same parameters to use (see
    `partition`).
    """

    def combine(
        self,
        results: List[Any],
    ) -> Dict[str, Any]:
        """Return dictionary with results given under respective
        machine id keys.

        Args:
            results: Collected results.

        Returns:
            Combined results.
        """
        machines = self.machines
        d = {}
        for i, result in enumerate(results):
            d[machines[i].oid] = result
        return d

    def partition(
        self,
        *args: List,
        **kwargs: Dict,
    ):
        """Same args and kwargs (untouched) for each.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Yields:
            (Tuple[List, Dict]): `args` and `kwargs` unchanged.
        """
        for idx, machine in enumerate(self.machines):
            yield (args, kwargs)


class PartitionByFunctionWorker(Worker):
    """No partitioning is done, but the `args` is extended with
    `nworkers` and `index`. This is suitable for cases in which the
    receiving function does the partition based on `nworkers` and the
    `index`.
    """

    def partition(
        self,
        *args: List,
        **kwargs: Dict,
    ):
        nworkers = self.nworkers if self.nworkers != None else len(self.machines)
        for i in range(nworkers):
            yield (list(args) + [self.nworkers, i], kwargs)


class InputMixin:
    """Mixin to partition the problem space into an input-defined
    number of partitions.
    """

    def combine(
        self,
        results: List[Any],
    ) -> Dict[int, Any]:
        """Return dictionary with results given under respective
        partition index.

        Args:
            results: Collected results.

        Returns:
            Combined results.
        """
        d = {}
        for i, result in enumerate(results):
            d[i] = result
        return d

    def partition(
        self,
        *args: List,
        **kwargs: Dict,
    ) -> Tuple[List, Dict]:
        """Partition input stream.

        Keyword Args:
            `_` (dict): Input directives.
            `*` (Any): All others for the call.

        Keyword Args: Keyword Input Directives:
            `fieldsname` (str): Name of fields list referenced with
                `transform`.Defaults to "f".
            `file` (file): Input stream.
            `fsep` (str): Field separator. Defaults to whitespace.
            `data` (str|bytes): Input data.
            `rsep` (str): Record separator. Defaults to newline.
            `striprsep` (bool): Strip record separator. Defaults to
                `False`.
            `transform` (str): Python code to transform data fields
                (accessible via list named `fieldsname`). Defaults to
                identity.

        Yields:
            `args` partitioned according to directives.
        """
        idirectives = kwargs.get("_", {})
        fieldsname = idirectives.get("fieldsname", "f")
        file = idirectives.get("file")
        fsep = idirectives.get("fsep", None)
        data = idirectives.get("data")
        rsep = idirectives.get("rsep", "\n")
        striprsep = idirectives.get("striprsep", False)
        transformstr = idirectives.get("transform", None)

        if transformstr:
            try:
                transformcode = compile(transformstr, filename="transform", mode="exec")
            except Exception as e:
                raise Exception(f"transform string compilation failed ({e})")
        else:
            transformcode = None

        # TODO: handle text and bytes lsep and fsep defaults
        if data != None:
            if type(data) == str:
                f = io.StringIO(data)
            else:
                f = io.BytesIO(data)
                if type(rsep) != bytes:
                    rsep = rsep.encode()
        elif file != None:
            f = file
        else:
            raise Exception("no input stream/data provided")

        while True:
            # TODO: update to support non-newline line separator
            rec = f.readline()
            if rec == "":
                break
            if striprsep == True:
                rec = rec.rstrip(rsep)
            fields = rec.split(fsep)
            if transformcode:
                import os.path

                exec(transformcode, {fieldsname: fields, "os": os})
            _args = [arg.format(*fields) for arg in args]
            yield (_args, {})


class ExecWorker(Worker):
    """Worker to call executable."""

    def __init__(
        self,
        cwd: str,
        network: "Network",
        nworkers: Union[int, None] = None,
        auto_collect: bool = True,
        text: bool = True,
        env: dict = None,
    ):
        """Initialize.

        Args:
            cwd: Working directory to use.
            network: See [Worker.__init__][penvm.ext.workers.Worker.__init__].
            nworkers: See [Worker.__init__][penvm.ext.workers.Worker.__init__].
            auto_collect: See [Worker.__init__][penvm.ext.workers.Worker.__init__].
            text: Work in text mode.
            env: Environment variable settings.
        """
        super().__init__(network, nworkers, auto_collect)
        self.cwd = cwd
        self.text = text
        self.env = env

    def call_one(
        self,
        idx: int,
        session: "Session",
        *args: List,
        **kwargs: Dict,
    ):
        """Call one.

        Args:
            args (str): `[0]`: Command path.
            args (List[Any]): `[1:]`: Command arguments.

        See [Worker.call_one][penvm.ext.workers.Worker.call_one].
        """
        try:
            tlogger = self.logger.enter()

            session.kernel.run_exec(
                args[0],
                args[1:],
                capture_output=True,
                text=self.text,
                cwd=self.cwd,
                env=self.env,
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def collect_one(
        self,
        idx: int,
        session: "Session",
    ) -> Dict:
        """Collect one result.

        Extract information from the payload:

        * `status`: Response status.
        * `returncode`: Return code from execution.
        * `stderr`: Stderr output.
        * `stdout`: Stdout output.

        See [Worker.collect_one][penvm.ext.workers.Worker.collect_one].
        """
        try:
            tlogger = self.logger.enter()

            resp = session.get_response()
            payload = resp.payload
            return {
                "status": payload.get("-status"),
                "returncode": payload.get("returncode"),
                "stderr": payload.get("stderr", ""),
                "stdout": payload.get("stdout", ""),
            }

        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()


class OpWorker(Worker):
    """Worker to call an op.

    Note:
        `auto_collect` is `False` by default because of the
        nature of operations (many do *not* send responses.)
    """

    def __init__(
        self,
        kernelname: str,
        network: "Network",
        nworkers: Union[int, None] = None,
        auto_collect: bool = False,
    ):
        """Initialize.

        Args:
            kernelname: Name of kernel to use.
            network: See [Worker.__init__][penvm.ext.workers.Worker.__init__].
            nworkers: See [Worker.__init__][penvm.ext.workers.Worker.__init__].
            auto_collect: See [Worker.__init__][penvm.ext.workers.Worker.__init__].
        """
        super().__init__(network, nworkers, auto_collect)
        self.kernelname = kernelname

    def call_one(
        self,
        idx: int,
        session: "Session",
        *args: List,
        **kwargs: Dict,
    ) -> Any:
        """Call one.

        Args:
            args (str): `[0]`: Kernel client op method name.
            args (List[Any]): `[1:]`: Arguments for the method call.

        See [Worker.call_one][penvm.ext.workers.Worker.call_one].
        """
        try:
            tlogger = self.logger.enter()

            opname = args[0]
            fn = getattr(session.kernel, opname)
            fn(*args[1:], **kwargs)
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def collect_one(
        self,
        idx: int,
        session: "Session",
    ) -> Dict:
        """Collect one result.

        See [Worker.collect_one][penvm.ext.workers.Worker.collect_one].
        """
        try:
            tlogger = self.logger.enter()
            resp = session.get_response()
            payload = resp.payload
            return payload.dict()
        except Exception as e:
            tlogger.error(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()


class PythonCodeWorker(Worker):
    """Worker to call Python code."""

    def __init__(
        self,
        fallback: str,
        code: str,
        network: "Network",
        nworkers: Union[int, None] = None,
        auto_collect: bool = True,
    ):
        """Initialize.

        Args:
            fallback: Fallback function/method to call.
            code: Python code to run.
            network: See [Worker.__init__][penvm.ext.workers.Worker.__init__].
            nworkers: See [Worker.__init__][penvm.ext.workers.Worker.__init__].
            auto_collect: See [Worker.__init__][penvm.ext.workers.Worker.__init__].
        """
        super().__init__(network, nworkers, auto_collect)
        self.fallback = fallback
        self.code = code

    def call_one(
        self,
        idx: int,
        session: "Session",
        *args: List,
        **kwargs: Dict,
    ) -> Any:
        """Call one.

        Args:
            args (str): `[0]`: Function name to call in provided code.<br>
            args (List[Any]): `[1:]`: Arguments to call function with.
            kwargs: Keyword arguments to call function with.

        See [Worker.call_one][penvm.ext.workers.Worker.call_one].
        """
        try:
            tlogger = self.logger.enter()
            session.kernel.run_python_function(
                self.code,
                args[0],
                args[1:],
                kwargs,
            )
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def collect_one(
        self,
        idx: int,
        session: "Session",
    ) -> Any:
        """Collect one result.

        Extract information from the payload:

        * `-status`: Execution status.
        * `return-value`: Function return value.

        See [Worker.collect_one][penvm.ext.workers.Worker.collect_one].
        """
        try:
            tlogger = self.logger.enter()

            resp = session.get_response()
            payload = resp.payload
            if payload.get("-status") == "error":
                # TODO: what to do?
                pass
            return payload.get("return-value")
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def run(
        self,
        *args: List,
        **kwargs: Dict,
    ) -> Any:
        """Run fallback or worker run.

        See [Worker.run][penvm.ext.workers.Worker.run].
        """
        try:
            tlogger = self.logger.enter()

            if self.use_fallback(*args, **kwargs) or not self.enabled:
                return self.fallback(*args, **kwargs)
            else:
                return super().run(*args, **kwargs)
        except Exception as e:
            self.logger.warning(f"EXCEPTION ({e})")
        finally:
            tlogger.exit()

    def use_fallback(
        self,
        *args: List,
        **kwargs: Dict,
    ):
        """Call fallback by default, if available."""
        return self.fallback != None

    def wrun(
        self,
        fnname: str,
    ) -> Callable:
        """Wrap run call to provide `fnname` with `args` and `kwargs`.

        This allows for the function name to *not* have to be in the
        `args` and better mirror what may have been the original call
        signature.

        Args:
            fnname: Function name to call.

        Returns:
            Wrapped function call.
        """

        def _run(*args, **kwargs) -> Any:
            return self.run(fnname, *args, **kwargs)

        return _run


class InputExecWorker(InputMixin, ExecWorker):
    """Combined [penvm.ext.workers.InputMixin][] and
    [penvm.ext.workers.ExecWorker][]."""

    pass


class InputPythonCodeWorker(InputMixin, PythonCodeWorker):
    """Combined [penvm.ext.workers.InputMixin][] and
    [penvm.ext.workers.PythonCodeWorker][]."""

    pass


class MirrorOpWorker(MirrorMixin, OpWorker):
    """Combined [penvm.ext.workers.MirrorMixin][] and
    [penvm.ext.workers.OpWorker][]."""

    pass


class MirrorExecWorker(MirrorMixin, ExecWorker):
    """Combined [penvm.ext.workers.MirrorMixin][] and
    [penvm.ext.workers.ExecWorker][]."""

    pass


class MirrorPythonCodeWorker(MirrorMixin, PythonCodeWorker):
    """Combined [penvm.ext.workers.MirrorMixin][] and
    [penvm.ext.workers.PythonCodeWorker][]."""

    pass
