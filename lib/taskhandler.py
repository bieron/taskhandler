from collections import defaultdict
from concurrent.futures import wait, FIRST_COMPLETED, ThreadPoolExecutor
from copy import deepcopy
import os
import re
import shlex
import subprocess
import time
import networkx

def human_interval(t):
    if t < 60:
        return f'{t:.2f}s'
    s = t % 60
    if t < 3600:
        return f'{t // 60}m{s:05.2f}s'
    m = t % 3600 // 60
    return f'{t // 3600}h{m:02d}m{s:05.2f}s'

class TaskInitError(Exception): pass

class Task:
    args, name, stdout, stderr, start_time, finish_time, exitcode = None, None, None, None, None, None, None
    final, shell = False, False
    stdin = '/dev/null'
    logdir = '/tmp'
    state = 'PENDING'
    deps = []

    def __init__(self, **kwa):
        try:
            self.args = kwa.pop('args')
        except KeyError:
            raise TaskInitError("Required attribute 'args' missing")

        allowed_attrs = ('name', 'logdir', 'stdin', 'stdout', 'stderr',
            'deps', 'final', 'shell')
        for a in allowed_attrs:
            if a not in kwa:
                continue
            setattr(self, a, kwa.pop(a))
        if kwa:
            raise TaskInitError(f"Unexpected attributes: {sorted(kwa)}")

        if not isinstance(self.args, list):
            self.args = shlex.split(self.args)

        if not self.name:
            self.name = ' '.join(self.args)

        if not os.access(self.stdin, os.R_OK):
            raise TaskInitError(f'{self.stdin} has to be readable')

        if self.deps and self.final:
            raise TaskInitError('Final task cannot have dependencies!')

        for a in ('stdout', 'stderr'):
            path = getattr(self, a)
            if not path:
                setattr(self, a, (self.logdir +'/'+ re.sub(r'\W+', '-', self.name)
                    ).replace('//', '/') +'.'+ a)
            elif not os.path.isabs(path):
                setattr(self, a, '/'.join([self.logdir, path]))

            try: # "ask for forgiveness, not for permission"
                with open(getattr(self, a), 'a'):
                    pass
            except PermissionError as e:
                raise TaskInitError(f'Write error for {a} log: {e}')


    def get_stderr(self):
        if self.exitcode is None:
            raise RuntimeError('Cannot get stderr of task that has not been run yet')
        with open(self.stderr) as f:
            return f.read()

    def get_stdout(self):
        if self.exitcode is None:
            raise RuntimeError('Cannot get output of task that has not been run yet')
        with open(self.stdout) as f:
            return f.read()


    def __str__(self):
        return f"<{self.__class__.__name__} '{self.name}' {' '.join(self.args)}>"


    def finish(self, exitcode=None):
        self.state = 'FINISHED'
        self.finish_time = time.time()
        if exitcode is not None:
            self.exitcode = exitcode


    def run(self):
        self.state = 'RUNNING'
        self.start_time = time.time()
        args = ' '.join(self.args) if self.shell else self.args
        with open(self.stdout, 'w') as _out, open(self.stderr, 'w') as _err, open(self.stdin) as _in:
            self.exitcode = subprocess.call(
                args, shell=self.shell, stdout=_out, stderr=_err, stdin=_in,
            )
        self.finish()


def _schedule_map_task(attr, cond=lambda t: True):
    """Helper curry for Scheduler class
    """
    def _map(self):
        return {getattr(t, attr) for t in self._tasks if cond(t)}
    return _map

class Scheduler:
    """Task runner/handler.
    Supports task dependency and parallelity.
    Registers tasks given as a list of dicts.
    """
    variables = {}

    def __init__(self, tasks, logger, register=None, logdir=None):
        """Initialize and register all given tasks.
        Validate them or throw error.
        """
        if logdir:
            for t in tasks:
                t['logdir'] = logdir

        self._has_run = False
        self._tasks = set()
        self.log = logger
        self._downstream = defaultdict(set)
        self._final_tasks = set()

        if register:
            for r in register:
                t = Task(args=shlex.split(register[r]), name='_register_' + r)
                t.run()
                if t.exitcode:
                    raise RuntimeError(f"Couldn't register {r} variable: {t.get_stderr()}")
                self.variables[r] = t.get_stdout().split()

        for task_def in tasks:
            try:
                for task in self._tasks_from_def(task_def):
                    self.log.debug('Registering %s', task)
                    self._tasks.add(task)
            except TaskInitError as e:
                raise RuntimeError(
                    f'Problem initializing task:\n{e}\nFull dict: {task_def}')

        if self._final_tasks:
            final_names = [t.name for t in self._final_tasks]
            final_deps = {n for n in self.all_names() if n not in final_names}
            for t in self._final_tasks:
                t.deps = final_deps

        for task in self._tasks:
            self.log.debug('Registering %s deps:\n%s', task, task.deps)
            for f in task.deps:
                self._downstream[f].add(task)

        self.validate()

    def validate(self):
        """Check if task names are unique.
        Check if every dep can be satisfied.
        Throw error if not.
        """
        names_count = defaultdict(int)
        for t in self._tasks:
            names_count[t.name] += 1
        repeated_names = [n for n in names_count if names_count[n] > 1]
        if repeated_names:
            raise RuntimeError('Following names are not unique:\n{}'.format(
                '\n\t'.join(repeated_names)))

        all_deps = {d for t in self._tasks for d in t.deps}
        missing_deps = [d for d in all_deps if d not in names_count]
        if missing_deps:
            raise RuntimeError(f'Following deps are not satisfied:\n{missing_deps}')

        final_names = [t.name for t in self._final_tasks]
        final_as_deps = [t for t in final_names if t in all_deps]
        if final_as_deps:
            raise RuntimeError(f"Final task cannot be a dependency:\n{final_as_deps}")

        graph = networkx.DiGraph()
        graph.add_edges_from([(d, t.name) for t in self._tasks for d in t.deps])
        cycles = tuple(networkx.simple_cycles(graph))
        if cycles:
            raise RuntimeError(f"Tasks have circular dependencies:\n{cycles}")

    def _tasks_from_def(self, task_def):
        """Unfold iterate_var tasks into one task per item
        """
        tasks = []

        deps = set()
        for dep in task_def.get('deps', []):
            pipe_count = dep.count('|')
            if pipe_count:
                if pipe_count > 1:
                    raise TaskInitError('Deps can iterate over at most one array!')
                d, var = dep.split('|')
                for v in self.variables[var]:
                    deps.add(d.format(dep=v))
                continue
            deps.add(dep)
        task_def['deps'] = deps

        if 'iterate_var' in task_def:
            if not (isinstance(task_def['iterate_var'], str) and task_def['iterate_var'].isalnum()):
                raise TaskInitError("iterate_var must be alphanumeric!")

            iterate_var = task_def.pop('iterate_var')
            items = self.variables[iterate_var]
            original_name = task_def.get('name', task_def['args'])
            if not items:
                self.log.warning(
                    '%s was supposed to be iterated by %s which is empty!',
                    original_name, iterate_var
                )


            for i in items:
                new_def = deepcopy(task_def)
                new_def['args'] = new_def['args'].format(item=i)
                new_def['name'] = original_name.format(item=i)
                if 'deps' in task_def:
                    new_def['deps'] = [d.format(item=i) for d in task_def['deps']]

                if new_def['name'] == original_name:
                    raise TaskInitError('Name with iterate_var must interpolate %(item)!')
                tasks.append(Task(**new_def))
        else:
            tasks.append(Task(**task_def))

        for t in tasks:
            if t.final:
                self._final_tasks.add(t)
        return tasks

    def execute(self, max_workers):
        """Wrapper for run(), creates executor on its own
        """
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            self.run(executor)

    def _finished_order(self):
        return sorted(
            [e for e in self._tasks if e.finish_time], key=lambda e: e.finish_time)

    def finished_order(self):
        """Return list of task names ordered by finish time
        """
        return [t.name for t in self._finished_order()]

    def failures(self):
        """Return list of names of failed tasks ordered by finish time
        """
        return [t.name for t in self._finished_order() if t.exitcode]

    def summary(self):
        """Return list of run info per task ordered by finish time
        """
        lines = [
            f'{t.exitcode:7d} {human_interval(t.finish_time-t.start_time)} {t.name}'
            for t in self._finished_order()
        ]
        if lines:
            return 'exitval time name\n' + '\n'.join(lines)
        return ''

    def all_deps(self):
        """Return set of all registered task dependencies
        """
        return {d for t in self._tasks for d in t.deps}

    all_names   = _schedule_map_task('name')
    all_stdouts = _schedule_map_task('stdout')
    all_stderrs = _schedule_map_task('stderr')
    failed_stderrs = _schedule_map_task('stderr', lambda t: t.exitcode)

    def run(self, executor):
        """Run tasks respecting the dependencies using an executor.
        """
        if self._has_run:
            raise RuntimeError("These tasks have been run already!")
        self._has_run = True

        count_deps = {t: len(t.deps) for t in self._tasks}
        base_tasks = {t for t, count in count_deps.items() if count == 0}

        future_to_task = {}
        for t in base_tasks:
            self.log.debug('Running %s', t)
            future_to_task[executor.submit(t.run)] = t
        not_done = set(future_to_task.keys())

        while not_done:
            done, not_done = wait(not_done, return_when=FIRST_COMPLETED)
            for future in done:
                task = future_to_task[future]
                if future.exception():
                    self.log.warning("Exception while running '%s':\n%s",
                        task.name, future.exception())
                    task.finish(-1)
                if task.exitcode:
                    self.log.warning("'%s' exitcode=%d stderr=%s",
                        task.name, task.exitcode, task.stderr)
                    dependants = [t.name for t in self._downstream[task.name]]
                    if dependants:
                        self.log.warning(
                            'Skipping tasks that depended on it: %s',
                            dependants
                        )
                    continue
                self.log.info('%s finished', task.name)
                for t in self._downstream[task.name]:
                    count_deps[t] -= 1
                    if count_deps[t] == 0:
                        self.log.debug('Running %s', t)
                        fut = executor.submit(t.run)
                        future_to_task[fut] = t
                        not_done.add(fut)
