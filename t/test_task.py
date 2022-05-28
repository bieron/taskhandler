import os
import sys
import pytest
from logging import getLogger
from stat import S_IWUSR
from time import time

dirname = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(dirname, '../lib'))
from taskhandler import Scheduler, TaskInitError, Task, human_interval
from util import loadSchedule

log = getLogger()
log.setLevel(100)

def wrap_schedule(tasks, logdir, register=None):
    return Scheduler(tasks, register=register, logger=log, logdir=str(logdir))

# Mockery of Task IO communication with OS
def mock_task_run(self):
    self.start_time = time()
    self.exitcode = int(self.args[0] == 'false')
    self.finish()


def test_human_interval():
    cases = {
        '1h22m05.00s': 3600+1320+5,
        '57m00.00s': 3600 - 180,
        '4m15.00s': 240 + 15,
        '33.00s': 33,
    }
    for expect, given in cases.items():
        assert expect == human_interval(given)


def test_init_attrs(tmpdir):
    with pytest.raises(TaskInitError, match="Required attribute 'args' missing"):
        Task()

    with pytest.raises(TaskInitError, match=r"Unexpected attributes: \['inquisition', 'spanish'\]"):
        Task(name='test', args=['true'], spanish='', inquisition='')

    t = Task(name='ni ni~ni', args=['true'])
    assert t.logdir == '/tmp', 'default logdir value'
    assert t.deps == [], 'default deps value'
    assert t.stdout == '/tmp/ni-ni-ni.stdout', 'default log name built from logdir and escaped name'
    assert t.stderr == '/tmp/ni-ni-ni.stderr', 'default log name built from logdir and escaped name'

    t = Task(name='test', args=['true'], stdout='relative', stderr='/tmp/path')
    assert t.stdout == '/tmp/relative', 'prepend relative paths with logdir'
    assert t.stderr == '/tmp/path', 'do not modify absolute paths'

    tempdir_mode = os.stat(tmpdir).st_mode
    os.chmod(tmpdir, tempdir_mode ^ S_IWUSR)
    with pytest.raises(TaskInitError, match="Write error for stderr log"):
        Task(name='test', args=['true'], stderr=tmpdir +'/stderr')


def test_run(tmpdir):
    t = Task(name="test", args=['true'], logdir=str(tmpdir))
    assert not t.finish_time
    assert t.state == 'PENDING'
    assert t.exitcode is None
    t.run()
    assert isinstance(t.finish_time, float)
    assert t.state == 'FINISHED'
    assert t.exitcode == 0
    assert os.path.isfile(t.stdout), 'stdout file exists'
    assert os.path.isfile(t.stderr), 'stderr file exists'

    t = Task(name="test", args=['false'], logdir=str(tmpdir))
    assert not t.finish_time
    assert t.state == 'PENDING'
    assert t.exitcode is None
    t.run()
    assert isinstance(t.finish_time, float)
    assert t.state == 'FINISHED'
    assert t.exitcode == 1
    assert os.path.isfile(t.stdout), 'stdout file exists'
    assert os.path.isfile(t.stderr), 'stderr file exists'


def test_shell(tmpdir):
    t = Task(args='echo podupadłość | grep -o dupa', shell=True, logdir=str(tmpdir))
    t.run()
    assert t.exitcode == 0
    assert t.get_stdout() == "dupa\n"

    t = Task(name='count random piped bytes', shell=True,
        args='dd if=/dev/urandom bs=1K count=50 2>/dev/null | wc -c', logdir=str(tmpdir))
    t.run()
    assert t.get_stdout() == '51200\n'


def test_dependencies():
    with pytest.raises(TaskInitError, match="Final task cannot have dependencies"):
        Task(final=True, deps=['something'], args=['true'])

    assert Task(final=True, args=['true']).final


def test_schedule_tasks(tmpdir):
    tasks = [
        {'args': ['true'], 'name': 'one'},
        {'args': ['true'], 'name': 'two', 'deps': ['one']}
    ]
    s = wrap_schedule(tasks, tmpdir)
    s.execute(8)
    assert s.finished_order() == ['one', 'two']
    assert not s.failures()

    tasks = [
        {'args': ['true'], 'name': 'one'},
        {'args': ['false'], 'name': 'two', 'deps': ['one']}
    ]
    s = wrap_schedule(tasks, tmpdir)
    s.execute(8)
    assert s.finished_order() == ['one', 'two']
    assert s.failures() == ['two']

    tasks = [
        {'args': 'false', 'name': 'one'},
        {'args': 'true', 'name': 'two', 'deps': ['one']}
    ]
    s = wrap_schedule(tasks, tmpdir)
    s.execute(8)
    assert s.finished_order(), ['one']
    assert s.failures() == ['one']


def test_schedule_task_deps(tmpdir):
    tasks = [{'args': ['true'], 'name': 'name', 'deps': ['unsatisfied']}]
    with pytest.raises(RuntimeError, match='deps.+not satisfied'):
        wrap_schedule(tasks, tmpdir)


def test_deps_cycles(tmpdir):
    tasks = [{'args': 'true', 'name': 'cycle', 'deps': ['cycle']}]
    with pytest.raises(RuntimeError, match="Tasks have circular dependencies:"):
        wrap_schedule(tasks, tmpdir)

    tasks = [
        {'args': 'true', 'name': 'alfa', 'deps': ['omega']},
        {'args': 'true', 'name': 'omega', 'deps': ['alfa']},
    ]
    with pytest.raises(RuntimeError, match="Tasks have circular dependencies:"):
        wrap_schedule(tasks, tmpdir)

    tasks = [
        {'args': 'true', 'name': 'rock', 'deps': ['scissors']},
        {'args': 'true', 'name': 'scissors', 'deps': ['paper']},
        {'args': 'true', 'name': 'paper', 'deps': ['rock']},
    ]
    with pytest.raises(RuntimeError, match="Tasks have circular dependencies:"):
        wrap_schedule(tasks, tmpdir)


def test_final_task(tmpdir):
    tasks = [{'args': 'true', 'final': True}]
    s = wrap_schedule(tasks, tmpdir)
    assert not s.all_deps()

    tasks = [
        {'args': 'true', 'name': 'other'},
        {'args': 'true', 'final': True},
    ]
    s = wrap_schedule(tasks, tmpdir)
    assert s.all_deps() == {'other'}

    tasks = [
        {'args': 'true', 'deps': ['final']},
        {'args': 'true', 'name': 'final', 'final': True},
    ]
    with pytest.raises(RuntimeError, match="Final task cannot be a dependency"):
        wrap_schedule(tasks, tmpdir)

    tasks = [
        {'args': 'true', 'name': 'independent'},
        {'args': 'true', 'name': 'b', 'final': True},
        {'args': 'true', 'name': 'c', 'final': True},
    ]
    s = wrap_schedule(tasks, tmpdir)
    assert s.all_deps(), {'independent'}


def test_schedule_fail(tmpdir, mocker):
    mocker.patch('os.access', return_value=True)

    with open(f'{dirname}/failure.yml') as f:
        data = loadSchedule(f)
    s = wrap_schedule(data['tasks'], tmpdir, data['register'])
    s.execute(8)
    assert s.failures()


def test_schedule_script(tmpdir, mocker):
    mocker.patch('os.access', return_value=True)
    mocker.patch('taskhandler.Task.run', new=mock_task_run)

    with open(f'{dirname}/schedule.yml') as f:
        data = loadSchedule(f)
    s = wrap_schedule(data['tasks'], tmpdir, data['register'])
    s.execute(8)
    assert not s.failures()


def test_schedule_vars(tmpdir):
    with pytest.raises(RuntimeError, match="iterate_var must be alphanumeric"):
        wrap_schedule([{'iterate_var': '', 'args': 'echo {item}'}], tmpdir)

    register = {'array': 'echo -e "3\n1\n4"'}
    tasks = [
        {'args': 'echo element:{item}', 'iterate_var': 'array'},
        {
            'args': 'echo {item} again', 'iterate_var': 'array',
            'deps': ['echo element:{item}']
        },
        {'args': 'echo another task', 'deps': ['echo {dep} again|array']}
    ]
    s = wrap_schedule(tasks, tmpdir, register)

    expected_deps = {f'echo element:{i}' for i in "314"} \
        | {f'echo {i} again' for i in "314"}
    assert s.all_deps() == expected_deps

    expected_names = expected_deps | {'echo another task'}
    assert s.all_names() == expected_names
