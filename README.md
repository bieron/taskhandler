# Installation

NAME
    schedule - task handler with support for parallelity and task dependencies


DESCRIPTION
    schedule runs tasks in order specified in a schedule file.
    Schedule can be given as a file or read from STDIN.

    It executes tasks locally, saving their stdouts and stderrs to --logdir.

    In case of task failure, none of its dependencies, direct or indirect, will be run,
    and the schedule will return nonzero exit code.
    However, tasks not depending on failed ones will still be executed.


INPUT FORMAT
    Accepted formats are YAML and JSON. Examples will use YAML given its superior readability.
    Schedule must be a dictionary with an obligatory key 'tasks', which must be a list.
    Each task is a dictionary.

```
tasks:
  # command to run
  - args: bin/mysql_migrate --offline -n 1
  # identifier of a task, used to create logs and define it as a dependency
    name: offline node 1

  - args: echo 1 is offline > /tmp/some-file
  # this task will run after the previous one, because it has it in its deps
    deps: ['offline node 1']
  # thanks to this option stream redirection will work
    shell: True
```

    Often we need to run a task couple of times with a different option. Usually these tasks are independent from each other.
    There is a way of creating a task template with a list variable,
    and schedule will create a task for each element of that list,
    interpolating the element into its args, name, and deps. The placeholder for the element is '{item}'.

```
tasks:
  - args: 'echo hello world{item}'
    iterate_var: numbers

register:
  numbers: seq 1 10
```

    The schedule above works as follows:
    Firstly, each register command is executed and each output line becomes an element of `numbers` list.
    For each element, a separate task is created, with {item} placeholder replaced with the element.
    This happens for all task templates with matching `iterate_var`.


GOOD PRACTICES

1. `name` is an optional argument - if missing, a default one will be build from `args`.
However please define an explicit name if you plan to use this task as a dependency.
It increases readability and can help you avoid incorrect order of task execution.


# run tasks defined in given schedule
    schedule -i schedule.yml

# can also read from standard input
    schedule < schedule.json

1. Install pyenv

```sh
    git clone https://github.com/pyenv/pyenv.git ~/.pyenv
    echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bash_profile
    echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bash_profile
    exec $SHELL
```

2. Install required python version
```sh
    pyenv install 3.6.4
```

3. Install required pyenv modules
```sh
    cd taskhandler/
    pip install -r requirements.txt
```
