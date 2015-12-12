#!/usr/bin/env bash

if [[ ! -d $PYENV_ROOT ]]
then
    export PYENV_ROOT="$HOME/.pyenv"
fi

declare -r PROJDIR="$PWD/.."
if [[ ! -s $PROJDIR/riak/__init__.py ]]
then
    echo "[ERROR] script must be run from the root of a clone of github.com/basho/riak-python-client" 1>&2
    exit 1
fi

if [[ ! -d $PROJDIR/riak_pb/src ]]
then
    (cd $PROJDIR && git submodule update --init)
fi

# Install pyenv if it's missing
if [[ ! -d $PYENV_ROOT ]]
then
    git clone https://github.com/yyuu/pyenv.git $PYENV_ROOT
else
    (cd $PYENV_ROOT && git fetch --all)
fi

(cd $PYENV_ROOT && git checkout $(git describe --tags $(git rev-list --tags --max-count=1)))

declare -r pyenv_virtualenv_dir="$PYENV_ROOT/plugins/pyenv-virtualenv"
if [[ ! -d $pyenv_virtualenv_dir ]]
then
    git clone https://github.com/yyuu/pyenv-virtualenv.git $pyenv_virtualenv_dir
else
    (cd $pyenv_virtualenv_dir && git fetch --all)
fi

(cd $pyenv_virtualenv_dir && git checkout $(git describe --tags $(git rev-list --tags --max-count=1)))

declare -r pyenv_alias_dir="$PYENV_ROOT/plugins/pyenv-alias"
if [[ ! -d $pyenv_alias_dir ]]
then
    git clone https://github.com/s1341/pyenv-alias.git $pyenv_alias_dir
else
    (cd $pyenv_alias_dir && git pull origin master)
fi

# Add pyenv root to PATH
# and initialize pyenv
if [[ $PATH != */.pyenv* ]]
then
    echo "[INFO] adding $PYENV_ROOT/bin to PATH"
    export PATH="$PYENV_ROOT/bin:$PATH"
fi

if [[ $(type -t pyenv) != 'function' ]]
then
    echo "[INFO] init pyenv"
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"
fi

# Now install (allthethings) versions for testing
for pyver in 2.7 3.3 3.4 3.5
do
    if ! pyenv versions | fgrep "riak_$pyver"
    then
        declare -i pymaj="${pyver%.*}"
        declare -i pymin="${pyver#*.}"
        pyver_latest="$(pyenv install --list | grep -E "^[[:space:]]+$pymaj\\.$pymin\\.[[:digit:]]\$" | tail -n1 | sed -e 's/[[:space:]]//g')"

        echo "[INFO] installing Python $pyver_latest"
        riak_pyver="riak_$pyver_latest"
        VERSION_ALIAS="$riak_pyver" pyenv install "$pyver_latest"
        pyenv virtualenv "$riak_pyver" "riak-py$pymaj$pymin"
    fi
done

(cd $PROJDIR && pyenv local riak-py35 riak-py34 riak-py33 riak-py27)

pyenv versions

if [[ $(python --version) == Python\ 3.* ]]
then
    pip install --upgrade pip
    for module in six tox python3-protobuf
    do
        if ! pip show --quiet $module
        then
            pip install --ignore-installed $module
            if ! pip show --quiet $module
            then
                echo "[ERROR] install of $module failed" 1>&2
                exit 1
            fi
        fi
    done
    pyenv rehash
else
    echo "[ERROR] expected Python 3 to be 'python' at this point" 1>&2
    exit 1
fi
