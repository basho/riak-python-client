#!/usr/bin/env bash

if [[ ! -d $PYENV_ROOT ]]
then
    export PYENV_ROOT="$HOME/.pyenv"
fi

TEST_ROOT=$PWD/..

# Install pyenv if it's missing
if [[ ! -d $PYENV_ROOT ]]
then
    git clone https://github.com/yyuu/pyenv.git $PYENV_ROOT
    (cd $PYENV_ROOT && git checkout $(git describe --tags $(git rev-list --tags --max-count=1)))
fi

# Upgrade it, if it's too old
if [[ -z $(pyenv install --list | grep 3.4.3) ]]
then
    (cd $PYENV_ROOT && git pull -u origin master && git checkout $(git describe --tags $(git rev-list --tags --max-count=1)))
fi

if [[ ! -d $PYENV_ROOT/plugins/pyenv-virtualenv ]]
then
    git clone https://github.com/yyuu/pyenv-virtualenv.git $PYENV_ROOT/plugins/pyenv-virtualenv
    (cd $PYENV_ROOT/plugins/pyenv-virtualenv && git checkout $(git describe --tags $(git rev-list --tags --max-count=1)))
fi

if [[ ! -d $PYENV_ROOT/plugins/pyenv-alias ]]
then
    git clone https://github.com/s1341/pyenv-alias.git $PYENV_ROOT/plugins/pyenv-alias
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
if [[ -z $(pyenv versions | grep riak_3.4.3) ]]
then
    VERSION_ALIAS="riak_3.4.3" pyenv install 3.4.3
    pyenv virtualenv riak_3.4.3 riak-py34
fi
if [[ -z $(pyenv versions | grep riak_3.3.6) ]]
then
    VERSION_ALIAS="riak_3.3.6" pyenv install 3.3.6
    pyenv virtualenv riak_3.3.6 riak-py33
fi
if [[ -z $(pyenv versions | grep riak_2.7.10) ]]
then
    VERSION_ALIAS="riak_2.7.10" pyenv install 2.7.10
    pyenv virtualenv riak_2.7.10 riak-py27
fi
if [[ -z $(pyenv versions | grep riak_2.7.9) ]]
then
    VERSION_ALIAS="riak_2.7.9" pyenv install 2.7.9
    pyenv virtualenv riak_2.7.9 riak-py279
fi
if [[ -z $(pyenv versions | grep riak_2.6.9) ]]
then
    VERSION_ALIAS="riak_2.6.9" pyenv install 2.6.9
    pyenv virtualenv riak_2.6.9 riak-py26
fi

(cd $TEST_ROOT && pyenv local riak-py34 riak-py33 riak-py27 riak-py279 riak-py26)

pyenv versions

# Now install tox
pip install --upgrade pip
if [[ -z $(pip show tox) ]]
then
    pip install -Iv tox
    if [[ -z $(pip show tox) ]]
    then
        echo "[ERROR] install of tox failed" 1>&2
        exit 1
    fi
    pyenv rehash
fi
