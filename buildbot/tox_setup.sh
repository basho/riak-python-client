#!/usr/bin/env bash
# pyenv root
export PYENV_ROOT="$HOME/.pyenv"
TEST_ROOT=$PWD/..

# Install pyenv if it's missing
if [[ ! -d $PYENV_ROOT ]]; then
    git clone git://github.com/yyuu/pyenv.git ${PYENV_ROOT}
    cd ${PYENV_ROOT}
    # Get the latest tagged version
    git checkout `git tag | tail -1`
fi

# Upgrade it, if it's too old
if [[ -z $(pyenv install --list | grep 3.4.3) ]]; then
    cd ${PYENV_ROOT}
    git pull origin master
    git pull -u origin master
    # Get the latest tagged version
    git checkout `git tag | tail -1`
fi

if [[ ! -d ${PYENV_ROOT}/plugins/pyenv-virtualenv ]]; then
    git clone https://github.com/yyuu/pyenv-virtualenv.git ${PYENV_ROOT}/plugins/pyenv-virtualenv
    cd ${PYENV_ROOT}/plugins/pyenv-virtualenv
    git checkout `git tag | tail -1`
fi

if [[ ! -d ${PYENV_ROOT}/plugins/pyenv-alias ]]; then
    git clone https://github.com/s1341/pyenv-alias.git ${PYENV_ROOT}/plugins/pyenv-alias
fi

# Add pyenv root to PATH
# and initialize pyenv
PATH="$PYENV_ROOT/bin:$PATH"
# initialize pyenv
eval "$(pyenv init -)"
# initialize pyenv virtualenv
eval "$(pyenv virtualenv-init -)"

# Now install (allthethings) versions for testing
if [[ -z $(pyenv versions | grep riak_3.4.3) ]]; then
    VERSION_ALIAS="riak_3.4.3" pyenv install 3.4.3
    pyenv virtualenv riak_3.4.3 riak-py34
fi
if [[ -z $(pyenv versions | grep riak_3.3.6) ]]; then
    VERSION_ALIAS="riak_3.3.6" pyenv install 3.3.6
    pyenv virtualenv riak_3.3.6 riak-py33
fi
if [[ -z $(pyenv versions | grep riak_2.7.10) ]]; then
    VERSION_ALIAS="riak_2.7.10" pyenv install 2.7.10
    pyenv virtualenv riak_2.7.10 riak-py27
fi
if [[ -z $(pyenv versions | grep riak_2.7.9) ]]; then
    VERSION_ALIAS="riak_2.7.9" pyenv install 2.7.9
    pyenv virtualenv riak_2.7.9 riak-py279
fi
if [[ -z $(pyenv versions | grep riak_2.6.9) ]]; then
    VERSION_ALIAS="riak_2.6.9" pyenv install 2.6.9
    pyenv virtualenv riak_2.6.9 riak-py26
fi
pyenv global riak-py34 riak-py33 riak-py27 riak-py279 riak-py26
pyenv versions

# Now install tox
pip install --upgrade pip
if [ -z "`pip show tox`" ]; then
    pip install -Iv tox
    if [ -z "`pip show tox`" ]; then
        echo "ERROR: Install of tox failed"
        exit 1
    fi
    pyenv rehash
fi
