#!/usr/bin/env bash
# pyenv root
export PYENV_ROOT="$HOME/.pyenv"

# Install pyenv if it's missing
if [[ ! -d $PYENV_ROOT ]]; then
    git clone git://github.com/yyuu/pyenv.git ${PYENV_ROOT}
    cd ${PYENV_ROOT}
    # Get the latest tagged version
    git checkout `git tag | tail -1`
    git clone https://github.com/yyuu/pyenv-virtualenv.git ${PYENV_ROOT}/plugins/pyenv-virtualenv
    cd plugins/pyenv-virtualenv
    git checkout `git tag | tail -1`
    git clone https://github.com/s1341/pyenv-alias.git ${PYENV_ROOT}/plugins/pyenv-alias

    # Add pyenv root to PATH
    # and initialize pyenv
    PATH="$PYENV_ROOT/bin:$PATH"
    # initialize pyenv
    eval "$(pyenv init -)"
    # initialize pyenv virtualenv
    eval "$(pyenv virtualenv-init -)"

    # Now load up (allthethings)
    VERSION_ALIAS="riak_3.4.3" pyenv install 3.4.3
    VERSION_ALIAS="riak_3.3.6" pyenv install 3.3.6
    VERSION_ALIAS="riak_2.7.10" pyenv install 2.7.10
    VERSION_ALIAS="riak_2.7.9" pyenv install 2.7.9
    VERSION_ALIAS="riak_2.6.9" pyenv install 2.6.9

    pyenv virtualenv riak_3.4.3 riak-py34
    pyenv virtualenv riak_3.3.6 riak-py33
    pyenv virtualenv riak_2.7.10 riak-py27
    pyenv virtualenv riak_2.7.9 riak-py279
    pyenv virtualenv riak_2.6.9 riak-py26
    pyenv versions
fi

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
