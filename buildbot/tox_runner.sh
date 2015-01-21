#!/usr/bin/env bash
# pyenv root
export PYENV_ROOT="$HOME/.pyenv"

# Add pyenv root to PATH
# and initialize pyenv
PATH="$PYENV_ROOT/bin:$PATH"
# initialize pyenv
eval "$(pyenv init -)"
# initialize pyenv virtualenv
eval "$(pyenv virtualenv-init -)"

# Change directory if an argument is passed in
if [[ ! -z "$1" ]]; then
    cd "$1"
fi
tox
