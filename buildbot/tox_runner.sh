#!/usr/bin/env bash
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

if [[ ! -z "$1" ]]; then
    cd "$1"
    shift
fi

tox -- $@
