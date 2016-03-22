import os

if os.environ.get('PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION', 'unset') == 'cpp':
    import riak.pb._riak_pbcpp
