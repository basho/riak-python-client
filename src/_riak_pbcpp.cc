#include <Python.h>

static PyMethodDef RiakPbMethods[] = {
	{NULL, NULL, 0, NULL} /* Sentinel */
};

PyMODINIT_FUNC
init_riak_pbcpp(void)
{
	PyObject *m;
	m = Py_InitModule("riak.pb._riak_pbcpp", RiakPbMethods);
	if (m == NULL)
		return;
}
