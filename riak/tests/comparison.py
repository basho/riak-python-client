# -*- coding: utf-8 -*-
from six import PY2, PY3
import collections
import warnings


class Comparison(object):
    '''
    Provide a cross-version object comparison operator
    since its name changed between Python 2.x and Python 3.x
    '''

    if PY3:
        # Stolen from Python 2.7.8's unittest
        _Mismatch = collections.namedtuple('Mismatch', 'actual expected value')

        def _count_diff_all_purpose(self, actual, expected):
            '''
            Returns list of (cnt_act, cnt_exp, elem)
            triples where the counts differ
            '''
            # elements need not be hashable
            s, t = list(actual), list(expected)
            m, n = len(s), len(t)
            NULL = object()
            result = []
            for i, elem in enumerate(s):
                if elem is NULL:
                    continue
                cnt_s = cnt_t = 0
                for j in range(i, m):
                    if s[j] == elem:
                        cnt_s += 1
                        s[j] = NULL
                for j, other_elem in enumerate(t):
                    if other_elem == elem:
                        cnt_t += 1
                        t[j] = NULL
                if cnt_s != cnt_t:
                    diff = self._Mismatch(cnt_s, cnt_t, elem)
                    result.append(diff)

            for i, elem in enumerate(t):
                if elem is NULL:
                    continue
                cnt_t = 0
                for j in range(i, n):
                    if t[j] == elem:
                        cnt_t += 1
                        t[j] = NULL
                diff = self._Mismatch(0, cnt_t, elem)
                result.append(diff)
            return result

        def _count_diff_hashable(self, actual, expected):
            '''
            Returns list of (cnt_act, cnt_exp, elem) triples
            where the counts differ
            '''
            # elements must be hashable
            s, t = self._ordered_count(actual), self._ordered_count(expected)
            result = []
            for elem, cnt_s in s.items():
                cnt_t = t.get(elem, 0)
                if cnt_s != cnt_t:
                    diff = self._Mismatch(cnt_s, cnt_t, elem)
                    result.append(diff)
            for elem, cnt_t in t.items():
                if elem not in s:
                    diff = self._Mismatch(0, cnt_t, elem)
                    result.append(diff)
            return result

        def _ordered_count(self, iterable):
            'Return dict of element counts, in the order they were first seen'
            c = collections.OrderedDict()
            for elem in iterable:
                c[elem] = c.get(elem, 0) + 1
            return c

        def assertItemsEqual(self, expected_seq, actual_seq, msg=None):
            """An unordered sequence specific comparison. It asserts that
            actual_seq and expected_seq have the same element counts.
            Equivalent to::

                self.assertEqual(Counter(iter(actual_seq)),
                                 Counter(iter(expected_seq)))

            Asserts that each element has the same count in both sequences.
            Example:
                - [0, 1, 1] and [1, 0, 1] compare equal.
                - [0, 0, 1] and [0, 1] compare unequal.
            """
            first_seq, second_seq = list(expected_seq), list(actual_seq)
            with warnings.catch_warnings():
                try:
                    first = collections.Counter(first_seq)
                    second = collections.Counter(second_seq)
                except TypeError:
                    # Handle case with unhashable elements
                    differences = self._count_diff_all_purpose(first_seq,
                                                               second_seq)
                else:
                    if first == second:
                        return
                    differences = self._count_diff_hashable(first_seq,
                                                            second_seq)

            if differences:
                standardMsg = 'Element counts were not equal:\n'
                lines = ['First has %d, Second has %d:  %r' %
                         diff for diff in differences]
                diffMsg = '\n'.join(lines)
                standardMsg = self._truncateMessage(standardMsg, diffMsg)

    def assert_raises_regex(self, exception, regexp):
        if PY2:
            return self.assertRaisesRegexp(exception, regexp)
        else:
            return self.assertRaisesRegex(exception, regexp)
