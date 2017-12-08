"""Utility functions."""


def convert_numbers(item):
    """
    Try to convert str to number.

    >>> try_int('2')
    2
    >>> try_int('foo')
    'foo'
    """
    try:
        return int(item)
    except ValueError:
        try:
            return float(item)
        except ValueError:
            return item
