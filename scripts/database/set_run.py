# -*- coding: utf-8 -*-
"""Set run name"""


def set_run_name(run):
    if run.count("_") == 4:
        index = run.rfind("_")
        run = run[:index]
    elif run.count("_") == 5:
        index1 = run.rfind("_")
        index2 = run.rfind("_", 0, index1)
        run = run[:index2]

    return run
