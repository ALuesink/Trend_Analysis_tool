# -*- coding: utf-8 -*-
"""Trend analysis package"""


def set_run_name(run):
    if run.count("_") == 3:
        index = run.rfind("_")
        run = run[:index]
        
    return run