# -*- coding: utf-8 -*-
"""Set run name"""

def set_run_name(run):
    if run.count("_") == 4:
        index = run.rfind("_")
        run = run[:index]
        
    return run