# -*- coding: utf-8 -*-
"""Update Run data in database"""

import warnings

import delete
import upload
from Trend_Analysis.processed_data import upload as proc_upload

def up_Run_in_database(run, path, sequencer):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        try:            
        
            delete.del_from_database(run)
            
            upload.up_to_database(run, path, sequencer)
            
            proc_upload.up_to_database(run, path, sequencer)
            
        except Exception, e:
            print(repr(e))