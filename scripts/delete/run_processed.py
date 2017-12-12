# -*- coding: utf-8 -*-
"""Delete processed run
"""

from ..database import connection, get, set_run
import warnings


def del_runprocessed(run):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        
        try:           
            engine = connection.engine()
            conn = engine.connect()
            
            sample_processed = connection.sample_processed_table(engine)
            
            run = set_run.set_run_name(run)
            run_in_db = get.runs()
            
            if run in run_in_db:            
                run_id = run_in_db[run]
                
                del_run = sample_processed.delete().where(sample_processed.c.Run_ID == run_id)
                conn.execute(del_run)
            else:
                print("There is no processed data in the database for this run")
            
            conn.close()
            
        except Exception, e:
            print(repr(e))