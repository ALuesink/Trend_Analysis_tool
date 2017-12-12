# -*- coding: utf-8 -*-
"""Delete processed samples from the database
"""

from ..database import connection, get, set_run
import warnings


def del_sampledata(run, samples):
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
                for sample in samples:
                    del_sample = sample_processed.delete().\
                        where(sample_processed.c.Run_ID == run_id ).\
                        where(sample_processed.c.Sample_name == sample)
                    conn.execute(del_sample)
            else:
                print("This run is not in the database")
            
            conn.close()
        except Exception, e:
            print(repr(e))