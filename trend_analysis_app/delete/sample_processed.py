# -*- coding: utf-8 -*-
"""Delete processed samples from the database"""

from sqlalchemy import create_engine, Table, MetaData
import warnings
import config

import database

def del_sampledata(run, samples):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        
        try:
            metadata = MetaData()
            engine = create_engine("mysql+pymysql://"+config.MySQL_DB["username"]+":"+config.MySQL_DB["password"]+"@"+config.MySQL_DB["host"]+"/"+config.MySQL_DB["database"], echo=False)
            
            Sample_Processed = Table("Sample_Processed", metadata, autoload=True, autoload_with=engine)
            
            conn = engine.connect()
            
            run_in_db = database.get.Runs()
            
            if run in run_in_db:            
                run_id = run_in_db[run]
                for sample in samples:
                    del_Sample = Sample_Processed.delete().where(Sample_Processed.c.Run_ID == run_id ).where(Sample_Processed.c.Sample_name == sample)
                    conn.execute(del_Sample)
            else:
                print("This run is not in the database")
            
            conn.close()
        except Exception, e:
            print(repr(e))