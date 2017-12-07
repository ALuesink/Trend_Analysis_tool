# -*- coding: utf-8 -*-
"""Delete processed run"""

from sqlalchemy import create_engine, Table, MetaData
import warnings
import config

import database

def del_runprocessed(run):
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
                
                del_Run = Sample_Processed.delete().where(Sample_Processed.c.Run_ID == run_id)
                conn.execute(del_Run)
            else:
                print("There is no processed data in the database for this run")
            
            conn.close()
            
        except Exception, e:
            print(repr(e))