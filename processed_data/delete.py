# -*- coding: utf-8 -*-
"""Delete processed data"""

from sqlalchemy import create_engine, Table, MetaData
import warnings
import config

import database

def del_from_database(run):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        
        try:
            metadata = MetaData()
            engine = create_engine("mysql+pymysql://"+config.MySQL_DB["username"]+":"+config.MySQL_DB["password"]+"@"+config.MySQL_DB["host"]+"/"+config.MySQL_DB["database"], echo=False)
            
            Sample_Processed = Table("Sample_Processed", metadata, autoload=True, autoload_with=engine)
            
            conn = engine.connect()
            
            run_in_db = database.get.Runs()
            run_id = run_in_db[run]
            
            del_Sample_Processed = Sample_Processed.delete().where(Sample_Processed.c.Run_ID == run_id)
            conn.execute(del_Sample_Processed)
            
            conn.close()
        
        except Exception, e:
            print(repr(e))