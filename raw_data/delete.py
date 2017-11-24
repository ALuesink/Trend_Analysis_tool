# -*- coding: utf-8 -*-
"""Delete raw data"""

from sqlalchemy import create_engine, Table, MetaData
import warnings
import config

import database

def del_from_database(run):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        
        try:
            metadata = MetaData()
            engine = engine = create_engine("mysql+pymysql://"+config.MySQL_DB["username"]+":"+config.MySQL_DB["password"]+"@"+config.MySQL_DB["host"]+"/"+config.MySQL_DB["database"], echo=False)      
            
            Run = Table("Run",metadata,autoload=True,autoload_with=engine)
            Run_per_Lane = Table("Run_per_Lane",metadata,autoload=True,autoload_with=engine)
            Sample_Sequencer = Table("Sample_Sequencer",metadata,autoload=True,autoload_with=engine)
            Sample_Processed = Table("Sample_Processed", metadata, autoload=True, autoload_with=engine)
            
            conn = engine.connect()        
            
            run_in_db = database.get.Runs()
            run_id = run_in_db[run]
            
            del_Run = Run.delete().where(Run.c.Run_ID == run_id)
            del_Run_per_Lane = Run_per_Lane.delete().where(Run_per_Lane.c.Run_ID == run_id)
            del_Sample_Sequencer = Sample_Sequencer.delete().where(Sample_Sequencer.c.Run_ID == run_id)
            del_Sample_Processed = Sample_Processed.delete().where(Sample_Processed.c.Run_ID == run_id)            
            
            conn.execute(del_Run)
            conn.execute(del_Run_per_Lane)
            conn.execute(del_Sample_Sequencer)
            conn.execute(del_Sample_Processed)            
            
            conn.close()
        
        except Exception, e:
            print(repr(e))