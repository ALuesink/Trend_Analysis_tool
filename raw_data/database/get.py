# -*- coding: utf-8 -*-
"""get Runs, Sequencer from database"""

from sqlalchemy import create_engine, select, Table, MetaData
import config

metadata = MetaData()
engine = create_engine("mysql+pymysql://"+config.MySQL_DB["username"]+":"+config.MySQL_DB["password"]+"@"+config.MySQL_DB["host"]+"/"+config.MySQL_DB["database"], echo=False)


def Runs():
    conn = engine.connect()    
    Run = Table("Run",metadata,autoload=True,autoload_with=engine)  
    
    select_run = select([Run.c.Run, Run.c.Run_ID])
    result_run = conn.execute(select_run).fetchall()
    run_in_db = {}
    for run in result_run:
        run_in_db[run[0]] = run[1]
    
    conn.close()
    return run_in_db
    
def Sequencer():
    conn = engine.connect()
    Sequencer = Table("Sequencer",metadata,autoload=True,autoload_with=engine)
    
    select_seq = select([Sequencer.c.Name, Sequencer.c.Seq_ID])
    result_seq = conn.execute(select_seq).fetchall()
    seq_in_db = {}
    for seq in result_seq:
        seq_in_db[seq[0]] = seq[1]
    
    conn.close()    
    return seq_in_db
    