# -*- coding: utf-8 -*-
"""get Runs, Sequencer, Bait Set and processed Runs from database"""

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

def BaitSet():
    conn = engine.connect()
    Bait_Set = Table("Bait_Set", metadata, autoload=True, autoload_with=engine)
    
    select_baitset = select([Bait_Set.c.Bait_ID, Bait_Set.c.Bait_name])
    result_baitset = conn.execute(select_baitset).fetchall()
    baitset_in_db = {}
    for baitset in result_baitset:
        baitset_in_db[baitset[1]] = baitset[0]
        
    conn.close()
    return baitset_in_db

def Runs_processed():
    conn = engine.connect()
    Sample_Processed = Table("Sample_Processed", metadata, autoload=True, autoload_with=engine)
    Run = Table("Run",metadata,autoload=True,autoload_with=engine)
    
    select_run_processed = select([Run.c.Run]).where(Sample_Processed.c.Run_ID == Run.c.Run_ID)
    result_run_processed = conn.execute(select_run_processed).fetchall()
    run_processed_db = set()
    for run in result_run_processed:
        run_processed_db.add(run[0])
        
    conn.close()
    return run_processed_db
    
