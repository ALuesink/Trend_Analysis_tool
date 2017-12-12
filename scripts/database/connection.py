# -*- coding: utf-8 -*-
"""Make database connection and set Table objects"""

from sqlalchemy import create_engine, Table, MetaData
import config

metadata = MetaData()
def engine():
    engine = create_engine("mysql+pymysql://{username}:{password}@{host}/{database}".format(
        username=config.MySQL_DB["username"],
        password=config.MySQL_DB["password"],
        host=config.MySQL_DB["host"],
        database=config.MySQL_DB["database"]
        ), echo=False)
    return engine

def sequencer_table(engine):
    sequencer = Table("Sequencer", metadata, autoload=True, autoload_with=engine)
    return sequencer
    
def run_table(engine):
    run = Table("Run", metadata, autoload=True, autoload_with=engine)
    return run

def run_per_lane_table(engine):
    run_per_lane = Table("Run_per_Lane", metadata, autoload=True, autoload_with=engine)
    return run_per_lane
    
def sample_sequencer_table(engine):
    sample_sequencer = Table("Sample_Sequencer", metadata, autoload=True, autoload_with=engine)
    return sample_sequencer

def sample_processed_table(engine):
    sample_processed = Table("Sample_Processed", metadata, autoload=True, autoload_with=engine)
    return sample_processed
    
def bait_set_table(engine):
    bait_set = Table("Bait_Set", metadata, autoload=True, autoload_with=engine)
    return bait_set