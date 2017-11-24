# -*- coding: utf-8 -*-
"""Upload raw data function"""

from sqlalchemy import create_engine, Table, MetaData
import config
import warnings

import database
import data

def up_to_database(run, path, sequencer):
    """Get data of inserted run and upload the data to the database"""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        
        try:        
            runs_db = database.get.Runs()
            if run in runs_db:
                print("This run is already in the database")
            else:
                run_stats, lane_stats = data.import_data.laneHTML(run,path)
                sample_stats = data.import_data.laneBarcodeHTML(run, path)
                
                metadata = MetaData()
                engine = engine = create_engine("mysql+pymysql://"+config.MySQL_DB["username"]+":"+config.MySQL_DB["password"]+"@"+config.MySQL_DB["host"]+"/"+config.MySQL_DB["database"], echo=False)
    
                Sequencer = Table("Sequencer",metadata,autoload=True,autoload_with=engine)
                Run = Table("Run",metadata,autoload=True,autoload_with=engine)
                Run_per_Lane = Table("Run_per_Lane",metadata,autoload=True,autoload_with=engine)
                Sample_Sequencer = Table("Sample_Sequencer",metadata,autoload=True,autoload_with=engine)
                
                conn = engine.connect()
                
                seq_db = database.get.Sequencer()
                seq_ID = 0            
                if sequencer in seq_db:
                    seq_ID = seq_db[sequencer]
                else:
                    insert_Seq = Sequencer.insert().values(Name=sequencer)
                    con_seq = conn.execute(insert_Seq)
                    seq_ID = con_seq.inserted_primary_key
                
                insert_Run = Run.insert().values(Run=str(run), Cluster_Raw=run_stats[0], Cluster_PF=run_stats[1], Yield_Mbases=run_stats[2], Seq_ID=seq_ID, Date=run_stats[3], asDate=run_stats[4],Sequencer=sequencer,PCT_PF_Cluster=run_stats[5])          
                con_Run = conn.execute(insert_Run)
                run_ID = con_Run.inserted_primary_key
                
                for lane in lane_stats:
                    insert_Lane = Run_per_Lane.insert().values(Lane=str(lane[0]), PF_Clusters=lane[1], PCT_of_lane=lane[2], PCT_perfect_barcode=lane[3], PCT_one_mismatch_barcode=lane[4], Yield_Mbases=lane[5], PCT_PF_Clusters=lane[6], PCT_Q30_bases=lane[7], Mean_Quality_Score=lane[8], Run_ID=run_ID)
                    conn.execute(insert_Lane)
                    
                for sample in sample_stats:
                    insert_Sample = Sample_Sequencer.insert().values(Lane=str(sample[0]),Project=sample[1].upper(),Sample_name=sample[2],Barcode_sequence=sample[3],PF_Clusters=sample[4],PCT_of_lane=sample[5],PCT_perfect_barcode=sample[6],PCT_one_mismatch_barcode=sample[7],Yield_Mbases=sample[8],PCT_PF_Clusters=sample[9],PCT_Q30_bases=sample[10],Mean_Quality_Score=sample[11],Run_ID=run_ID)
                    conn.execute(insert_Sample)
                
                conn.close()
        except Exception, e:
            print(repr(e))

