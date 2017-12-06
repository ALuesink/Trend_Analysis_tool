# -*- coding: utf-8 -*-
"""Upload raw data"""

from sqlalchemy import create_engine, Table, MetaData
import config
import warnings

import data
import database

def up_to_database(run, path, sequencer):
    """Get data of inserted run and upload the data to the database"""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        print("run: " + run)
        print("path: " + path)
        print("sequencer: " + sequencer)
        try:        
            runs_db = database.get.Runs()
            if run in runs_db:
                print("This run is already in the database")
            else:
                run_dict, lane_dict = data.import_data.laneHTML(run,path)
                samples_dict = data.import_data.laneBarcodeHTML(run, path)
                
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
                
                insert_Run = Run.insert().values(Run=str(run), Cluster_Raw=run_dict['Cluster_Raw'], Cluster_PF=run_dict['Cluster_PF'], Yield_Mbases=run_dict['Yield_Mbases'], Seq_ID=seq_ID, Date=run_dict['Date'], asDate=run_dict['asDate'],Sequencer=sequencer)          

                con_Run = conn.execute(insert_Run)
                run_ID = con_Run.inserted_primary_key
                
                for lane in lane_dict:
                    insert_Lane = Run_per_Lane.insert().values(Lane=str(lane[lane]['Lane']), PF_Clusters=lane[lane]['PF_Clusters'], PCT_of_lane=lane[lane]['PCT_of_lane'], PCT_perfect_barcode=lane[lane]['PCT_perfect_barcode'], PCT_one_mismatch_barcode=lane[lane]['PCT_one_mismatch_barcode'], Yield_Mbases=lane[lane]['Yield_Mbases'], PCT_PF_Clusters=lane[lane]['PCT_PF_Clusters'], PCT_Q30_bases=lane[lane]['PCT_Q30_bases'], Mean_Quality_Score=lane[lane]['Mean_Quality_Score'], Run_ID=run_ID)
                    conn.execute(insert_Lane)
                    
                for sample in samples_dict:
                    insert_Sample = Sample_Sequencer.insert().values(Lane=str(sample[sample]['Lane']),Project=sample[sample]['Project'].upper(),Sample_name=sample[sample]['Sample_name'],Barcode_sequence=sample[sample]['Barcode_sequence'],PF_Clusters=sample[sample]['PF_Clusters'],PCT_of_lane=sample[sample]['PCT_of_lane'],PCT_perfect_barcode=sample[sample]['PCT_perfect_barcode'],PCT_one_mismatch_barcode=sample[sample]['PCT_one_mismatch_barcode'],Yield_Mbases=sample[sample]['Yield_Mbases'],PCT_PF_Clusters=sample[sample]['PCT_PF_Clusters'],PCT_Q30_bases=sample[sample]['PCT_Q30_bases'],Mean_Quality_Score=sample[sample]['Mean_Quality_Score'],Run_ID=run_ID)
                    conn.execute(insert_Sample)
                
                conn.close()
        except Exception, e:
            print(repr(e))