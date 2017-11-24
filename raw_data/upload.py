# -*- coding: utf-8 -*-
"""Upload raw data function"""

from sqlalchemy import create_engine, Table, MetaData
from datetime import datetime
from html_table_parser import HTMLTableParser
from utils import convert_numbers
import commands
import config
import warnings

import database

def up_to_database(run, path, sequencer):
    """Get data of inserted run and upload the data to the database"""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        runs_db = database.get.Runs()
        if run in runs_db:
            print("This run is already in the database")
        else:
            try:
                run_stats, lane_stats = laneHTML(run,path)
                sample_stats = laneBarcodeHTML(run, path)
                
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

def laneHTML(run, path):
    
    try:    
        stats_lane = []
        stats_run = []
        epoch = datetime.utcfromtimestamp(0)
        
        date = run.split("_")[0]
        date = "20" + date[0:2] + "-" + date[2:4] + "-" + date[4:6]
        d = datetime.strptime(date, "%Y-%m-%d")
        as_date = (d-epoch).days

        lanehtml = commands.getoutput("find "+ str(path) + str(run) +"/Data/Intensities/BaseCalls/Reports/html/*/all/all/all/ -iname \"lane.html\"")

        with open(lanehtml, "r") as lane:
            html = lane.read()
            tableParser = HTMLTableParser()
            tableParser.feed(html)
            tables = tableParser.tables                         #tables[1]==run tables[2]==lane
            
            stats_run = tables[1][1]
            stats_run = [convert_numbers(item.replace(",", "")) for item in stats_run]
            PCT_PF = (float(stats_run[1])/stats_run[0])*100
            PCT_PF = float("{0:.2f}".format(PCT_PF))
            stats_run.extend([date,as_date,PCT_PF])                
            
            for lane in tables[2][1:]:
                lane = [convert_numbers(item.replace(",", "")) for item in lane]
                stats_lane.append(lane)
        
        return stats_run, stats_lane
    except Exception, e:
        print(repr(e))

def laneBarcodeHTML(run, path):
    try:
        stats_sample = []

        samplehtml = commands.getoutput("find " + str(path) + str(run) + "/Data/Intensities/BaseCalls/Reports/html/*/all/all/all/ -iname \"laneBarcode.html\"")

        with open(samplehtml, "r") as sample:
            html = sample.read()
            tableParser = HTMLTableParser()
            tableParser.feed(html)
            tables = tableParser.tables                         #tables[1]==run tables[2]==sample
            
            for sample_lane in tables[2][1:]:
                if sample_lane[1].upper() != "DEFAULT":
                    stats_sample_lane = [convert_numbers(item.replace(",","")) for item in sample_lane]
                    stats_sample.append(stats_sample_lane)
        
        return stats_sample

    except Exception, e:
        print(repr(e))



















