# -*- coding: utf-8 -*-
"""Upload raw data
"""

from ..database import connection, get, set_run
import warnings
import data


def up_to_database(run, path, sequencer):
    """Get data of inserted run and upload the data to the database
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error")
#        print("run: " + run)
#        print("path: " + path)
#        print("sequencer: " + sequencer)
        try:
            run = set_run.set_run_name(run)
            runs_db = get.runs()

            if run in runs_db:
                print("This run is already in the database")
            else:
                run_dict, lane_dict = data.import_data.laneHTML(run, path)
                samples_dict = data.import_data.laneBarcodeHTML(run, path)

                engine = connection.engine()

                sequencer_table = connection.sequencer_table(engine)
                run_table = connection.run_table(engine)
                run_lane_table = connection.run_per_lane_table(engine)
                sample_sequencer_table = connection.sample_sequencer_table(engine)

                conn = engine.connect()

                seq_db = get.sequencer()
                seq_ID = 0
                if sequencer in seq_db:
                    seq_ID = seq_db[sequencer]
                else:
                    insert_seq = sequencer_table.insert().values(Name=sequencer)
                    con_seq = conn.execute(insert_seq)
                    seq_ID = con_seq.inserted_primary_key

                insert_run = run_table.insert().values(
                    Run=str(run), Cluster_Raw=run_dict['Cluster_Raw'],
                    Cluster_PF=run_dict['Cluster_PF'],
                    Yield_Mbases=run_dict['Yield_Mbases'], Seq_ID=seq_ID,
                    Date=run_dict['Date'], asDate=run_dict['asDate'],
                    Sequencer=sequencer)

                con_run = conn.execute(insert_run)
                run_ID = con_run.inserted_primary_key

                for lane in lane_dict:
                    insert_lane = run_lane_table.insert().values(
                        Lane=str(lane_dict[lane]['Lane']), 
                        PF_Clusters=lane_dict[lane]['PF_Clusters'],
                        PCT_of_lane=lane_dict[lane]['PCT_of_lane'], 
                        PCT_perfect_barcode=lane_dict[lane]['PCT_perfect_barcode'],
                        PCT_one_mismatch_barcode=lane_dict[lane]['PCT_one_mismatch_barcode'],
                        Yield_Mbases=lane_dict[lane]['Yield_Mbases'], 
                        PCT_PF_Clusters=lane_dict[lane]['PCT_PF_Clusters'],
                        PCT_Q30_bases=lane_dict[lane]['PCT_Q30_bases'],
                        Mean_Quality_Score=lane_dict[lane]['Mean_Quality_Score'], 
                        Run_ID=run_ID)

                    conn.execute(insert_lane)

                for sample in samples_dict:
                    insert_sample = sample_sequencer_table.insert().values(
                        Lane=str(samples_dict[sample]['Lane']), 
                        Project=samples_dict[sample]['Project'].upper(),
                        Sample_name=samples_dict[sample]['Sample_name'], 
                        Barcode_sequence=samples_dict[sample]['Barcode_sequence'],
                        PF_Clusters=samples_dict[sample]['PF_Clusters'], 
                        PCT_of_lane=samples_dict[sample]['PCT_of_lane'],
                        PCT_perfect_barcode=samples_dict[sample]['PCT_perfect_barcode'],
                        PCT_one_mismatch_barcode=samples_dict[sample]['PCT_one_mismatch_barcode'],
                        Yield_Mbases=samples_dict[sample]['Yield_Mbases'], 
                        PCT_PF_Clusters=samples_dict[sample]['PCT_PF_Clusters'],
                        PCT_Q30_bases=samples_dict[sample]['PCT_Q30_bases'],
                        Mean_Quality_Score=samples_dict[sample]['Mean_Quality_Score'], 
                        Run_ID=run_ID)

                    conn.execute(insert_sample)

                conn.close()
        except Exception, e:
            print(e)
