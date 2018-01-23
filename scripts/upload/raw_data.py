# -*- coding: utf-8 -*-
"""Upload raw data"""

from sqlalchemy import select
from ..database import connection, get, set_run
import warnings
import sys
import data


def up_to_database(path):
    """Get data of inserted run and upload the data to the database"""
    with warnings.catch_warnings():
        warnings.simplefilter("always")
        warnings.filterwarnings("error")
        try:
            path = path.strip().rstrip('/')
            run = path.split("/")[-1]
            sequencer = path.split("/")[-2]
            run = set_run.set_run_name(run)
            runs_db = get.runs()

            if run in runs_db:
                sys.stdout.write('This run is already in the database \n')
            else:
                run_dict, lane_dict = data.import_data.laneHTML(run, path)
                samples_dict = data.import_data.laneBarcodeHTML(run, path)

                engine = connection.engine()
                conn = engine.connect()

                run_table = connection.run_table(engine)
                run_lane = connection.run_per_lane_table(engine)
                sample_sequencer = connection.sample_sequencer_table(engine)

                try:
                    insert_run = run_table.insert().values(
                        Run=str(run), Cluster_Raw=run_dict['Cluster_Raw'],
                        Cluster_PF=run_dict['Cluster_PF'],
                        Yield_Mbases=run_dict['Yield_Mbases'],
                        Date=run_dict['Date'], asDate=run_dict['asDate'],
                        Sequencer=sequencer
                        )

                    con_run = conn.execute(insert_run)
                    run_ID = con_run.inserted_primary_key

                    for lane in lane_dict:
                        insert_lane = run_lane.insert().values(
                            Lane=str(lane_dict[lane]['Lane']),
                            PF_Clusters=lane_dict[lane]['PF_Clusters'],
                            PCT_of_lane=lane_dict[lane]['PCT_of_lane'],
                            PCT_perfect_barcode=lane_dict[lane]['PCT_perfect_barcode'],
                            PCT_one_mismatch_barcode=lane_dict[lane]['PCT_one_mismatch_barcode'],
                            Yield_Mbases=lane_dict[lane]['Yield_Mbases'],
                            PCT_PF_Clusters=lane_dict[lane]['PCT_PF_Clusters'],
                            PCT_Q30_bases=lane_dict[lane]['PCT_Q30_bases'],
                            Mean_Quality_Score=lane_dict[lane]['Mean_Quality_Score'],
                            Run_ID=run_ID
                            )

                        conn.execute(insert_lane)

                    for sample in samples_dict:
                        insert_sample = sample_sequencer.insert().values(
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
                            Run_ID=run_ID
                            )

                        conn.execute(insert_sample)

                except Warning, w:
                    print(w)
                    query = select([run_table.c.Run_ID]).\
                        where(run_table.c.Run == run)
                    res = conn.execute(query).fetchall()
                    run_id = res[0][0]

                    delete = run_table.delete().where(run_table.c.Run_ID == run_id)
                    conn.execute(delete)
                    sys.stdout.write('Data deleted from database \n')
                    sys.exit()

                conn.close()

        except Exception, e:
            print(e)
