# -*- coding: utf-8 -*-
"""Delete run data from the whole database
"""

from ..database import connection, get, set_run
import warnings


def del_all_rundata(run):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        try:
            run = set_run.set_run_name(run)

            engine = connection.engine()
            conn = engine.connect()

            run = connection.run_table(engine)
            run_per_lane = connection.run_per_lane_table(engine)
            sample_sequencer = connection.sample_sequencer_table(engine)
            sample_processed = connection.sample_processed_table(engine)

            runs_in_db = get.runs()

            if run in runs_in_db:
                run_id = runs_in_db[run]

                del_run = run.delete().where(run.c.Run_ID == run_id)
                del_run_per_lane = run_per_lane.delete().\
                    where(run_per_lane.c.Run_ID == run_id)
                del_sample_sequencer = sample_sequencer.delete().\
                    where(sample_sequencer.c.Run_ID == run_id)
                del_sample_processed = sample_processed.delete().\
                    where(sample_processed.c.Run_ID == run_id)

                conn.execute(del_run)
                conn.execute(del_run_per_lane)
                conn.execute(del_sample_sequencer)
                conn.execute(del_sample_processed)
            else:
                print("This run is not in the database")

            conn.close()

        except Exception, e:
            print(e)
