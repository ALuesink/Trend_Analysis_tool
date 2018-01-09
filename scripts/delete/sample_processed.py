# -*- coding: utf-8 -*-
"""Delete processed samples from the database"""

from ..database import connection, get, set_run
import warnings


def del_sampledata(run, samples):
    with warnings.catch_warnings():
        warnings.simplefilter('error')

        try:
            run = set_run.set_run_name(run)

            engine = connection.engine()
            conn = engine.connect()

            sample_processed = connection.sample_processed_table(engine)

            runs_in_db = get.runs()

            if run in runs_in_db:
                run_id = runs_in_db[run]

                for sample in samples:
                    del_sample = sample_processed.delete().\
                        where(sample_processed.c.Run_ID == run_id).\
                        where(sample_processed.c.Sample_name == sample)
                    conn.execute(del_sample)
            else:
                sys.stdout.write('This run is not in the database \n')

            conn.close()

        except Exception, e:
            sys.stdout.write(e)
