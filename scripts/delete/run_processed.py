# -*- coding: utf-8 -*-
"""Delete processed run"""

from ..database import connection, get, set_run
import warnings


def del_runprocessed(run):
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

                del_run = sample_processed.delete().where(sample_processed.c.Run_ID == run_id)
                conn.execute(del_run)
            else:
                print('There is no processed data in the database for this run \n')

            conn.close()

        except Exception, e:
            sys.stdout.write(e)
