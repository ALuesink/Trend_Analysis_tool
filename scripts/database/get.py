# -*- coding: utf-8 -*-
"""get Runs, Sequencer, Bait Set and processed Runs from database
"""

from sqlalchemy import select
import connection

engine = connection.engine()


def runs():
    """Get Runs and Run ID's of runs already in the database
    """
    run_in_db = {}

    conn = engine.connect()
    run = connection.run_table(engine)

    select_run = select([run.c.Run, run.c.Run_ID])
    result_run = conn.execute(select_run).fetchall()

    for run in result_run:
        run_in_db[run[0]] = run[1]

    conn.close()

    return run_in_db


def sequencer():
    """Get Sequencer and Sequencer ID's of sequencers already in the database
    """
    seq_in_db = {}

    conn = engine.connect()
    sequencer = connection.sequencer_table(engine)

    select_seq = select([sequencer.c.Name, sequencer.c.Seq_ID])
    result_seq = conn.execute(select_seq).fetchall()

    for seq in result_seq:
        seq_in_db[seq[0]] = seq[1]

    conn.close()

    return seq_in_db


def bait_set():
    """Get Bait Set and Bait Set ID's of bait sets already in the database
    """
    baitset_in_db = {}

    conn = engine.connect()
    bait_set = connection.bait_set_table(engine)

    select_baitset = select([bait_set.c.Bait_ID, bait_set.c.Bait_name])
    result_baitset = conn.execute(select_baitset).fetchall()

    for baitset in result_baitset:
        baitset_in_db[baitset[1]] = baitset[0]

    conn.close()

    return baitset_in_db


def runs_processed():
    """Get Runs which are already in the Sample Processed table
    """
    run_processed_db = set()

    conn = engine.connect()
    sample_processed = connection.sample_processed_table(engine)
    run = connection.run_table(engine)

    select_run_processed = select([run.c.Run]).\
        where(sample_processed.c.Run_ID == run.c.Run_ID)
    result_run_processed = conn.execute(select_run_processed).fetchall()

    for run in result_run_processed:
        run_processed_db.add(run[0])

    conn.close()

    return run_processed_db


def sample_run_processed():
    """Get the combination of samples and their run id's from the Sample Processed table
    """
    sample_run_db = {}

    conn = engine.connect()
    sample_processed = connection.sample_processed_table(engine)
    run = connection.run_table(engine)

    select_sample_run = select([sample_processed.c.Sample_name, run.c.Run]).\
        where(sample_processed.c.Run_ID == run.c.Run_ID)
    result_sample_run = conn.execute(select_sample_run).fetchall()

    for sample in result_sample_run:
        sample_run_db[sample[0]] = sample[1]

    conn.close()

    return sample_run_db
