"""Trend Analysis tool"""

import argparse
import config
import scripts.upload
import scripts.delete


# Upload functions
def upload_raw_data(args):
    """Upload raw run data to the database"""
    scripts.upload.raw_data.up_to_database(args.run, args.path, args.sequencer)


def upload_processed_data(args):
    """Upload processed run data to the database"""
    scripts.upload.run_processed.up_to_database(args.run, args.path)


def upload_sample_processed(args):
    """Upload processed sample data to the database"""
    scripts.upload.sample_processed.up_to_database(args.run, args.path, args.samples)


# Delete frunctions
def delete_run_all_data(args):
    """Delete run data from the whole database"""
    scripts.delete.run_all.del_all_rundata(args.run)


def delete_run_raw_data(args):
    """Delete raw run data from the database"""
    scripts.delete.run_rawdata.del_run_rawdata(args.run)


def delete_sample_proc_data(args):
    """Delete processed samples from the database"""
    scripts.delete.sample_processed.del_sampledata(args.run, args.samples)


def delete_run_proc_data(args):
    """Delete processed run data"""
    scripts.delete.run_processed.del_runprocessed(args.run)


# Delete + upload functions
def update_run_data(args):
    """Delete and then upload all run data"""
    scripts.delete.run_all.del_all_rundata(args.run)
    scripts.upload.raw_data.up_to_database(args.run, args.path_raw, args.sequencer)
    scripts.upload.run_processed.up_to_database(args.run, args.path_proc)


def update_proc_run_data(args):
    """Delete and then upload processed run data"""
    scripts.delete.run_processed.del_runprocessed(args.run)
    scripts.upload.run_processed.up_to_database(args.run, args.path)


def update_sample_proc_data(args):
    """Delete and then upload processed sample data"""
    scripts.delete.sample_processed.del_sampledata(args.run, args.samples)
    scripts.upload.sample_processed.up_to_database(args.run, args.path, args.samples)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    # upload data
    parser_upload = subparser.add_parser('upload', help='Upload data to database')
    subparser_upload = parser_upload.add_subparsers()

    parser_upload_raw = subparser_upload.add_parser('raw_data', help='upload raw data to database')
    parser_upload_raw.add_argument('run', help='Run name')
    parser_upload_raw.add_argument('path', help='Path to run')
    parser_upload_raw.add_argument('sequencer', choices=config.Sequencers, help='Sequencer name')
    parser_upload_raw.set_defaults(func=upload_raw_data)

    parser_upload_processed = subparser_upload.add_parser('processed_data', help='upload processed data to database')
    parser_upload_processed.add_argument('run', help='Run name')
    parser_upload_processed.add_argument('path', help='Path to run')
    parser_upload_processed.set_defaults(func=upload_processed_data)

    parser_upload_samples_proc = subparser_upload.add_parser('sample_processed', help='upload processed sample data to database')
    parser_upload_samples_proc.add_argument('run', help='Run name')
    parser_upload_samples_proc.add_argument('path', help='Path to run')
    parser_upload_samples_proc.add_argument('samples', default=[], nargs='+', help='Sample names')
    parser_upload_samples_proc.set_defaults(func=upload_sample_processed)

    # delete data
    parser_delete = subparser.add_parser('delete', help='Delete data from database')
    subparser_delete = parser_delete.add_subparsers()

    parser_delete_run_all = subparser_delete.add_parser('run_all', help='delete run data from all tables in database')
    parser_delete_run_all.add_argument('run', help='Run name')
    parser_delete_run_all.set_defaults(func=delete_run_all_data)

    parser_delete_raw_run = subparser_delete.add_parser('raw_run', help='delete raw run data from the database')
    parser_delete_raw_run.add_argument('run', help='Run name')
    parser_delete_raw_run.set_defaults(func=delete_run_raw_data)

    parser_delete_sample_proc = subparser_delete.add_parser('sample_proc', help='delete processed sample data from the database')
    parser_delete_sample_proc.add_argument('run', help='Run name')
    parser_delete_sample_proc.add_argument('samples', default=[], nargs='+', help='Sample names')
    parser_delete_sample_proc.set_defaults(func=delete_sample_proc_data)

    parser_delete_run_proc = subparser_delete.add_parser('run_proc', help='delete processed run data from the database')
    parser_delete_run_proc.add_argument('run', help='Run name')
    parser_delete_run_proc.set_defaults(func=delete_run_proc_data)

    # update data
    parser_update = subparser.add_parser('update', help='Delete and update data')
    subparser_update = parser_update.add_subparsers()

    parser_update_sample_proc = subparser_update.add_parser('sample', help='delete and update processed sample data')
    parser_update_sample_proc.add_argument('run', help='Run name')
    parser_update_sample_proc.add_argument('path', help='Path to run')
    parser_update_sample_proc.add_argument('samples', default=[], nargs='+', help='Sample names')
    parser_update_sample_proc.set_defaults(func=update_sample_proc_data)

    parser_update_run = subparser_update.add_parser('run_all', help='delete and update all run data')
    parser_update_run.add_argument('run', help='Run name')
    parser_update_run.add_argument('path_raw', help='Path to raw run')
    parser_update_run.add_argument('path_proc', help='Path to processed run')
    parser_update_run.add_argument('sequencer', choices=config.Sequencers, help='Sequencer name')
    parser_update_run.set_defaults(func=update_run_data)

    args = parser.parse_args()
    args.func(args)
