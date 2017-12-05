"""Trend Analysis tool"""

import argparse

import upload
import delete

#Upload functions
def upload_raw_data(args):
    """Upload raw run data to the database"""
    upload.raw_data.up_to_database(args.run, args.path, args.sequencer)

def upload_processed_data(args):
    """Upload processed run data to the database"""
    upload.run_processed.up_to_database(args.run, args.path)
    
def upload_sample_processed(args):
    """Upload processed sample data to the database"""
    upload.sample_processed.up_to_database(args.run, args.path, args.samples)
    
#Delete frunctions
def delete_run_all_data(args):
    """Delete run data from the whole database"""
    delete.run_all.del_all_rundata(args.run)

def delete_run_raw_data(args):
    """Delete raw run data from the database"""
    delete.run_rawdata.del_run_rawdata(args.run)
    
def delete_sample_data(args):
    """Delete processed samples from the database"""
    delete.sample_processed.del_sampledata(args.run, args.samples)

#Delete + upload functions
def update_sample_data(args):
    """Delete and then update processed sample data"""
    delete.sample_processed.del_sampledata(args.run, args.samples)
    upload.sample_processed.up_to_database(args.run, args.path, args.samples)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()
    
#    output_parser = argparse.ArgumentParser(add_help=False)
    
    # raw data
    parser_raw = subparser.add_parser('raw_data', help='Raw data functions')
    subparser_raw = parser_raw.add_subparsers()
    
    parser_raw_upload = subparser_raw.add_parser('upload', help='upload raw data to database')
    parser_raw_upload.add_argument('run', help='Run name')
    parser_raw_upload.add_argument('path', help='Path to run')
    parser_raw_upload.add_argument('sequencer', choices=['hiseq_umc01', 'nextseq_umc01', 'nextseq_umc02', 'novaseq_umc01'], help='Sequencer name')
    parser_raw_upload.set_defaults(func=upload_raw_data)
    
    parser_raw_delete = subparser_raw.add_parser('delete', help='delete all run data from database')
    parser_raw_delete.add_argument('run', help='Run name')
    parser_raw_delete.set_defaults(func=delete_run_all_data)
    
    parser_raw_delete = subparser_raw.add_parser('delete', help='delete all raw run data from database')
    parser_raw_delete.add_argument('run', help='Run name')
    parser_raw_delete.set_defaults(func=delete_run_raw_data)


    # processed data
    parser_processed = subparser.add_parser('processed_data', help='Processed data functions')
    subparser_processed = parser_processed.add_subparsers()
    
    parser_processed_upload = subparser_processed.add_parser('upload', help='upload processed data to database')
    parser_processed_upload.add_argument('run', help='Run name')
    parser_processed_upload.add_argument('path', help='Path to run')
    parser_processed_upload.set_defaults(func=upload_processed_data)
    
    parser_processed_upload = subparser_processed.add_parser('upload', help='upload sample data to database')
    parser_processed_upload.add_argument('run', help='Run name')
    parser_processed_upload.add_argument('path', help='Path to run')
    parser_processed_upload.add_argument('samples', default=[], nargs='+', help='Sample names')
    parser_processed_upload.set_defaults(func=upload_sample_processed)
    
    parser_processed_delete = subparser_processed.add_parser('delete', help='delete processed sample data from database')
    parser_processed_delete.add_argument('run', help='Run name')
    parser_processed_delete.add_argument('samples', help='Samples names')
    parser_processed_delete.set_defaults(func=delete_sample_data)
    
    parser_processed_update = subparser_processed.add_parser('update', help='delete and upload processed sample data')    
    parser_processed_update.add_argument('run', help='Run name')
    parser_processed_update.add_argument('path', help='Path to run')
    parser_processed_update.add_argument('samples', default=[], nargs='+', help='Sample names')
    
    args = parser.parse_args()
    args.func(args) 
