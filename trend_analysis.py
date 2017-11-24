"""Trend Analysis tool"""

import argparse

import raw_data
import processed_data


# Upload functions
def upload_raw_data(args):
    """Upload Raw data to the database"""
    raw_data.upload.up_to_database(args.run, args.path, args.sequencer)

def upload_processed_data(args):
    """Upload Processed data to the database"""
    processed_data.upload.up_to_database(args.run, args.path)
    
# Delete functions
def del_run_data(args):
    """Delete all run data from the database"""
    raw_data.delete.del_from_database(args.run)

def del_processed_data(args):
    """Delete processed run data from the database"""
    processed_data.delete.del_from_database(args.run)

# Update functions
def update_run_data(args):
    """Update the Run data by first delete all data and then upload the new data"""
    raw_data.update.up_Run_in_database(args.run, args.path, args.sequencer)


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
    parser_raw_upload.add_arguement('sequencer', help='Sequencer name')
    parser_raw_upload.set_defaults(func=upload_raw_data)
    
    parser_raw_delete = subparser_raw.add_parser('delete', help='delete all run data from database')
    parser_raw_delete.add_argument('run', help='Run name')
    parser_raw_delete.set_defaults(func=del_run_data)

    parser_raw_update = subparser_raw.add_parser('update', help='delete all run data and upload new run data to database')    
    parser_raw_update.add_argument('run', help='Run name')
    parser_raw_update.add_argument('path', help='Path to run')
    parser_raw_update.add_argument('sequencer', help='Sequencer name')
    parser_raw_update.set_defaults(func=update_run_data)
    
    # processed data
    parser_processed = subparser.add_parser('processed_data', help='Processed data functions')
    subparser_processed = parser_processed.add_subparsers()
    
    parser_processed_upload = subparser_processed.add_parser('upload', help='upload processed data to database')
    parser_processed_upload.add_argument('run', help='Run name')
    parser_processed_upload.add_argument('path', help='Path to run')
    parser_processed_upload.set_defaults(func=upload_processed_data)
    
    parser_processed_delete = subparser_processed.add_parser('delete', help='delete processed run data from database')
    parser_processed_delete.add_argument('run', help='Run name')
    parser_processed_delete.set_defaults(func=del_processed_data)
    
    