"""Trend Analysis tool"""

import argparse

import raw_data

def upload_raw_data(args):
    """Upload Raw data to database"""
    raw_data.upload.to_database(args.run, args.path, args.sequencer)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()
    
    output_parser = argparse.ArgumentParser(add_help=False)
    
    # raw data
    parser_raw = subparser.add_parser('raw_data', help='Raw data')
    subparser_raw = parser_raw.add_subparsers()
    
    parser_raw_upload = subparser_raw.add_parser('upload', help='upload raw data to database', parents=[output_parser])
    parser_raw_upload.add_argument('run', help='Run name')
    parser_raw_upload.add_argument('path', help='Path to run')
    parser_raw_upload.add_arguement('sequencer', help='Sequencer name')
    parser_raw_upload.set_defaults(func=upload_raw_data)