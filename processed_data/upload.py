# -*- coding: utf-8 -*-
"""Upload processed data to the database"""
from sqlalchemy import create_engine, Table, MetaData
import commands
import vcf
import config
import warnings

import database

def to_database(run, path):
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        runs_processed_db = database.get.Runs_processed()

        if run in runs_processed_db:
            print("This run is already in the database")
        else:
            try:
                vcf_file(run, path)
                picard_files(run, path)
                
                
                
            except Exception, e:
                 print(repr(e))
                 
def vcf_file(run, path):
    try:
        dic_samples = {}
        file_vcf = commands.getoutput("find " + str(path) + str(run) + "/ -maxdepth 1 -iname \"*.filtered_variants.vcf\"")
            
        with open(file_vcf, "r") as vcffile:
            vcf_file = vcf.Reader(vcffile)
            list_samples = vcf_file.samples
            for sample in list_samples:
                dic_samples[sample] = [0,0,0]
            for variant in vcf_file:
                samples = []
                if "DB"in variant.INFO:
                    DB = 1
                else:	
                    DB = 0

                if not variant.FILTER:
                    PASS = 1
                else:
                    PASS = 0
                
                if variant.num_het != 0:
                    het_samples = variant.get_hets()
                    samples = [item.sample for item in het_samples]
                if variant.num_hom_alt != 0:
                    hom_samples = [item.sample for item in variant.get_hom_alts()]
                    samples.extend(hom_samples)
                    
                for sample in samples:
                    stats = dic_samples[sample]
                    stats[0] += 1
                    stats[1] += DB
                    stats[2] += PASS
                    dic_samples[sample] = stats
        
        return dic_samples

    except Exception, e:
        print(repr(e))
    
    
def picard_files(run, path):
    try:
        QCStats_file = commands.getoutput("find " + str(path) + str(run) + "/QCStats/ -iname \"HSMetrics_summary.transposed.txt\"")
        runstats_file = commands.getoutput("find " + str(path) + str(run) + "/ -iname \"run_stats.txt\"")
        
        with open(runstats_file, "r") as runstats:
            run_stats = runstats.read()
            if 
        
        
        
        
        
        
        
        
    