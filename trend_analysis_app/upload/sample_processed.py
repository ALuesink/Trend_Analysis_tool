# -*- coding: utf-8 -*-
"""Upload processed samples to the database"""

from sqlalchemy import create_engine, Table, MetaData
import config
import warnings

import database
import data

def up_to_database(run, path, samples):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        try:
#            runs_processed_db = database.get.Runs_processed()
            
            sample_vcf = data.import_data.vcf_file(run, path)                   #dictionary: keys are sample names, values are vcf stats
            sample_dup = data.import_data.runstat_file(run, path)               #dictionary: keys are sample names, values percentage duplication
            dict_samples = data.import_data.HSMetrics(run, path)                #dictionary: keys are sample names, values are HSMetrics/Picard stats
            
            metadata = MetaData()
            engine = create_engine("mysql+pymysql://"+config.MySQL_DB["username"]+":"+config.MySQL_DB["password"]+"@"+config.MySQL_DB["host"]+"/"+config.MySQL_DB["database"], echo=False)
            conn = engine.connect()
            
            Sample_Processed = Table("Sample_Processed", metadata, autoload=True, autoload_with=engine)
            Bait_Set = Table("Bait_Set", metadata, autoload=True, autoload_with=engine)
            
            run_in_db = database.get.Runs()
            run_id = run_in_db[run]
            
            baitset_db = database.get.BaitSet()
            
            for sample in samples:
                bait_id = 0
                vcf = sample_vcf[sample]
                dup = sample_dup[sample]
                stats = dict_samples[sample]
                
                if stats['Bait_name'] in baitset_db:
                    bait_id = baitset_db[stats['Bait_name']]
                else:
                    insert_BaitSet = Bait_Set.insert().values(Bait_name=stats['Bait_name'], Genome_Size=stats['Genome_Size'], Bait_territory=stats['Bait_territory'], Target_territory=stats['Target_territory'], Bait_design_efficiency=stats['Bait_design_efficiency'])
                    con_BaitSet = conn.execute(insert_BaitSet)
                    bait_id = con_BaitSet.inserted_primary_key
                
                insert_Sample = Sample_Processed.insert().values(Sample_name=sample, Total_number_of_reads=stats['Total_number_of_reads'], Percentage_reads_mapped=stats['Percentage_reads_mapped'], Total_reads=stats['Total_reads'], PF_reads=stats['PF_reads'], PF_unique_reads=stats['PF_unique_reads'], PCT_PF_reads=stats['PCT_PF_reads'], PCT_PF_UQ_reads=stats['PCT_PF_UQ_reads'], PF_UQ_reads_aligned=stats['PF_UQ_reads_aligned'], PCT_PF_UQ_reads_aligned=stats['PCT_PF_UQ_reads_aligned'], PF_UQ_bases_aligned=stats['PF_UQ_bases_aligned'], On_bait_bases=stats['On_bait_bases'], Near_bait_bases=stats['Near_bait_bases'], Off_bait_bases=stats['Off_bait_bases'], On_target_bases=stats['On_target_bases'], PCT_selected_bases=stats['PCT_selected_bases'], PCT_off_bait=stats['PCT_off_bait'], On_bait_vs_selected=stats['On_bait_vs_selected'], Mean_bait_coverage=stats['Mean_bait_coverage'], Mean_target_coverage=stats['Mean_target_coverage'], PCT_usable_bases_on_bait=stats['PCT_usable_bases_on_bait'], PCT_usable_bases_on_target=stats['PCT_usable_bases_on_target'], Fold_enrichment=stats['Fold_enrichment'], Zero_CVG_targets_PCT=stats['Zero_CVG_targets_PCT'], Fold_80_base_penalty=stats['Fold_80_base_penalty'], PCT_target_bases_2X=stats['PCT_target_bases_2X'], PCT_target_bases_10X=stats['PCT_target_bases_10X'], PCT_target_bases_20X=stats['PCT_target_bases_20X'], PCT_target_bases_30X=stats['PCT_target_bases_30X'], PCT_target_bases_40X=stats['PCT_target_bases_40X'], PCT_target_bases_50X=stats['PCT_target_bases_50X'], PCT_target_bases_100X=stats['PCT_target_bases_100X'], HS_library_size=stats['HS_library_size'], HS_penalty_10X=stats['HS_penalty_10X'], HS_penalty_20X=stats['HS_penalty_20X'], HS_penalty_30X=stats['HS_penalty_30X'], HS_penalty_40X=stats['HS_penalty_40X'], HS_penalty_50X=stats['HS_penalty_50X'], HS_penalty_100X=stats['HS_penalty_100X'], AT_dropout=stats['AT_dropout'],GC_dropout=stats['GC_dropout'],Duplication=dup,Number_variants=vcf[0],dbSNP_variants=vcf[1],PASS_variants=vcf[2],Run_ID=run_id,Bait_ID=bait_id)
                conn.execute(insert_Sample)
                
           
            conn.close()
            
        except Exception, e:
            print(repr(e))