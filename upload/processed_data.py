# -*- coding: utf-8 -*-
"""Upload processed_data to database"""

from sqlalchemy import create_engine, Table, MetaData
import config
import warnings

import database
import data

def up_to_database(run, path):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        try: 
            
            runs_processed_db = database.get.Runs_processed()
    
            if run in runs_processed_db:
                print("This run is already in the database")
            else:
                
                sample_vcf, list_samples = data.import_data.vcf_file(run, path)
                sample_dup = data.import_data.runstat_file(run, path)
                sample_stats = data.import_data.HSMetrics(run, path)
                
                metadata = MetaData()
                engine = create_engine("mysql+pymysql://"+config.MySQL_DB["username"]+":"+config.MySQL_DB["password"]+"@"+config.MySQL_DB["host"]+"/"+config.MySQL_DB["database"], echo=False)
                conn = engine.connect()
                
#                Run = Table("Run", metadata, autoload=True, autoload_with=engine)
                Sample_Processed = Table("Sample_Processed", metadata, autoload=True, autoload_with=engine)
                Bait_Set = Table("Bait_Set", metadata, autoload=True, autoload_with=engine)
                
                run_in_db = database.get.Runs()
                run_id = run_in_db[run]
                                
                baitset_db = database.get.BaitSet()
                
                for sample in list_samples:
                    bait_id = 0
                    vcf = sample_vcf[sample]
                    dup = sample_dup[sample]
                    stats = sample_stats[sample]
                    
                    if stats[3] in baitset_db:
                        bait_id = baitset_db[stats[3]]
                    else:
                        insert_BaitSet = Bait_Set.insert().values(Bait_name=stats[3], Genome_Size=stats[4], Bait_territory=stats[5], Target_territory=stats[6], Bait_design_efficiency=stats[7])
                        con_BaitSet = conn.execute(insert_BaitSet)
                        bait_id = con_BaitSet.inserted_primary_key
                    
                    insert_Sample = Sample_Processed.insert().values(Sample_name=sample, Total_number_of_reads=stats[1], Percentage_reads_mapped=stats[2], Total_reads=stats[8], PF_reads=stats[9], PF_unique_reads=stats[10], PCT_PF_reads=stats[11], PCT_PF_UQ_reads=stats[12], PCT_UQ_reads_aligned=stats[13], PCT_PF_UQ_reads_aligned=stats[14], PF_UQ_bases_aligned=stats[15], On_bait_bases=stats[16], Near_bait_bases=stats[17], Off_bait_bases=stats[18], On_target_bases=stats[19], PCT_selected_bases=stats[20], PCT_off_bait=stats[21], On_bait_vs_selected=stats[22], Mean_bait_coverage=stats[23], Mean_target_coverage=stats[24], PCT_usable_bases_on_bait=stats[25], PCT_usable_bases_on_target=stats[26], Fold_enrichment=stats[27], Zero_CVG_targets_PCT=stats[28], Fold_80_base_penalty=stats[29], PCT_target_bases_2X=stats[30], PCT_target_bases_10X=stats[31], PCT_target_bases_20X=stats[32], PCT_target_bases_30X=stats[33], PCT_target_bases_40X=stats[34], PCT_target_bases_50X=stats[35], PCT_target_bases_100X=stats[36], HS_library_size=stats[37], HS_penalty_10X=stats[38], HS_penalty_20X=stats[39], HS_penalty_30X=stats[40], HS_penalty_40X=stats[41], HS_penalty_50X=stats[42], HS_penalty_100X=stats[43], AT_dropout=stats[44],GC_dropout=stats[45],Duplication=dup,Number_variants=vcf[0],PCT_dbSNP_variants=vcf[1],PCT_PASS_variants=vcf[2],Run_ID=run_id,Bait_ID=bait_id)
                    conn.execute(insert_Sample)
                
                conn.close()
                
        except Exception, e:
            print(repr(e))
