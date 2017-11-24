# -*- coding: utf-8 -*-
"""Upload processed data to the database"""
from sqlalchemy import create_engine, Table, MetaData
import commands
import vcf
import config
import warnings

import database

def up_to_database(run, path):
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        runs_processed_db = database.get.Runs_processed()

        if run in runs_processed_db:
            print("This run is already in the database")
        else:
            try:
                sample_vcf, list_samples = vcf_file(run, path)
                sample_dup = runstat_file(run, path)
                sample_stats = HSMetrics(run, path)
                
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
                 
def vcf_file(run, path):
    try:
        dic_samples = {}
        list_samples = []
        file_vcf = commands.getoutput("find " + str(path) + str(run) + "/ -maxdepth 1 -iname \"*.filtered_variants.vcf\"")
            
        with open(file_vcf, "r") as vcffile:
            vcf_file = vcf.Reader(vcffile)
            list_samples = vcf_file.samples
            for sample in list_samples:
                dic_samples[sample] = [0,0,0]
                list_samples.append(sample)
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
        
        return dic_samples, list_samples
        # dic_samples[sample name] = [number of variant, Percentage dbSNP variants from total, Percentage PASS variants from total]

    except Exception, e:
        print(repr(e))
    
    
def runstat_file(run, path):
    try:
        sample_dup = {}        
        runstats_file = commands.getoutput("find " + str(path) + str(run) + "/ -iname \"run_stats.txt\"")
        
        with open(runstats_file, "r") as runstats:
            run_stats = runstats.read()
            run_stats = run_stats.split("working") 
            
            for sample in run_stats[1:]:
                stats = sample.split("\n")
                
                sample_name = stats[0].split("/")[-1]
                sample_name = sample_name.replace("_dedup.flagstat...", "")
        
                dup = stats[15]
                dup = dup.split("%")[0].strip("\t").strip()
                dup = float("{0:.2f}".format(dup))
                
                sample_dup[sample_name] = dup
                
        return sample_dup
        # sample_dup[sample name] = duplication
        
    except Exception, e:
        print(repr(e))
        
def HSMetrics(run, path):
    try:
        sample_stats = {}        
        QCStats_file = commands.getoutput("find " + str(path) + str(run) + "/QCStats/ -iname \"HSMetrics_summary.transposed.txt\"")

        with open(QCStats_file, "r") as QCStats:
            sample = []
            qc_stats = QCStats.read().split("\n")
            for line in qc_stats:
                l = line.split("\t")
                sample.append(l)
            
            qc_table = [list(i) for i in map(None,*sample)]
            qc_table[0][0] = "Sample"
            
            for stats in qc_table[1:]:
                stats = stats[:-1]      #there is a None at the end of each line
                stats[0] = stats[0].replace("_dedup", "")
                stats[2] = float(stats[2].strip("%"))
                index_to_pct = [7,11,12,14,20,21,22,25,26,28,30,31,32,33,34,35,36]
                for i in index_to_pct:
                    stats[i] = float(stats[i])*100
                    stats[i] = float("{0:.2f}".format(stats[i]))
                    
                index_format = [23,24,27,29,38,39,40,41,42,43,44,45]
                for i in index_format:
                    stats[i] = float("{0:.2f}".format(stats[i]))
                
                sample_stats[stats[0]] = stats
                
        return sample_stats
        # sample_stats[sample name] = [Sample name, Total number of reads, Percentage reads mapped, BAIT_SET, GENOME_SIZE, BAIT_TERRITORY, TARGET_TERRITORY, BAIT_DESIGN_EFFICIENCY, TOTAL_READS, PF_READS, PF_UNIQUE_READS, PCT_PF_READS, PCT_PF_UQ_READS, PF_UQ_READS_ALIGNED, PCT_PF_UQ_READS_ALIGNED, PF_UQ_BASES_ALIGNED, ON_BAIT_BASES, NEAR_BAIT_BASES, OFF_BAIT_BASES, ON_TARGET_BASES, PCT_SELECTED_BASES, PCT_OFF_BAIT, ON_BAIT_VS_SELECTED, MEAN_BAIT_COVERAGE, MEAN_TARGET_COVERAGE, PCT_USABLE_BASES_ON_BAIT, PCT_USABLE_BASES_ON_TARGET, FOLD_ENRICHMENT, ZERO_CVG_TARGETS_PCT, FOLD_80_BASE_PENALTY, PCT_TARGET_BASES_2X, PCT_TARGET_BASES_10X, PCT_TARGET_BASES_20X, PCT_TARGET_BASES_30X, PCT_TARGET_BASES_40X, PCT_TARGET_BASES_50X, PCT_TARGET_BASES_100X, HS_LIBRARY_SIZE, HS_PENALTY_10X, HS_PENALTY_20X, HS_PENALTY_30X, HS_PENALTY_40X, HS_PENALTY_50X, HS_PENALTY_100X, AT_DROPOUT, GC_DROPOUT]
        
    except Exception, e:
        print(repr(e))




