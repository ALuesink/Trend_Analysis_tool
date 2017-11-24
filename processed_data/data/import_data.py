# -*- coding: utf-8 -*-
"""Import processed data"""

import commands
import vcf

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