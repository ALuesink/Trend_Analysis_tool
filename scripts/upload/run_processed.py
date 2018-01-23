# -*- coding: utf-8 -*-
"""Upload processed run data to database"""

from sqlalchemy import select
from ..database import connection, get, set_run
import warnings
import sys
import data


def up_to_database(path):
    with warnings.catch_warnings():
        warnings.simplefilter("always")
        warnings.filterwarnings("error")
        try:
            path = path.strip().rstrip('/')
            run = path.split("/")[-1]
            run_core = set_run.set_run_name(run)
            runs_processed_db = get.runs_processed()

            if run in runs_processed_db:
                sys.stdout.write('This run is already in the database \n')
            else:
                sample_vcf = data.import_data.vcf_file(path)                   # dictionary: keys are sample names, values are vcf stats
                sample_dup = data.import_data.runstat_file(path)               # dictionary: keys are sample names, values percentage duplication
                dict_samples = data.import_data.HSMetrics(path)                # dictionary: keys are sample names, values are HSMetrics/Picard stats

                engine = connection.engine()
                conn = engine.connect()

                sample_processed = connection.sample_processed_table(engine)
                bait_set = connection.bait_set_table(engine)

                run_in_db = get.runs()
                run_id = run_in_db[run_core]
                baitset_db = get.bait_set()

                for sample in dict_samples:
                    bait_id = 0
                    vcf = sample_vcf[sample]
                    dup = sample_dup[sample]
                    stats = dict_samples[sample]

                    if stats['Bait_name'] in baitset_db:
                        bait_id = baitset_db[stats['Bait_name']]
                    else:
                        try:
                            insert_bait_set = bait_set.insert().values(
                                Bait_name=stats['Bait_name'], Genome_Size=stats['Genome_Size'],
                                Bait_territory=stats['Bait_territory'],
                                Target_territory=stats['Target_territory'],
                                Bait_design_efficiency=stats['Bait_design_efficiency']
                            )

                            conn.execute(insert_bait_set)

                        except Warning, w:
                            print(w)
                            query = select([bait_set.c.Bait_ID]).\
                                order_by(bait_set.c.Bait_ID.desc()).limit(1)
                            res = conn.execute(query).fetchall()
                            bait_id = res[0][0]

                            delete = bait_set.delete().where(bait_set.c.Bait_ID == bait_id)
                            conn.execute(delete)
                            sys.stdout.write('Baitset: Data deleted from database \n')
                            sys.exit()

                    try:
                        insert_sample = sample_processed.insert().values(
                            Sample_name=sample, Total_number_of_reads=stats['Total_number_of_reads'],
                            Percentage_reads_mapped=stats['Percentage_reads_mapped'],
                            Total_reads=stats['Total_reads'], PF_reads=stats['PF_reads'],
                            PF_unique_reads=stats['PF_unique_reads'], PCT_PF_reads=stats['PCT_PF_reads'],
                            PCT_PF_UQ_reads=stats['PCT_PF_UQ_reads'],
                            PF_UQ_reads_aligned=stats['PF_UQ_reads_aligned'],
                            PCT_PF_UQ_reads_aligned=stats['PCT_PF_UQ_reads_aligned'],
                            PF_UQ_bases_aligned=stats['PF_UQ_bases_aligned'], On_bait_bases=stats['On_bait_bases'],
                            Near_bait_bases=stats['Near_bait_bases'], Off_bait_bases=stats['Off_bait_bases'],
                            On_target_bases=stats['On_target_bases'], PCT_selected_bases=stats['PCT_selected_bases'],
                            PCT_off_bait=stats['PCT_off_bait'], On_bait_vs_selected=stats['On_bait_vs_selected'],
                            Mean_bait_coverage=stats['Mean_bait_coverage'],
                            Mean_target_coverage=stats['Mean_target_coverage'],
                            PCT_usable_bases_on_bait=stats['PCT_usable_bases_on_bait'],
                            PCT_usable_bases_on_target=stats['PCT_usable_bases_on_target'],
                            Fold_enrichment=stats['Fold_enrichment'], Zero_CVG_targets_PCT=stats['Zero_CVG_targets_PCT'],
                            Fold_80_base_penalty=stats['Fold_80_base_penalty'],
                            PCT_target_bases_2X=stats['PCT_target_bases_2X'],
                            PCT_target_bases_10X=stats['PCT_target_bases_10X'], PCT_target_bases_20X=stats['PCT_target_bases_20X'],
                            PCT_target_bases_30X=stats['PCT_target_bases_30X'], PCT_target_bases_40X=stats['PCT_target_bases_40X'],
                            PCT_target_bases_50X=stats['PCT_target_bases_50X'], PCT_target_bases_100X=stats['PCT_target_bases_100X'],
                            HS_library_size=stats['HS_library_size'], HS_penalty_10X=stats['HS_penalty_10X'],
                            HS_penalty_20X=stats['HS_penalty_20X'], HS_penalty_30X=stats['HS_penalty_30X'],
                            HS_penalty_40X=stats['HS_penalty_40X'], HS_penalty_50X=stats['HS_penalty_50X'],
                            HS_penalty_100X=stats['HS_penalty_100X'], AT_dropout=stats['AT_dropout'],
                            GC_dropout=stats['GC_dropout'], Duplication=dup, Number_variants=vcf[0],
                            dbSNP_variants=vcf[1], PASS_variants=vcf[2], Run_ID=run_id, Bait_ID=bait_id
                            )

                        conn.execute(insert_sample)

                    except Warning, w:
                        print(w)
                        delete = sample_processed.delete().\
                            where(sample_processed.c.Run_ID == run_id)
                        conn.execute(delete)
                        sys.stdout.write('Sample insert: Data deleted from database \n')
                        sys.exit()

                conn.close()

        except Exception, e:
            print(e)
