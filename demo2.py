#!/usr/bin/env python3

import os
import sys
import argparse
from pipeline import Pipeline, return_cmd, sys_core, sys_mem


parser = argparse.ArgumentParser(description = "wes_test")
parser.add_argument("-m", type = int, help = "The process number run at a same time", default = 5, dest = "multi")
parser.add_argument("-t", type = int, help = "If test, deault 1", default = 1, dest = "test")
parser.add_argument("-r", type = int, help = "If rm , default 0", default = 0, dest = "rm")
parser.add_argument("-p", type = float, help = "Percent", default = 0.75, dest = "percent")


params = parser.parse_args()

per_mem  = int(sys_mem*params.percent/params.multi) if int(sys_mem*params.percent/params.multi) >= 3 else 4
per_core = int(sys_core*params.percent/params.multi) if int(sys_core*params.percent/params.multi) >= 1 else 1

all_rawdata_path = "../RawData"
all_cleandata_path = "../CleanData"
all_tmp_path = "../Tmpdata"
all_results_path = "../Results"
all_log_path = "../log"
pipeline = Pipeline('../Record/record.csv', params.multi, params.test)


# qc check and trim_galore, use fastqc to detect the quality of files before/after trim_galore
# actually, since the qualiy of rawdata is good, trim and fq_after were not processed
def qc(ID, kind, rawdata_path, cleandata_path):
    # find raw_data_path for fq.gz files
    find_cmd = "find {} -type f | sort | grep fq.gz$".format(rawdata_path)
    rawdata_files = return_cmd(find_cmd)
    # fastqc before
    os.system('mkdir -p ../fastqc/{}/before'.format(ID + kind))
    for rawdata_file in rawdata_files:
        fastqc_template = "fastqc -o ../fastqc/{ID}/before -t {per_core} {rawdata_file}"
        fastqc_cmd = fastqc_template.format(ID = ID + kind, per_core = per_core, rawdata_file = rawdata_file)
        pipeline.append(ID + kind, "fastqc_before", fastqc_cmd, rawdata_file, run_sync = True)
    multiqc_cmd = "multiqc -n {ID} -o ../fastqc/before ../fastqc/{ID}/before/*.zip".format(ID = ID + kind)
    pipeline.append(ID + kind, "multiqc_before", multiqc_cmd, ID + kind + ".html")
    # trim_glare
    # paired files to trim_galore
    # rawdata_files_paired = zip(rawdata_files[::2], rawdata_files[1::2])
    # for (fq1, fq2) in rawdata_files_paired:
        # trim_galore_cmd = "trim_galore --length 50 --stringency 5 -q 25 -e 0.1 \
            # --paired --phred33 \
            # -o {cleandata_path} \
            # {fq1} {fq2}".format(cleandata_path = cleandata_path, fq1 = fq1, fq2 = fq2)
        # pipeline.append(ID + kind, "trim_galore", trim_galore_cmd, run_sync = True, log = os.path.join(all_log_path, "{}.log".format(fq1)))
    # fastqc after
    # os.system('mkdir -p ../fastqc/{}/after'.format(ID + kind))
    # find_cmd = "find {} -type f | sort | grep fq.gz$".format(cleandata_path)
    # cleandata_files = return_cmd(find_cmd)
    # for cleandata_file in cleandata_files:
        # fastqc_template = "fastqc -o ../fastqc/{ID}/after -t {per_core} {cleandata_file}"
        # fastqc_cmd = fastqc_template.format(ID = ID + kind, per_core = per_core, cleandata_file = cleandata_file)
        # pipeline.append(ID + kind, "fastqc_after", fastqc_cmd, cleandata_file, run_sync = True)


# recal
def recal(ID, kind, data_path, tmp_path, target_path, rm = 0):
    find_cmd = "find {} -type f | sort | grep fq.gz$".format(data_path)
    fq_files = return_cmd(find_cmd)
    merge_bams = []
    for (fq1, fq2) in zip(fq_files[0::2], fq_files[1::2]):
        bam_name = os.path.basename(fq1).split(".")[0].replace("_1", "")
        # bwa_mem
        RG = '@RG\\tID:%s\\tPL:illumina\\tSM:%s' % (ID + kind, ID + kind)
        bwa_mem_template = "bwa mem -t {per_core} -M -R \"{RG}\" \
                            /mnt/bioinfo/bundle/hg38/Homo_sapiens_assembly38.fasta.gz \
                            {fq1} {fq2} | samtools sort -@ 2 -m {per_mem}G -o {tmp_path}/{bam_name}.sort.bam -"
        bwa_mem_cmd = bwa_mem_template.format(per_core = per_core - 2, per_mem = int(per_mem * 0.6), RG = RG,
                                              fq1 = fq1, fq2 = fq2, tmp_path = tmp_path, bam_name = bam_name)
        log = os.path.join(tmp_path, bam_name + ".bwa_mem.log")
        pipeline.append(ID + kind, "bwa_mem", bwa_mem_cmd, "{}.sort.bam".format(bam_name), log = log, run_sync = True)
        # markdup
        mark_dup_template = "gatk MarkDuplicates \
                            -I {tmp_path}/{bam_name}.sort.bam \
                            -M {tmp_path}/{bam_name}.markdup.sort.metrics.txt \
                            -O {tmp_path}/{bam_name}.markdup.sort.bam"
        mark_dup_cmd = mark_dup_template.format(tmp_path = tmp_path, bam_name = bam_name)
        log = os.path.join(tmp_path, bam_name+".markdup.log")
        merge_bams.append("{tmp_path}/{bam_name}.markdup.sort.bam".format(tmp_path = tmp_path, bam_name = bam_name))
        pipeline.append(ID + kind, "markdup", mark_dup_cmd, "{}.markdup.sort.bam".format(bam_name), log = log, run_sync = True)
    # merge
    merge_bams_template = "gatk MergeSamFiles -O {}/{}.merge.bam -I "
    merge_bams = " -I ".join(merge_bams)
    merge_bams_cmd = merge_bams_template.format(tmp_path, ID + kind) + merge_bams
    log = os.path.join(tmp_path, ID + kind + ".merge.log")
    pipeline.append(ID + kind, "merge_bams", merge_bams_cmd, ID + kind + ".merge.bam", log = log, run_sync = True)
    # fixinfo
    fix_info_template = 'gatk --java-options "-Xmx{per_mem}G -Djava.io.tmpdir=/tmp" FixMateInformation \
                        -I {tmp_path}/{ID}.merge.bam \
                        -O {tmp_path}/{ID}.fix.merge.bam \
                        -SO coordinate'
    fix_info_cmd = fix_info_template.format(tmp_path = tmp_path, ID = ID + kind, per_mem = per_mem)
    log = os.path.join(tmp_path, ID + kind + ".fix.log")
    pipeline.append(ID + kind, "fix_info", fix_info_cmd, ID + kind + ".fix.merge.bam", log = log, run_sync = True)
    # index
    index_cmd = "samtools index {}/{}.fix.merge.bam".format(tmp_path, ID + kind)
    pipeline.append(ID + kind, "index_fix", index_cmd, ID + kind + ".fix.merge.bam")
    # bqsr
    bqsr_template = 'gatk --java-options "-Xmx{per_mem}G -Djava.io.tmpdir=/tmp" BaseRecalibrator \
                    -R /mnt/bioinfo/bundle/hg38/Homo_sapiens_assembly38.fasta \
                    -I {tmp_path}/{ID}.fix.merge.bam  \
                    --known-sites /mnt/bioinfo/bundle/hg38/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz \
                    --known-sites /mnt/bioinfo/bundle/hg38/1000G_phase1.snps.high_confidence.hg38.vcf.gz \
                    --known-sites /mnt/bioinfo/bundle/hg38/beta/Homo_sapiens_assembly38.known_indels.vcf.gz \
                    -O {tmp_path}/{ID}.bqsr.table'
    bqsr_cmd = bqsr_template.format(ID = ID + kind, per_mem = per_mem, Interval_list = "./exon_probe.hg38.gene.bed", tmp_path = tmp_path)
    log = os.path.join(tmp_path, ID + kind + ".bqsr.log")
    pipeline.append(ID + kind, "bqsr", bqsr_cmd, ID + kind + ".BQSR.table", log = log, run_sync = True)
    # ApplyBQSR, instead PrintRead
    apply_bqsr_template = 'gatk --java-options "-Xmx{per_mem}G -Djava.io.tmpdir=/tmp" ApplyBQSR \
                            -R /mnt/bioinfo/bundle/hg38/Homo_sapiens_assembly38.fasta \
                            --bqsr-recal-file {tmp_path}/{ID}.bqsr.table \
                            -I {tmp_path}/{ID}.fix.merge.bam \
                            -O {target_path}/{ID}.recal.bam'
    apply_bqsr_cmd = apply_bqsr_template.format(per_mem = per_mem, tmp_path = tmp_path, ID = ID + kind, target_path = target_path)
    log = os.path.join(tmp_path, ID + kind + ".recal.log")
    pipeline.append(ID + kind, "recal", apply_bqsr_cmd, ID + kind + ".recal.bam", log = log, run_sync = True)
    # qualimap
    qualimap_template = "qualimap bamqc --java-mem-size={per_mem}G -gff exon_probe.hg38.gene.bed -bam {target_path}/{ID}.recal.bam"
    qualimpap_cmd = qualimap_template.format(per_mem = per_mem, target_path = target_path, ID = ID + kind)
    log = os.path.join(target_path, ID + kind + ".qualimpa.log")
    pipeline.append(ID + kind, "qualimpap", qualimpap_cmd, ID + kind + ".recal.bam", log = log, run_sync = True)
    # HaplotypeCaller
    haplotype_caller_template = 'gatk --java-options "-Xmx{per_mem}G -Djava.io.tmpdir=/tmp" HaplotypeCaller \
                            --native-pair-hmm-threads {per_core} \
                            -R /mnt/bioinfo/bundle/hg38/Homo_sapiens_assembly38.fasta \
                            -I {target_path}/{ID}.recal.bam \
                            -O {target_path}/{ID}.gvcf.gz \
                            -ERC GVCF'
    haplotype_caller_cmd = haplotype_caller_template.format(target_path = target_path, ID = ID + kind, per_mem = per_mem, per_core = per_core)
    log = os.path.join(target_path, ID + kind + ".hc.log")
    pipeline.append(ID + kind, "haplotype_caller", haplotype_caller_cmd, ID + kind + ".gvcf.gz", log = log, run_sync = True)
    # HaplotypeCaller exon
    haplotype_caller_exon_template = 'gatk --java-options "-Xmx{per_mem}G -Djava.io.tmpdir=/tmp" HaplotypeCaller \
                            --native-pair-hmm-threads {per_core} \
                            -R /mnt/bioinfo/bundle/hg38/Homo_sapiens_assembly38.fasta \
                            -L ./exon.list \
                            --dbsnp /mnt/bioinfo/bundle/hg38/dbsnp_146.hg38.vcf.gz \
                            -I {target_path}/{ID}.recal.bam \
                            -O {target_path}/{ID}.exon.gvcf.gz \
                            -ERC GVCF'
    haplotype_caller_exon_cmd = haplotype_caller_exon_template.format(target_path = target_path, ID = ID + kind, per_mem = per_mem, per_core = per_core)
    log = os.path.join(target_path, ID + kind + ".hc.exon.log")
    pipeline.append(ID + kind, "haplotype_caller_exon", haplotype_caller_exon_cmd, ID + kind + ".exon.gvcf.gz", log = log, run_sync = True)


# main function
def wgs(ID, normal_path, tumor_path,
        normal_clean_path, tumor_clean_path,
        normal_tmp_path, tumor_tmp_path,
        target_path, rm = params.rm):
    # create each
    for each in (normal_clean_path, tumor_clean_path, normal_tmp_path, tumor_tmp_path, target_path):
        os.system("mkdir -p {}".format(each))
    qc(ID, "normal", normal_path, normal_clean_path)
    qc(ID, "tumor", tumor_path, tumor_clean_path)
    recal(ID, "normal", normal_path, normal_tmp_path, target_path, params.rm)
    recal(ID, "tumor", tumor_path, tumor_tmp_path, target_path, params.rm)


# find fq.gz files, pair them, then add to pipeline
find_rawdata_path_cmd = "find {all_rawdata_path} -maxdepth 1 -type d | sort ".format(all_rawdata_path = all_rawdata_path)
paths = return_cmd(find_rawdata_path_cmd)[1:]
# zip normal and tumor, then add to pipeline
normal_tumor = zip(paths[0::2], paths[1::2])
try:
    for (normal_path, tumor_path) in normal_tumor:
        normal_path_name = os.path.basename(normal_path)
        tumor_path_name = os.path.basename(tumor_path)
        ID = normal_path_name[:-3]

        normal_clean_path = os.path.join(all_cleandata_path, normal_path_name)
        tumor_clean_path  = os.path.join(all_cleandata_path, tumor_path_name)

        normal_tmp_path   = os.path.join(all_tmp_path, normal_path_name)
        tumor_tmp_path    = os.path.join(all_tmp_path, tumor_path_name)

        target_path = os.path.join(all_results_path, ID)
        wgs(ID, normal_path, tumor_path, normal_clean_path, tumor_clean_path, normal_tmp_path, tumor_tmp_path, target_path, rm = params.rm)
except KeyboardInterrupt:
    print("Ctrl+C pressed ,exiting")
    pipeline.terminate()
    sys.exit(0)
pipeline.run_pipeline()
