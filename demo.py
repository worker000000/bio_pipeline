#!/usr/bin/env python3
import os
import argparse
from pipeline import Pipeline, return_cmd

print("This is a demo using E.coli genome")

parser = argparse.ArgumentParser(description = "ngs demo pipeline")
parser.add_argument('-t', type = int, help = "test mode OR not,1 for true, 0 for false", default = 1, dest = 'test')
args = parser.parse_args()
demo_record = "demo/output/demo.csv"


pipeline = Pipeline(demo_record, test = args.test)

fq1 = "demo/input/SRR1770413_1.head.fastq.gz"
fq2 = "demo/input/SRR1770413_2.head.fastq.gz"

ID = os.path.basename(fq1).split("_")[0]

bwa_mem_template = "bwa mem -t 4 -R '@RG\tID:SRR1770413\tPL:illumina\tSM:E.coli_K12' demo/genome/E.coli_K12_MG1655.fa {} {} | samtools view -Sb - > demo/output/SRR1770413.bwa_mem.bam"
bwa_mem_cmd = bwa_mem_template.format(fq1, fq2)
pipeline.append(ID, "bwa_mem", bwa_mem_cmd, log = "demo/output/bwa_mem.log")


reorder_template = "java -Xmx16g -Djava.io.tmpdir=/tmp \
    -jar demo/tools/picard-tools-1.119/ReorderSam.jar \
    I=demo/output/{}.bwa_mem.bam \
    O=demo/output/{}.reorder.bwa_mem.bam \
    REFERENCE=demo/genome/E.coli_K12_MG1655.fa \
    VALIDATION_STRINGENCY=SILENT \
    CREATE_INDEX=TRUE"
reorder_cmd = reorder_template.format(ID, ID)
pipeline.append(ID, "reorder", reorder_cmd, log = "demo/output/reorder.log")


mark_dup_template = "java -Xmx16g -Djava.io.tmpdir=/tmp \
    -jar demo/tools/picard-tools-1.119/MarkDuplicates.jar \
    I=demo/output/{}.reorder.bwa_mem.bam \
    O=demo/output/{}.nodup.reorder.bwa_mem.bam \
    METRICS_FILE=demo/output/{}.duplicate_report.txt \
    CREATE_INDEX=true \
    ASSUME_SORTED=true \
    REMOVE_DUPLICATES=true \
    VALIDATION_STRINGENCY=LENIENT"
mark_dup_cmd = mark_dup_template.format(ID, ID, ID)
pipeline.append(ID, "mark_dup", mark_dup_cmd, log = "demo/output/mark_dup.log")


pipeline.run_pipeline()
