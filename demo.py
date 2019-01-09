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

bwa_mem_template = "bwa mem -t 4 -R '@RG\tID:SRR1770413\tPL:illumina\tSM:E.coli_K12' demo/genome/E.coli_K12_MG1655.fa {} {} | samtools view -Sb - > demo/output/SRR1770413.bam"
bwa_mem_cmd = bwa_mem_template.format(fq1, fq2)
pipeline.append("SRR1770413", "bwa_mem", bwa_mem_cmd, log = "demo/output/bwa_mem.log")






pipeline.run_pipeline()
