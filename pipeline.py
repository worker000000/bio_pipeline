#coding:utf8
'''Author leoatchina'''
import os
import traceback
import collections
import signal
import csv
import re
import datetime
import subprocess
from multiprocessing import cpu_count
from multiprocessing import Pool

meminfo = open('/proc/meminfo').read()
matched = re.search(r'^MemTotal:\s+(\d+)', meminfo)
sys_mem = int(int(matched.groups()[0])/1024/1024)
sys_core = cpu_count()

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def return_cmd(cmd,print_only = False):
    try:
        if cmd:
            if print_only:
                print("===================================================\n%s\n" % cmd)
                return None
            else:
                tmp = os.popen(cmd).readlines()
                return [line.strip() for line in tmp]
        else:
            raise Exception("cmd is None")
    except Exception as e:
        print(e)
        return None

def read_csv(csv_file, delimiter=",", encoding="utf-8"):
    with open(csv_file, encoding = encoding) as csv_handle:
        csv_reader = csv.reader(csv_handle,delimiter = delimiter)
        return [i for i in csv_reader]

def write_2csv(csv_file, *lst):
    if lst:
        line = ",".join(lst)
        os.system("echo %s >> %s" % (line, csv_file))

def run_cmd(id, procedure, cmd, target, test, run_record, log = None):
    run_array = []
    if run_record:
        for line in read_csv(run_record):
            run_array.append("%s:%s:%s" % (line[0], line[1], line[2]))
    try:
        index = "%s:%s:%s" % (id, procedure, target)
        start_time = datetime.datetime.now()
        now        = start_time.strftime("%Y-%m-%d %H:%M:%S")
        if not index in run_array:
            print("================= %s ====================\n%s\n" % (now, cmd))
            if not test:
                if log:
                    with open(log,'wb') as F:
                        p = subprocess.Popen(cmd, stdout = F, stderr = F, shell = True)
                        p.wait()
                else:
                    subprocess.check_output(cmd, shell = True)
                end_time   = datetime.datetime.now()
                cost_time  = str(end_time - start_time)
                start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
                end_time   = end_time.strftime("%Y-%m-%d %H:%M:%S")
                if run_record:
                    write_2csv(run_record, id, procedure, target, start_time, end_time, cost_time)
                print(index, "finished at", end_time )
    except subprocess.CalledProcessError as e:
        end_time   = datetime.datetime.now()
        end_time   = end_time.strftime("%Y-%m-%d %H:%M:%S")
        print(index, "error at", end_time )
    except Exception as ex:
        traceback.print_exc()
        raise ex

class Pipeline(object):
    ''' pipeline to run '''
    def __del__(self):
        self.pool.terminate()

    def __init__(self, id, test = 1, run_record = None ):
        self.id         = id
        self.test       = test
        self.run_record = run_record
        self.pipeline   = collections.OrderedDict()
        self.pool       = Pool(1)
        self.pool.terminate()

    def append(self, procedure, cmd, target = None, log = None, sync = 0):
        # 有些procedure, 是'无视'target,可以并行跑,用sync !=0 表示
        # 其实这样冗余了，只要不写target，在一个procedure里，不同的cmd在target没有指明，或者指向一致的情况下本来就是并行的
        # 但增加了也好
        if sync:
            index = procedure
        else:
            if target is None:
                target = ""
            index = "%s:%s" % (procedure, target)
        if self.pipeline.get(index, None):
            self.pipeline[index].append([procedure, cmd, target, log])
        else:
            self.pipeline[index] = [[procedure, cmd, target, log]]

    def run_pipeline(self):
        for index in self.pipeline:
            each_procedure = self.pipeline[index]
            async_cnt = len(each_procedure)
            if async_cnt > sys_core:
                async_cnt = sys_core
            self.pool = Pool(async_cnt, init_worker,  maxtasksperchild = async_cnt)
            for work in each_procedure:
                procedure, cmd, target, log = work
                self.pool.apply_async(run_cmd,(self.id, procedure, cmd, target, self.test, self.run_record, log))
            self.pool.close()
            self.pool.join()
            self.pool.terminate()

    def print_pipeline(self):
        for index in self.pipeline:
            cmd_tar_logs = self.pipeline[index]
            for cmd_tar_log in cmd_tar_logs:
                cmd, target, log = cmd_tar_log
                print("===============================\n%s\n\n%s" % (index, cmd ))

    def terminate(self):
        self.pool.terminate()

def mkdir(*paths):
    for path in paths:
        if(os.path.exists(path)):
            pass
        else:
            os.system("mkdir -p %s" % path)

def mkcsv(file,*lst):
    if(os.path.isfile(file)):
        pass
    else:
        if lst:
            line = ",".join(lst)
            os.system("echo %s>%s" %(line,file))
